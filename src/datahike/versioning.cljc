(ns datahike.versioning
  "Git-like versioning tools for Datahike.
   All operations support both synchronous (CLJ default) and asynchronous modes."
  (:require [konserve.core :as k]
            [konserve.store :as ks]
            [datahike.config :as dc]
            [datahike.connections :refer [delete-connection!]]
            [datahike.store :refer [store-identity add-cache-and-handlers]]
            [datahike.writing :refer [stored->db db->stored stored-db?
                                      commit! create-commit-id get-and-clear-pending-kvs!
                                      write-pending-kvs!]]
            [datahike.writer]
            [datahike.index.secondary :as sec]
            #?(:clj  [superv.async :refer [go-try- <?- <? S go-loop-try]]
               :cljs [superv.async :refer [S]
                                   :refer-macros [go-try- <?- <? go-loop-try]])
            [datahike.db.utils :refer [db?]]
            [datahike.tools :as dt]
            [replikativ.logging :as log]
            [konserve.utils :refer [#?(:clj async+sync) multi-key-capable? *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            #?(:cljs [clojure.core.async :refer [<!]]))
  #?(:cljs (:require-macros [clojure.core.async :refer [go]]))
  #?(:clj (:import [datahike.connector Connection])))

(defn- branch-check [branch]
  (when-not (keyword? branch)
    (log/raise "Branch must be a keyword." {:type :branch-must-be-keyword :branch branch})))

(defn- db-check [db]
  (when-not (db? db)
    (log/raise "You must provide a DB value." {:type :db-value-required :db db})))

(defn- parent-check [parents]
  (when-not (pos? (count parents))
    (log/raise "You must provide at least one parent."
               {:type :must-provide-at-least-one-parent :parents parents})))

(defn- commit-id-check [commit-id]
  (when-not (uuid? commit-id)
    (log/raise "Commit-id must be a uuid."
               {:type :commit-id-must-be-uuid :commit-id commit-id})))

(defn- extract-store
  "Extract konserve store from a connection or db value."
  [conn-or-db]
  (cond
    #?(:clj (instance? Connection conn-or-db) :cljs (satisfies? IDeref conn-or-db))
    (:store @conn-or-db)

    (db? conn-or-db)
    (:store conn-or-db)

    :else
    ;; Assume it's a raw store
    conn-or-db))

;; ========================= public API =========================

(defn branches
  "List all known branch names. Returns set of keywords."
  ([conn] (branches conn {:sync? true}))
  ([conn opts]
   (let [store (extract-store conn)
         opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try- (<?- (k/get store :branches nil opts)))))))

(defn branch-history
  "Returns the commit history of the branch of the connection in
  form of all stored db values. Performs backtracking and returns dbs in order.
  Always returns a channel."
  [conn]
  (let [{:keys [store] {:keys [branch]} :config} @conn]
    (go-loop-try S [[to-check & r] [branch]
                    visited #{}
                    reachable []]
                 (if to-check
                   (if (visited to-check) ;; skip
                     (recur r visited reachable)
                     (if-let [raw-db (<? S (k/get store to-check))]
                       (let [{{:keys [datahike/parents]} :meta
                              :as db} (stored->db raw-db store)]
                         (recur (concat r parents)
                                (conj visited to-check)
                                (conj reachable db)))
                       reachable))
                   reachable))))

(defn branch!
  "Create a new branch from commit-id or existing branch as new-branch.
   Secondary indices are CoW-branched via their native branching support."
  ([conn from new-branch] (branch! conn from new-branch {:sync? true}))
  ([conn from new-branch opts]
   (let [opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  (let [store (:store @conn)
                        existing-branches (<?- (k/get store :branches nil opts))
                        _ (when (and existing-branches (existing-branches new-branch))
                            (log/raise "Branch already exists." {:type :branch-already-exists
                                                                 :new-branch new-branch}))
                        stored-db (<?- (k/get store from nil opts))]
                    (when-not (stored-db? stored-db)
                      (throw (ex-info "From does not point to an existing branch or commit."
                                      {:type :from-branch-does-not-point-to-existing-branch-or-commit
                                       :from from})))
                  ;; Branch secondary indices via their native CoW support.
                  ;; Prefer live indices from the connection (they hold the write lock).
                    (let [sec-keys (:secondary-index-keys stored-db)
                          live-indices (:secondary-indices @conn)
                          from-branch (or (when (keyword? from) from) :db)
                          branched-sec-keys
                          #?(:clj
                             (when (or (seq sec-keys) (seq live-indices))
                               (reduce-kv
                                (fn [acc idx-ident idx]
                                  (if (satisfies? sec/IVersionedSecondaryIndex idx)
                                    (let [branched (sec/-sec-branch idx store from-branch new-branch)
                                          key-map (sec/-sec-flush branched store new-branch)]
                                      (when (instance? java.io.Closeable branched)
                                        (.close ^java.io.Closeable branched))
                                      (assoc acc idx-ident key-map))
                                    (if-let [key-map (get sec-keys idx-ident)]
                                      (assoc acc idx-ident
                                             (sec/branch-from-key-map key-map store from-branch new-branch))
                                      acc)))
                                {} (or live-indices {})))
                             :cljs nil)
                          updated-db (cond-> (assoc-in stored-db [:config :branch] new-branch)
                                       (seq branched-sec-keys) (assoc :secondary-index-keys branched-sec-keys))]
                      (<?- (k/assoc store new-branch updated-db opts))
                      (<?- (k/update store :branches #(conj (set %) new-branch) opts)))))))))

(defn delete-branch!
  "Removes this branch from set of known branches. The branch will still be
  accessible until the next gc. Remote readers need to release their connections."
  ([conn branch] (delete-branch! conn branch {:sync? true}))
  ([conn branch opts]
   (when (= branch :db)
     (log/raise "Cannot delete main :db branch. Delete database instead."
                {:type :cannot-delete-main-db-branch}))
   (let [opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  (let [store (:store @conn)
                        existing-branches (<?- (k/get store :branches nil opts))]
                    (when-not (and existing-branches (existing-branches branch))
                      (log/raise "Branch does not exist." {:type :branch-does-not-exist
                                                           :branch branch}))
                    (delete-connection! [(store-identity (get-in @conn [:config :store])) branch])
                    (<?- (k/update store :branches #(disj (set %) branch) opts))))))))

(defn force-branch!
  "Force the branch to point to the provided db value. Branch will be created if
  it does not exist. Parents must point to a set of branches or commits.

  WARNING: This overwrites the branch head unconditionally, like git reset --hard.
  Existing connections to this branch will see stale state and must be released
  and reconnected. Use with care — you can render data inaccessible."
  ([db branch parents] (force-branch! db branch parents {:sync? true}))
  ([db branch parents opts]
   (db-check db)
   (branch-check branch)
   (parent-check parents)
   (let [opts (select-keys opts [:sync?])
         sync? (:sync? opts)]
     (async+sync sync? *default-sync-translation*
                 (go-try-
                  (let [store (:store db)
                        ;; Flush first, then compute the audit-grade cid
                        ;; from the post-flush stored form (true merkle
                        ;; root). Same pattern as datahike.writing/commit!.
                        db-with-parents (-> db
                                            (assoc-in [:config :branch] branch)
                                            (assoc-in [:meta :datahike/parents] parents))
                        [schema-meta-kv-to-write pre-cid-store]
                        (db->stored db-with-parents true)
                        cid (create-commit-id db-with-parents pre-cid-store)
                        db-to-store (assoc-in pre-cid-store
                                              [:meta :datahike/commit-id] cid)
                        pending-kvs (get-and-clear-pending-kvs! store)]

                  ;; Update the set of known branches
                    (<?- (k/update store :branches #(conj (set %) branch) opts))

                  ;; Write all data
                    (if (multi-key-capable? store)
                      (let [writes-map (cond-> (into {} pending-kvs)
                                         schema-meta-kv-to-write (assoc (first schema-meta-kv-to-write) (second schema-meta-kv-to-write))
                                         true                    (assoc cid db-to-store)
                                         true                    (assoc branch db-to-store))]
                        (<?- (k/multi-assoc store writes-map opts)))
                      (do
                        (<?- (write-pending-kvs! store pending-kvs sync?))
                        (when schema-meta-kv-to-write
                          (<?- (k/assoc store (first schema-meta-kv-to-write) (second schema-meta-kv-to-write) opts)))
                        (<?- (k/assoc store cid db-to-store opts))
                        (<?- (k/assoc store branch db-to-store opts))))
                    nil))))))

(defn commit-id
  "Retrieve the commit-id for this db."
  [db]
  (db-check db)
  (get-in db [:meta :datahike/commit-id]))

(defn parent-commit-ids
  "Retrieve parent commit ids from db."
  [db]
  (db-check db)
  (get-in db [:meta :datahike/parents]))

(defn commit-as-db
  "Loads the database stored at this commit id.
   First argument can be a connection, db value, or raw konserve store."
  ([conn-or-store cid] (commit-as-db conn-or-store cid {:sync? true}))
  ([conn-or-store cid opts]
   (commit-id-check cid)
   (let [store (extract-store conn-or-store)
         opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  (when-let [raw-db (<?- (k/get store cid nil opts))]
                    (stored->db raw-db store)))))))

(defn branch-as-db
  "Loads the database stored at this branch.
   First argument can be a connection, db value, or raw konserve store."
  ([conn-or-store branch] (branch-as-db conn-or-store branch {:sync? true}))
  ([conn-or-store branch opts]
   (branch-check branch)
   (let [store (extract-store conn-or-store)
         opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  (when-let [raw-db (<?- (k/get store branch nil opts))]
                    (stored->db raw-db store)))))))

(def ^:private fork-semantic-keys
  "Config keys that are baked into the data on disk. A fork inherits them
  from the source; a target config may only restate them identically."
  [:keep-history? :schema-flexibility :attribute-refs? :crypto-hash? :index])

(defn- commit-instant
  "Timestamp of a raw stored db map: updated-at, falling back to created-at."
  [stored]
  (let [{:keys [datahike/updated-at datahike/created-at]} (:meta stored)]
    (or updated-at created-at)))

(defn- fork-point-check [at]
  (when-not (or (nil? at) (int? at) (inst? at))
    (log/raise "Fork point :at must be a transaction id (long) or an inst."
               {:type :invalid-fork-point :at at})))

(defn fork-database
  "Fork the source database into an independent, WRITABLE target database.

  Copies every konserve key from the source store into the target store,
  then points the target's :db branch at the commit selected by :at:

    - :at a tx-id long — the commit whose :max-tx equals it exactly
      (one commit per transaction); raises :fork-point-after-head when
      it is newer than the head and :fork-point-not-found when no
      commit in history carries it.
    - :at an inst — the newest commit whose commit timestamp is <= it.
    - :at absent — the head (fork of the current state).

  The fork preserves everything byte-faithfully as of the fork point:
  entity ids, transaction ids, tx-meta, schema, and full history
  (as-of/history keep working when :keep-history? is true; the commit
  walk itself works either way since every transaction commits). New
  transactions on the fork continue from the fork point's :max-tx.

  The target gets a FRESH store identity: when the target config carries
  no [:store :id] one is minted. Returns the target's EFFECTIVE config —
  connect with exactly that value (it carries the minted store id and
  the semantic settings inherited from the source). delete-database on
  the returned config only ever touches the target store.

  Semantic settings (:keep-history?, :schema-flexibility, :index,
  :attribute-refs?, :crypto-hash?) are baked into the stored data, so
  they are inherited from the source; a target config restating one
  differently raises :fork-config-mismatch.

  Copying runs at the konserve layer (general for every backend that
  enumerates keys). Copying from a store that is being written to
  concurrently can tear — quiesce writers or verify the fork's head
  afterwards. Secondary indices whose data lives outside the konserve
  store (e.g. Lucene directories) are not copied and need a backfill."
  ([source-config target-config]
   (fork-database source-config target-config {:sync? true}))
  ([source-config target-config {:keys [at] :as opts}]
   (fork-point-check at)
   (let [opts (merge {:sync? true} (select-keys opts [:sync?]))
         sync? (:sync? opts)
         target-config-as-arg target-config
         source-config (dc/load-config source-config)
         target-config (dc/load-config
                        (if (get-in target-config [:store :id])
                          target-config
                          (assoc-in target-config [:store :id]
                                    #?(:clj (java.util.UUID/randomUUID)
                                       :cljs (random-uuid)))))
         src-store-config (:store source-config)
         tgt-store-config (:store target-config)]
     (when (= (store-identity src-store-config) (store-identity tgt-store-config))
       (log/raise "Fork target must have its own store identity, not the source's."
                  {:type :fork-target-same-store-identity
                   :store-id (store-identity src-store-config)}))
     (async+sync
      sync? *default-sync-translation*
      (go-try-
       (let [src-exists? (<?- (ks/store-exists? src-store-config opts))
             _ (when-not src-exists?
                 (log/raise "Source database does not exist."
                            {:type :db-does-not-exist :config source-config}))
             src-raw (<?- (ks/connect-store src-store-config opts))
             src-store* (add-cache-and-handlers src-raw source-config)
             src-head* (<?- (k/get src-store* :db nil opts))
             _ (when-not src-head*
                 (ks/release-store src-store-config src-store*)
                 (log/raise "Source database does not exist."
                            {:type :db-does-not-exist :config source-config}))
             ;; Trust the stored index over the intended one (mirrors connect).
             stored-config (:config src-head*)
             [source-config src-store src-head]
             (if (= (:index source-config) (:index stored-config))
               [source-config src-store* src-head*]
               (let [source-config (assoc source-config :index (:index stored-config))
                     s (add-cache-and-handlers src-raw source-config)]
                 [source-config s (<?- (k/get s :db nil opts))]))
             ;; Semantic keys are data properties — the target may not disagree.
             _ (doseq [sk fork-semantic-keys]
                 (when (and (contains? target-config-as-arg sk)
                            (not= (get target-config-as-arg sk) (get stored-config sk)))
                   (log/raise "Fork target config conflicts with the source's stored config."
                              {:type :fork-config-mismatch
                               :key sk
                               :target-value (get target-config-as-arg sk)
                               :source-value (get stored-config sk)})))
             ;; Resolve the fork point on the source BEFORE creating the target,
             ;; so bad :at values fail without leaving a partial store behind.
             at-stored
             (cond
               (nil? at) src-head
               (int? at)
               (do
                 (when (> at (:max-tx src-head))
                   (log/raise "Fork point is newer than the database head."
                              {:type :fork-point-after-head
                               :at at :head-max-tx (:max-tx src-head)}))
                 (loop [queue [src-head] visited #{}]
                   (if-let [stored (first queue)]
                     (let [mt (:max-tx stored)]
                       (cond
                         (= mt at) stored
                         (< mt at) (recur (vec (rest queue)) visited) ; dead path
                         :else
                         (let [parents (remove visited (get-in stored [:meta :datahike/parents]))
                               parent-vals
                               (loop [[p & r] (seq parents) acc []]
                                 (if p
                                   (let [pv (<?- (k/get src-store p nil opts))]
                                     (when-not pv
                                       (log/raise "Commit missing from store (garbage-collected?)."
                                                  {:type :fork-commit-missing :commit-id p :at at}))
                                     (recur r (conj acc pv)))
                                   acc))]
                           (recur (into (vec (rest queue)) parent-vals)
                                  (into visited parents)))))
                     (log/raise "No commit with this transaction id in history."
                                {:type :fork-point-not-found :at at}))))
               :else ; inst
               (let [at-ms (inst-ms at)]
                 (loop [queue [src-head] visited #{}]
                   (if-let [stored (first queue)]
                     (let [ts (commit-instant stored)]
                       (if (or (nil? ts) (<= (inst-ms ts) at-ms))
                         stored
                         (let [parents (remove visited (get-in stored [:meta :datahike/parents]))
                               parent-vals
                               (loop [[p & r] (seq parents) acc []]
                                 (if p
                                   (let [pv (<?- (k/get src-store p nil opts))]
                                     (when-not pv
                                       (log/raise "Commit missing from store (garbage-collected?)."
                                                  {:type :fork-commit-missing :commit-id p :at at}))
                                     (recur r (conj acc pv)))
                                   acc))]
                           (recur (into (vec (rest queue)) parent-vals)
                                  (into visited parents)))))
                     (log/raise "Fork point predates the database's history."
                                {:type :fork-point-not-found :at at})))))
             at-cid (get-in at-stored [:meta :datahike/commit-id])
             _ (when-not at-cid
                 (log/raise "Fork-point commit carries no commit-id."
                            {:type :fork-point-missing-commit-id :at at}))
             ;; Create the target store and copy every key.
             tgt-raw (<?- (ks/create-store tgt-store-config opts))
             tgt-store (add-cache-and-handlers tgt-raw (assoc source-config :store tgt-store-config))
             existing-tgt-db (<?- (k/get tgt-store :db nil opts))
             _ (when existing-tgt-db
                 (ks/release-store src-store-config src-store)
                 (ks/release-store tgt-store-config tgt-store)
                 (log/raise "Target database already exists."
                            {:type :db-already-exists :config tgt-store-config}))
             key-metas (<?- (k/keys src-store opts))
             _ (loop [[km & r] (seq key-metas)]
                 (when km
                   (let [kk (:key km)]
                     (<?- (k/assoc tgt-store kk (<?- (k/get src-store kk nil opts)) opts)))
                   (recur r)))
             ;; Re-identify the fork-point value as the TARGET and force :db to it.
             fork-config (-> (:config at-stored)
                             (assoc :store tgt-store-config
                                    :branch :db)
                             (dissoc :name)
                             (cond-> (:name target-config)
                               (assoc :name (:name target-config))))
             at-db (stored->db (assoc at-stored :config fork-config) tgt-store)
             _ (<?- (force-branch! at-db :db #{at-cid} opts))]
         (ks/release-store src-store-config src-store)
         (ks/release-store tgt-store-config tgt-store)
         fork-config))))))

(defn merge!
  "Create a merge commit to the current branch of this connection for parent
  commit uuids or branch keywords. It is the responsibility of the caller to
  make sure that tx-data contains the data to be merged into the branch from
  the parents. This function ensures that the parent commits are properly tracked.

  Routed through the writer for proper serialization with concurrent transactions.
  Returns a tx-report (sync) or promise/channel (async)."
  ([conn parents tx-data]
   (merge! conn parents tx-data nil))
  ([conn parents tx-data tx-meta]
   (parent-check parents)
   @(datahike.writer/merge-db! conn {:parents parents
                                     :tx-data tx-data
                                     :tx-meta tx-meta})))

(defn merge-async!
  "Async version of merge!. Returns a promise (CLJ) or channel (CLJS)."
  ([conn parents tx-data]
   (merge-async! conn parents tx-data nil))
  ([conn parents tx-data tx-meta]
   (parent-check parents)
   (datahike.writer/merge-db! conn {:parents parents
                                    :tx-data tx-data
                                    :tx-meta tx-meta})))
