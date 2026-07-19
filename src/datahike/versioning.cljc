(ns datahike.versioning
  "Git-like versioning tools for Datahike.
   All operations support both synchronous (CLJ default) and asynchronous modes."
  (:require [konserve.core :as k]
            [konserve.store :as ks]
            [datahike.config :as dc]
            [datahike.connections :refer [*connections*]]
            [datahike.gc-guard :as guard]
            [datahike.store :refer [store-identity add-cache-and-handlers]]
            [datahike.writing :refer [stored->db db->stored stored-db?
                                      #?@(:clj [release-db])
                                      commit! create-commit-id get-and-clear-pending-kvs!
                                      write-pending-kvs! branch-heads-as-commits]]
            [datahike.writer]
            [datahike.index.secondary :as sec]
            ;; cljs: S is a VAR (the supervisor) → :refer; go-try-/<?-/<?/go-loop-try
            ;; are MACROS → :refer-macros. (Was missing <? entirely and putting S
            ;; under :refer-macros, so both were :undeclared-var on cljs.)
            #?(:clj  [superv.async :refer [go-try- <?- <? S go-loop-try]]
               :cljs [superv.async :refer [S] :refer-macros [go-try- <?- <? go-loop-try]])
            [datahike.db.utils :refer [db?]]
            [datahike.tools :as dt]
            [replikativ.logging :as log]
            [konserve.utils :refer [#?(:clj async+sync) multi-key-capable? *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            #?(:cljs [clojure.core.async :refer [<!]]))
  #?(:cljs (:require-macros [clojure.core.async :refer [go]])))

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
  "Extract konserve store from a connection or db value.

   A connection is detected via `IDeref` rather than the concrete
   `Connection` class: `deftype` recompilation (circular loads, REPL
   reloads) can drift the class identity so `instance? Connection` sees a
   stale class and silently misroutes a live connection to the raw-store
   branch — which surfaced as a konserve `get-lock` NPE from
   `commit-as-db`. A db value is not `IDeref`, so the two stay distinct."
  [conn-or-db]
  (cond
    #?(:clj (instance? clojure.lang.IDeref conn-or-db) :cljs (satisfies? IDeref conn-or-db))
    (:store @conn-or-db)

    (db? conn-or-db)
    (:store conn-or-db)

    :else
    ;; Assume it's a raw store
    conn-or-db))

(defn- attached-cache-context
  "Derive committed cache ownership for a value loaded through an attached
   connection or committed DB. Raw stores and detached values return nil.

   The attached source owns exact attribute revisions only for its own commit.
   A different materialized commit keeps the same connection generation but
   uses its own commit ID as the conservative revision: stored commits do not
   retain the process-local attribute revision map, so cross-commit promotion
   would otherwise compare two absent revisions as unchanged."
  [conn-or-db commit-id]
  (let [source
        (cond
          #?(:clj (instance? clojure.lang.IDeref conn-or-db)
             :cljs (satisfies? IDeref conn-or-db))
          @conn-or-db

          (db? conn-or-db)
          conn-or-db

          :else nil)
        {:datahike.cache/keys [connection-id generation committed?]
         source-commit-id :datahike.cache/commit-id
         :as source-context}
        (:cache-context source)]
    (when (and committed? connection-id generation)
      (if (= source-commit-id commit-id)
        source-context
        {:datahike.cache/connection-id connection-id
         :datahike.cache/generation generation
         :datahike.cache/commit-id commit-id
         :datahike.cache/committed? true
         :datahike.cache/conservative-revision commit-id}))))

#?(:clj
   (defn- branch-secondary-indices
     "Branch the secondary state selected by `stored-db`.

      A live index is valid only when `from` names this connection's attached
      branch and the selected stored head is the exact db held by the
      connection. Commit ids, sibling branches, and stale attached heads must
      branch from the selected commit's durable key maps instead."
     [conn stored-db from new-branch store]
     (let [live-db @conn
           live-indices (:secondary-indices live-db)
           stored-key-maps (:secondary-index-keys stored-db)
           live-source? (and (keyword? from)
                             (= from (get-in live-db [:config :branch]))
                             (= (get-in stored-db [:meta :datahike/commit-id])
                                (get-in live-db [:meta :datahike/commit-id])))
           static-key-maps
           (if live-source?
             (keep (fn [[idx-ident key-map]]
                     (let [idx (get live-indices idx-ident)]
                       (when-not (and idx (satisfies? sec/IVersionedSecondaryIndex idx))
                         key-map)))
                   stored-key-maps)
             (vals stored-key-maps))]
       ;; Preflight every detached secondary before the first native fork.
       ;; A mixed-index branch is all-or-nothing at the capability boundary.
       (doseq [key-map static-key-maps]
         (sec/check-branch-from-key-map key-map store from new-branch))
       (if live-source?
         (reduce
          (fn [acc idx-ident]
            (let [idx (get live-indices idx-ident)
                  key-map (get stored-key-maps idx-ident)]
              (cond
                (and idx (satisfies? sec/IVersionedSecondaryIndex idx))
                (let [branched (sec/-sec-branch idx store from new-branch)
                      branched-key-map (sec/-sec-flush branched store new-branch)]
                  (when (instance? java.io.Closeable branched)
                    (.close ^java.io.Closeable branched))
                  (assoc acc idx-ident branched-key-map))

                key-map
                (assoc acc idx-ident
                       (sec/branch-from-key-map key-map store from new-branch))

                :else acc)))
          {}
          (into (set (keys stored-key-maps)) (keys live-indices)))
         (reduce-kv
          (fn [acc idx-ident key-map]
            (assoc acc idx-ident
                   (sec/branch-from-key-map key-map store from new-branch)))
          {}
          (or stored-key-maps {}))))))

#?(:clj
   (defn- force-secondary-key-maps
     "Guard and replace native secondary destinations before primary publish."
     [source-key-maps current-key-maps store branch]
     (doseq [[idx-ident source-key-map] source-key-maps]
       (sec/check-force-from-key-map source-key-map
                                     (get current-key-maps idx-ident)
                                     store branch))
     (reduce-kv
      (fn [forced idx-ident source-key-map]
        (assoc forced idx-ident
               (sec/force-from-key-map source-key-map
                                       (get current-key-maps idx-ident)
                                       store branch)))
      {}
      source-key-maps)))

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
   (branch-check new-branch)
   (let [opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  ;; GC GUARD: a secondary index's -sec-flush writes konserve keys, and
                  ;; the new branch's head record is written before `:branches` names it
                  ;; — until then NOTHING points at either, so a concurrent collector
                  ;; would sweep them (GC's whitelist comes from `:branches`).
                  (let [gc-sid   (:id (:store (:config @conn)))
                        gc-token (guard/writing! gc-sid)]
                    (try
                      (let [store (:store @conn)
                            commit-source? (uuid? from)
                            commit-graph? (get (:config @conn) :commit-graph? true)
                            _ (when (and commit-source? (false? commit-graph?))
                                (log/raise "This store was created with :commit-graph? false, so branching from a commit-id is unavailable."
                                           {:type :commit-graph-disabled
                                            :from from
                                            :commit-graph? false}))
                            _ (when-not (or (keyword? from) commit-source?)
                                (log/raise "From must be a branch keyword or commit UUID."
                                           {:type :invalid-branch-source :from from}))
                            existing-branches (<?- (k/get store :branches nil opts))
                            _ (when (and existing-branches (existing-branches new-branch))
                                (log/raise "Branch already exists." {:type :branch-already-exists
                                                                     :new-branch new-branch}))
                            stored-db (<?- (k/get store from nil opts))]
                        (when-not (stored-db? stored-db)
                          (log/raise (if commit-source?
                                       "Commit record does not exist."
                                       "Branch does not exist.")
                                     {:type (if commit-source? :commit-not-found :branch-does-not-exist)
                                      :from from
                                      :commit-graph? commit-graph?}))
                        ;; Branch secondary indices from the SAME selected root as
                        ;; the primary database. Only an exact attached head may
                        ;; reuse the connection's live index instances.
                        (let [sec-keys (:secondary-index-keys stored-db)
                              branched-sec-keys
                              #?(:clj
                                 (when (or (seq sec-keys) (seq (:secondary-indices @conn)))
                                   (branch-secondary-indices conn stored-db from new-branch store))
                                 :cljs nil)
                              updated-db (cond-> (assoc-in stored-db [:config :branch] new-branch)
                                           (seq branched-sec-keys) (assoc :secondary-index-keys branched-sec-keys))]
                          (<?- (k/assoc store new-branch updated-db opts))
                          ;; :branches is the GC discovery pointer and is published last.
                          (<?- (k/update store :branches #(conj (set %) new-branch) opts))))
                      (finally
                        (guard/done! gc-sid gc-token)))))))))

(defn delete-branch!
  "Removes this branch from set of known branches. The branch will still be
  accessible until the next gc. Connections to the branch must be released
  before deletion."
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
                    (let [store-id (store-identity (get-in @conn [:config :store]))
                          active-connections
                          (filterv (fn [[candidate-store candidate-branch & _]]
                                     (and (= store-id candidate-store)
                                          (= branch candidate-branch)))
                                   (keys @*connections*))]
                      (when (seq active-connections)
                        (log/raise "Cannot delete a branch with an active connection. Release it first."
                                   {:type :branch-has-active-connection
                                    :branch branch
                                    :connections active-connections})))
                    (<?- (k/update store :branches #(disj (set %) branch) opts))))))))

(defn force-branch!
  "Force the branch to point to the provided db value. Branch will be created if
  it does not exist. Parents must point to a set of branches or commits.

  By default this overwrites the branch head unconditionally, like git reset --hard.
  Pass `:expected-current-commit` in opts to reject a stale branch head before
  mutation. The new head is read back and verified before this function returns.

  The caller must hold exclusive write access to this store before forcing a
  branch. The expected-head check catches stale plans, but konserve does not
  provide a cross-operation compare-and-set against an independent writer.

  Existing connections to this branch will see stale state and must be released
  and reconnected. Use with care — you can render data inaccessible."
  ([db branch parents] (force-branch! db branch parents {:sync? true}))
  ([db branch parents opts]
   (db-check db)
   (branch-check branch)
   (parent-check parents)
   (let [unknown-opts (seq (remove #{:sync? :expected-current-commit} (keys opts)))
         _ (when unknown-opts
             (log/raise "Unknown force-branch! option."
                        {:type :invalid-force-branch-options
                         :unknown-options (vec unknown-opts)}))
         guard? (contains? opts :expected-current-commit)
         expected-current-commit (:expected-current-commit opts)
         _ (when (and guard? (some? expected-current-commit))
             (commit-id-check expected-current-commit))
         opts (merge {:sync? true} (select-keys opts [:sync?]))
         sync? (:sync? opts)]
     (async+sync sync? *default-sync-translation*
                 (go-try-
                  ;; GC GUARD: same values-then-pointer sequence as commit!, but this
                  ;; runs on the CALLER's thread and needs no writer at all — which is
                  ;; exactly why the guard lives in the store rather than in the writer.
                  (let [gc-sid   (:id (:store (:config db)))
                        gc-token (guard/writing! gc-sid)]
                    (try
                      (let [store (:store db)
                            current-stored (<?- (k/get store branch nil opts))
                            current-commit (get-in current-stored [:meta :datahike/commit-id])
                            resolved-parents (branch-heads-as-commits store parents)
                            _ (when (and guard? (not= expected-current-commit current-commit))
                                (log/raise "Branch head changed before force-branch!."
                                           {:type :stale-branch-head
                                            :branch branch
                                            :expected-current-commit expected-current-commit
                                            :current-commit current-commit}))
                            ;; Flush first, then compute the audit-grade cid from
                            ;; the post-flush stored form (true merkle root).
                            db-with-parents (-> db
                                                (assoc-in [:config :branch] branch)
                                                (assoc-in [:meta :datahike/parents] resolved-parents))
                            [schema-meta-kv-to-write pre-cid-store*]
                            (db->stored db-with-parents true)
                            pre-cid-store
                            #?(:clj
                               (if-let [source-key-maps
                                        (seq (:secondary-index-keys pre-cid-store*))]
                                 (assoc pre-cid-store* :secondary-index-keys
                                        (force-secondary-key-maps
                                         (into {} source-key-maps)
                                         (:secondary-index-keys current-stored)
                                         store branch))
                                 pre-cid-store*)
                               :cljs pre-cid-store*)
                            cid (create-commit-id db-with-parents pre-cid-store)
                            db-to-store (assoc-in pre-cid-store
                                                  [:meta :datahike/commit-id] cid)
                            pending-kvs (get-and-clear-pending-kvs! store)
                            ;; Same opt-out as datahike.writing/commit!: no
                            ;; commit-graph store means no separate cid record.
                            commit-graph? (get (:config db) :commit-graph? true)]
                        ;; Write every immutable value before the mutable head.
                        (if (multi-key-capable? store)
                          (let [[meta-key meta-val] schema-meta-kv-to-write
                                writes (cond-> (vec pending-kvs)
                                         schema-meta-kv-to-write (conj [meta-key meta-val])
                                         commit-graph?           (conj [cid db-to-store]))
                                metas (into {} (map (fn [[key _]] [key {:immutable? true}])) writes)]
                            (<?- (k/multi-assoc store writes metas opts)))
                          (do
                            (<?- (write-pending-kvs! store pending-kvs sync?))
                            (when schema-meta-kv-to-write
                              (<?- (k/assoc store
                                            (first schema-meta-kv-to-write)
                                            (second schema-meta-kv-to-write)
                                            {:immutable? true}
                                            opts)))
                            (when commit-graph?
                              (<?- (k/assoc store cid db-to-store {:immutable? true} opts)))))

                        ;; Recheck inside the mutable head update. The GC guard
                        ;; protects the values-to-head window; the expected head
                        ;; rejects a stale plan in this quiesced operation.
                        (<?- (k/update
                              store branch
                              (fn [stored]
                                (let [stored-commit (get-in stored [:meta :datahike/commit-id])]
                                  (when (and guard?
                                             (not= expected-current-commit stored-commit))
                                    (log/raise "Branch head changed during force-branch!."
                                               {:type :stale-branch-head
                                                :branch branch
                                                :expected-current-commit expected-current-commit
                                                :current-commit stored-commit}))
                                  db-to-store))
                              opts))

                        ;; Publish the GC discovery pointer only after its head exists.
                        (<?- (k/update store :branches #(conj (set %) branch) opts))
                        (let [stored-head (<?- (k/get store branch nil opts))
                              stored-commit (get-in stored-head [:meta :datahike/commit-id])]
                          (when-not (= cid stored-commit)
                            (log/raise "Forced branch head did not match on readback."
                                       {:type :force-branch-readback-mismatch
                                        :branch branch
                                        :expected-commit cid
                                        :stored-commit stored-commit})))
                        nil)
                      (finally
                        (guard/done! gc-sid gc-token)))))))))

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
   First argument can be a connection, db value, or raw konserve store.

   Loading through an attached connection or committed DB preserves that
   connection generation's committed cache identity. Loading through a raw
   store or detached DB remains detached."
  ([conn-or-store cid] (commit-as-db conn-or-store cid {:sync? true}))
  ([conn-or-store cid opts]
   (commit-id-check cid)
   (let [store (extract-store conn-or-store)
         opts (select-keys opts [:sync? :secondary-indices?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  (when-let [raw-db (<?- (k/get store cid nil opts))]
                    (let [cache-context
                          (attached-cache-context conn-or-store cid)]
                      (cond-> (stored->db raw-db store opts)
                        cache-context
                        (assoc :cache-context cache-context)))))))))

(defn release-materialized-db
  "Release native resources owned by a database returned from commit-as-db.
   Ordinary connection database values and primary-only materializations are
   no-ops. The caller must ensure no read is still using the value."
  [db]
  (db-check db)
  #?(:clj (release-db db)
     :cljs []))

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
