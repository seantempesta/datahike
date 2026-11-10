(ns datahike.versioning
  "Git-like versioning tools for Datahike.
   All operations support both synchronous (CLJ default) and asynchronous modes."
  (:require [konserve.core :as k]
            [konserve.store :as ks]
            [datahike.config :as dc]
            [datahike.connections :refer [*connections*]]
            [datahike.gc-guard :as guard]
            [datahike.store :refer [store-identity add-cache-and-handlers]]
            [datahike.roster :refer [roster roster-add! roster-remove!]]
            [datahike.writing :refer [stored->db db->stored stored-db? stored-revisions
                                      #?@(:clj [release-db])
                                      commit! create-commit-id get-and-clear-pending-kvs!
                                      write-pending-kvs! branch-heads-as-commits
                                      read-head resolve-head compact-head compact-head? commit-graph-writes
                                      commit-graph-records commit-graph-key]]
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
            [clojure.core.async :as async]
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

   The attached source owns exact attribute revisions for its own commit. A
   different materialized commit keeps the same connection generation and
   takes the revisions its stored record carries (`stored-revisions`)."
  [conn-or-db commit-id stored-db]
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
        (merge {:datahike.cache/connection-id connection-id
                :datahike.cache/generation generation
                :datahike.cache/commit-id commit-id
                :datahike.cache/committed? true}
               (stored-revisions stored-db))))))

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
     "The secondary key maps a forced head installs: the source's, as they are.
      (A per-index-type guarded native force was ours, for Proximum; dropped
      with that integration.)"
     [source-key-maps _current-key-maps _store _branch]
     source-key-maps))

(defn- roster-add-held!
  "Append branch to the roster while the caller holds the store's roster permit."
  [store-id permit store branch opts]
  (guard/require-reachability-permit! store-id :roster permit)
  (roster-add! store branch opts))

(defn- roster-remove-held!
  "Append branch's removal to the roster and delete its head, while the caller
  holds the store's roster permit. The head goes too: a branch exists exactly when
  its head key does, which is what branch! and delete-branch! test (O(1))."
  [store-id permit store branch opts]
  (guard/require-reachability-permit! store-id :roster permit)
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (<?- (roster-remove! store branch opts))
               (<?- (k/dissoc store branch opts)))))

;; ========================= public API =========================

(defn branches
  "List all known branch names. Returns set of keywords."
  ([conn] (branches conn {:sync? true}))
  ([conn opts]
   (let [store (extract-store conn)
         opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try- (<?- (roster store opts)))))))

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
                     (if-let [raw-db (<? S (read-head store to-check {:sync? false}))]
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
   (let [opts (select-keys opts [:sync? :datahike.gc-guard/reachability-permit])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  ;; The roster permit starts before either source or roster is
                  ;; read and ends after head + roster publication. A caller such
                  ;; as Seon start may pass its already-held permit explicitly;
                  ;; nested acquisition is rejected by validation, not made
                  ;; implicitly reentrant.
                  (let [gc-sid (:id (:store (:config @conn)))
                        supplied-permit (:datahike.gc-guard/reachability-permit opts)
                        roster-permit
                        (or (some->> supplied-permit
                                     (guard/require-reachability-permit! gc-sid :roster))
                            (<?- (guard/acquire-reachability-permit!
                                  gc-sid :roster {:sync? (:sync? opts)})))
                        owned-permit? (nil? supplied-permit)
                        store-opts (dissoc opts :datahike.gc-guard/reachability-permit)]
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
                            _ (when (<?- (k/exists? store new-branch store-opts))
                                (log/raise "Branch already exists." {:type :branch-already-exists
                                                                     :new-branch new-branch}))
                            ;; A branch made from a branch whose head is a compact head is
                            ;; that same pointer under a new name: nothing about the commit
                            ;; is read, unless secondary indices must be branched with it.
                            raw-head (when (keyword? from) (<?- (k/get store from nil store-opts)))
                            pointer? (and commit-graph?
                                          (compact-head? raw-head)
                                          (not (:datahike/secondary? raw-head))
                                          (empty? (:secondary-indices @conn)))]
                        (if pointer?
                          (do (<?- (k/assoc store new-branch raw-head store-opts))
                              (<?- (roster-add-held! gc-sid roster-permit store new-branch store-opts)))
                        (let [stored-db (if (keyword? from)
                                          (<?- (resolve-head store from raw-head store-opts))
                                          (<?- (read-head store from store-opts)))]
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
                              updated-db (if (and commit-graph? (empty? branched-sec-keys))
                                           (compact-head (get-in stored-db [:meta :datahike/commit-id]) stored-db)
                                           (cond-> (assoc-in stored-db [:config :branch] new-branch)
                                             (seq branched-sec-keys) (assoc :secondary-index-keys branched-sec-keys)))]
                          (<?- (k/assoc store new-branch updated-db store-opts))
                          ;; :branches is the GC discovery pointer and is published last.
                          (<?- (roster-add-held! gc-sid roster-permit store new-branch store-opts))))))
                      (finally
                        (when owned-permit?
                          (guard/release-reachability-permit! roster-permit))))))))))

(defn delete-branch!
  "Removes this branch from set of known branches. The branch will still be
  accessible until the next gc. Connections to the branch must be released
  before deletion."
  ([conn branch] (delete-branch! conn branch {:sync? true}))
  ([conn branch opts]
   (when (= branch :db)
     (log/raise "Cannot delete main :db branch. Delete database instead."
                {:type :cannot-delete-main-db-branch}))
   (let [opts (select-keys opts [:sync? :datahike.gc-guard/reachability-permit])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  (let [store (:store @conn)
                        store-id (store-identity (get-in @conn [:config :store]))
                        supplied-permit (:datahike.gc-guard/reachability-permit opts)
                        roster-permit
                        (or (some->> supplied-permit
                                     (guard/require-reachability-permit! store-id :roster))
                            (<?- (guard/acquire-reachability-permit!
                                  store-id :roster {:sync? (:sync? opts)})))
                        owned-permit? (nil? supplied-permit)
                        store-opts (dissoc opts :datahike.gc-guard/reachability-permit)]
                    (try
                      (do
                        (when-not (<?- (k/exists? store branch store-opts))
                          (log/raise "Branch does not exist." {:type :branch-does-not-exist
                                                               :branch branch}))
                        (let [active-connections
                              (filterv (fn [[candidate-store candidate-branch & _]]
                                         (and (= store-id candidate-store)
                                              (= branch candidate-branch)))
                                       (keys @*connections*))]
                          (when (seq active-connections)
                            (log/raise "Cannot delete a branch with an active connection. Release it first."
                                       {:type :branch-has-active-connection
                                        :branch branch
                                        :connections active-connections})))
                        (<?- (roster-remove-held! store-id roster-permit store branch store-opts)))
                      (finally
                        (when owned-permit?
                          (guard/release-reachability-permit! roster-permit))))))))))

(defn force-branch!
  "Force the branch to point to the provided db value. Branch will be created if
  it does not exist. Parents must point to a set of branches or commits.

  By default this overwrites the branch head unconditionally, like git reset --hard.
  Pass `:expected-current-commit` in opts to reject a stale branch head before
  mutation. The new head is read back and verified before this function returns;
  its commit id is the return value.

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
   (let [unknown-opts (seq (remove #{:sync? :expected-current-commit
                                    :datahike.gc-guard/reachability-permit}
                                  (keys opts)))
         _ (when unknown-opts
             (log/raise "Unknown force-branch! option."
                        {:type :invalid-force-branch-options
                         :unknown-options (vec unknown-opts)}))
         guard? (contains? opts :expected-current-commit)
         expected-current-commit (:expected-current-commit opts)
         _ (when (and guard? (some? expected-current-commit))
             (commit-id-check expected-current-commit))
         opts (merge {:sync? true}
                     (select-keys opts [:sync? :datahike.gc-guard/reachability-permit]))
         sync? (:sync? opts)]
     (async+sync sync? *default-sync-translation*
                 (go-try-
                  (let [gc-sid (:id (:store (:config db)))
                        supplied-permit (:datahike.gc-guard/reachability-permit opts)
                        roster-permit
                        (or (some->> supplied-permit
                                     (guard/require-reachability-permit! gc-sid :roster))
                            (<?- (guard/acquire-reachability-permit!
                                  gc-sid :roster {:sync? sync?})))
                        owned-permit? (nil? supplied-permit)
                        store-opts (dissoc opts :datahike.gc-guard/reachability-permit)]
                    (try
                      (let [store (:store db)
                            current-stored (<?- (read-head store branch store-opts))
                            ;; the roster gets an entry only for a branch this creates
                            existed-before? (some? current-stored)
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
                            commit-graph? (get (:config db) :commit-graph? true)
                            graph-writes (when commit-graph?
                                           (commit-graph-writes store cid resolved-parents branch))]
                        ;; Write every immutable value before the mutable head.
                        (if (multi-key-capable? store)
                          (let [[meta-key meta-val] schema-meta-kv-to-write
                                writes (cond-> (vec pending-kvs)
                                         schema-meta-kv-to-write (conj [meta-key meta-val])
                                         commit-graph?           (into graph-writes)
                                         commit-graph?           (conj [cid db-to-store]))
                                metas (into {} (map (fn [[key _]] [key {:immutable? true}])) writes)]
                            (<?- (k/multi-assoc store writes metas store-opts)))
                          (do
                            (<?- (write-pending-kvs! store pending-kvs sync?))
                            (when schema-meta-kv-to-write
                              (<?- (k/assoc store
                                            (first schema-meta-kv-to-write)
                                            (second schema-meta-kv-to-write)
                                            {:immutable? true}
                                            store-opts)))
                            (doseq [[gk gv] graph-writes]
                              (<?- (k/assoc store gk gv {:immutable? true} store-opts)))
                            (when commit-graph?
                              (<?- (k/assoc store cid db-to-store {:immutable? true}
                                           store-opts)))))

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
                                  (if commit-graph? (compact-head cid db-to-store) db-to-store)))
                              store-opts))

                        ;; Publish the GC discovery pointer only after its head exists.
                        (when-not existed-before?
                          (<?- (roster-add-held! gc-sid roster-permit store branch store-opts)))
                        (let [stored-head (<?- (k/get store branch nil store-opts))
                              stored-commit (get-in stored-head [:meta :datahike/commit-id])]
                          (when-not (= cid stored-commit)
                            (log/raise "Forced branch head did not match on readback."
                                       {:type :force-branch-readback-mismatch
                                        :branch branch
                                        :expected-commit cid
                                        :stored-commit stored-commit})))
                        ;; The installed head, verified under the roster
                        ;; permit: callers identify their own update by it
                        ;; instead of rereading a head another writer may move.
                        cid)
                      (finally
                        (when owned-permit?
                          (guard/release-reachability-permit! roster-permit))))))))))

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

(defn- graph-reader
  "A per-call reader of commit graph records: (read cid) is cid's record or
   nil when the store holds no commit `cid`."
  [store]
  (let [seen (volatile! {})]
    (fn [cid]
      (if-let [record (get @seen cid)]
        record
        (let [{:keys [stored derived]} (commit-graph-records store [cid])]
          (vswap! seen into (concat stored derived))
          (get @seen cid))))))

(defn commit-graph-record
  "Commit `cid`'s graph record, {:datahike/parents #{cid} :datahike/generation n
   :datahike/branch kw} (the branch it was written on, so a first-parent walk
   needs no full commit record):
   the stored one, else derived from the commit records (nothing written);
   nil when the store holds no commit `cid`. A pure function of an immutable
   commit id, so a caller may cache it forever and hand it to `merge-base`.
   A commit without a stored record is derived through its whole unrecorded
   lineage on each call: run `backfill-commit-graph!` once before caching
   many of them."
  [conn-or-store cid]
  (let [{:keys [stored derived]} (commit-graph-records (extract-store conn-or-store) [cid])]
    (or (get stored cid) (get derived cid))))

(defn- reaches?
  "Whether `x` is an ancestor of `y` (or `y` itself). Commits below `x`'s
   generation cannot reach it, so the walk stops there."
  [node x y]
  (let [gx (:datahike/generation (node x))]
    (loop [todo [y] seen #{}]
      (if-let [c (peek todo)]
        (cond
          (= c x) true
          (contains? seen c) (recur (pop todo) seen)
          :else (let [n (node c)]
                  (recur (into (pop todo)
                               (when (and n (> (:datahike/generation n) gx))
                                 (:datahike/parents n)))
                         (conj seen c))))
        false))))

(defn- paint-down-to-common
  "git's paint_down_to_common over `node` (cid -> graph record): every commit
   reached from both `a` and `b` that no other such commit descends from
   through the walk, highest generation first. Flags: 1 reached from `a`, 2
   from `b`, 4 stale. It stops when only stale entries remain."
  [node a b]
  (let [entry (fn [c] [(:datahike/generation (node c)) c])
        queue0 (sorted-set-by (fn [[g1 c1] [g2 c2]]
                                (let [g (compare g2 g1)] (if (zero? g) (compare c1 c2) g)))
                              (entry a) (entry b))
        stale? (fn [f] (pos? (bit-and f 4)))]
    (loop [queue queue0
           flags (merge-with bit-or {a 1} {b 2})
           nonstale (count queue0)
           result #{}]
      (if (zero? nonstale)
        result
        (let [[_ c :as e] (first queue)
              queue (disj queue e)
              f (get flags c)
              nonstale (if (stale? f) nonstale (dec nonstale))
              both? (= 3 (bit-and f 7))
              fl (cond-> (bit-and f 7) both? (bit-or 4))
              [queue flags nonstale]
              (reduce (fn [[queue flags nonstale] p]
                        (let [pf (get flags p 0)]
                          (if (or (= fl (bit-and pf fl)) (nil? (node p)))
                            [queue flags nonstale]
                            (let [pf' (bit-or pf fl)
                                  queued? (contains? queue (entry p))
                                  nonstale (cond
                                             (not queued?) (cond-> nonstale (not (stale? pf')) inc)
                                             (and (not (stale? pf)) (stale? pf')) (dec nonstale)
                                             :else nonstale)]
                              [(conj queue (entry p)) (assoc flags p pf') nonstale]))))
                      [queue (assoc flags c fl) nonstale]
                      (:datahike/parents (node c)))]
          (recur queue flags nonstale (cond-> result both? (conj c))))))))

(defn merge-base
  "The merge base of `a` and `b`: their best common ancestor commit id.

   `a` and `b` are commit ids or branch keywords. Answers nil when they share
   no ancestor. This is git's merge-base, walked over the commit graph
   records by generation. Raises :merge-base-criss-cross naming every best
   common ancestor when there is more than one, :commit-not-found or
   :branch-does-not-exist for an unknown input, and :commit-graph-disabled
   on a store created with :commit-graph? false.

   A pure function of two commit ids: callers may cache it forever.

   `read-record` (cid -> graph record or nil, default a reader of this call
   alone) lets callers that ask for many merge bases share the record reads:
   back it with a cache of `commit-graph-record` keyed by commit id."
  ([conn-or-store a b] (merge-base conn-or-store a b nil))
  ([conn-or-store a b read-record]
   (let [config (cond
                  #?(:clj (instance? clojure.lang.IDeref conn-or-store)
                     :cljs (satisfies? IDeref conn-or-store)) (:config @conn-or-store)
                  (db? conn-or-store) (:config conn-or-store))
         store (extract-store conn-or-store)
         cid-of (fn [x]
                  (cond
                    (uuid? x) x
                    (keyword? x)
                    (let [head (read-head store x {:sync? true})]
                      (when-not head
                        (log/raise "Branch does not exist."
                                   {:type :branch-does-not-exist :branch x}))
                      (when (false? (get-in head [:config :commit-graph?]))
                        (log/raise "This store was created with :commit-graph? false, so it has no merge base."
                                   {:type :commit-graph-disabled :commit-graph? false}))
                      (get-in head [:meta :datahike/commit-id]))
                    :else (log/raise "A merge base is computed for commit ids or branch keywords."
                                     {:type :invalid-merge-base-input :input x})))]
     (when (false? (:commit-graph? config))
       (log/raise "This store was created with :commit-graph? false, so it has no merge base."
                  {:type :commit-graph-disabled :commit-graph? false}))
     (let [a (cid-of a)
           b (cid-of b)
           node (or read-record (graph-reader store))]
       (doseq [c [a b]]
         (when-not (node c)
           (log/raise "Commit record does not exist." {:type :commit-not-found :commit-id c})))
       (let [found (paint-down-to-common node a b)
             best (into #{} (remove (fn [x] (some #(and (not= x %) (reaches? node x %)) found))) found)]
         (when (< 1 (count best))
           (log/raise "More than one best common ancestor (criss-cross merge)."
                      {:type :merge-base-criss-cross :merge-bases best}))
         (first best))))))

(defn backfill-commit-graph!
  "Writes the commit graph record of every commit a branch head reaches
   that has no complete one: the commits written before graph records
   existed, and records written before `:datahike/branch` was added.

   One derivation for the whole store (`commit-graph-records` over every
   branch head), written parents first in batches of `:batch-size` (default
   1000), so a stored record always has its ancestors' records and a torn
   run resumes where it stopped. Records are derived from immutable commits,
   so the write is idempotent and needs no writer: a concurrent commit
   writing the same record writes the same value, and a record whose commit
   gc sweeps meanwhile is an orphan the next gc takes. Once done, a run reads
   one record per branch head and writes nothing.

   Returns {:datahike/branches n :datahike/written n}. Raises
   :commit-graph-disabled on a store created with :commit-graph? false."
  ([conn-or-store] (backfill-commit-graph! conn-or-store {}))
  ([conn-or-store {:keys [batch-size] :or {batch-size 1000}}]
   (let [store (extract-store conn-or-store)
         branches (roster store)
         heads (keep #(read-head store % {:sync? true}) branches)]
     (when (some #(false? (get-in % [:config :commit-graph?])) heads)
       (log/raise "This store was created with :commit-graph? false, so it has no commit graph records."
                  {:type :commit-graph-disabled :commit-graph? false}))
     (let [{:keys [derived]} (commit-graph-records store (into #{} (keep #(get-in % [:meta :datahike/commit-id])) heads))
           writes (into [] (map (fn [[c r]] [(commit-graph-key c) r]))
                        (sort-by (comp :datahike/generation val) derived))]
       (doseq [batch (partition-all batch-size writes)]
         (if (multi-key-capable? store)
           (k/multi-assoc store (vec batch) (into {} (map (fn [[gk _]] [gk {:immutable? true}])) batch) {:sync? true})
           (doseq [[gk gv] batch] (k/assoc store gk gv {:immutable? true} {:sync? true}))))
       {:datahike/branches (count branches) :datahike/written (count writes)}))))

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
                          (attached-cache-context conn-or-store cid raw-db)]
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
                  (when-let [raw-db (<?- (read-head store branch opts))]
                    (stored->db raw-db store)))))))

(defn merge!
  "Create a merge commit to the current branch of this connection for parent
  commit uuids or branch keywords. It is the responsibility of the caller to
  make sure that tx-data contains the data to be merged into the branch from
  the parents. This function ensures that the parent commits are properly tracked.

  Routed through the writer for proper serialization with concurrent transactions.
  The arg-map form `{:parents :tx-data :tx-meta :datahike/expected-basis-t}`
  shares `transact!`'s basis fence. Returns a tx-report."
  ([conn {:keys [parents] :as arg-map}]
   (parent-check parents)
   @(datahike.writer/merge-db! conn arg-map))
  ([conn parents tx-data]
   (merge! conn parents tx-data nil))
  ([conn parents tx-data tx-meta]
   (parent-check parents)
   @(datahike.writer/merge-db! conn {:parents parents
                                     :tx-data tx-data
                                     :tx-meta tx-meta})))

(defn merge-async!
  "Async version of merge!. Returns a promise (CLJ) or channel (CLJS)."
  ([conn {:keys [parents] :as arg-map}]
   (parent-check parents)
   (datahike.writer/merge-db! conn arg-map))
  ([conn parents tx-data]
   (merge-async! conn parents tx-data nil))
  ([conn parents tx-data tx-meta]
   (parent-check parents)
   (datahike.writer/merge-db! conn {:parents parents
                                    :tx-data tx-data
                                    :tx-meta tx-meta})))
