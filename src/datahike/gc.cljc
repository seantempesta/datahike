(ns datahike.gc
  (:require [clojure.set :as set]
            [datahike.config :as dc]
            [datahike.constants :as c]
            [datahike.datom :as dd]
            [datahike.gc-guard :as guard]
            [datahike.index.interface :refer [-mark -seed-root! -slice with-storage collection-key]]
            [datahike.index.secondary :as sec]
            [datahike.writing :refer [read-head commit-graph-key]]
            [datahike.roster :refer [roster roster-store-keys]]
            [datahike.schema :as schema]
            [konserve.core :as k]
            [konserve.gc :refer [sweep!]]
            [replikativ.logging :as log]
            [superv.async :refer [<? S go-try <<? go-try- <?-]]
            [konserve.utils :as ku :refer [#?(:clj async+sync) *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            ;; go-loop drives start-background-gc!'s scheduler; it's a MACRO, so
            ;; cljs needs it via :require-macros (mirrors datahike.versioning).
            #?(:clj  [clojure.core.async :as async :refer [go-loop]]
               :cljs [clojure.core.async :as async])
            [datahike.schema-cache :as sc])
  #?(:clj  (:import [java.util Date])
     :cljs (:require-macros [clojure.core.async :refer [go-loop]])))

;; meta-data does not get passed in macros
(defn get-time [d]
  (.getTime ^Date d))

(defn- require-branches
  "The roster's branches, or a refusal to collect: a store that holds a database names
  at least one branch, so a roster read back empty is a failed read, and a sweep against
  it would keep only the roster and delete every head, commit and node (upstream
  51cf69d3, #1117)."
  [branches]
  (if (seq branches)
    branches
    (log/raise "Store names no branches; refusing to collect, as an empty mark would sweep the whole store."
               {:type :gc-branches-unreadable :branches branches})))

(defn- require-complete-mark
  "`marked`, one mark per branch, or a refusal to sweep. A walk that threw comes back
  as its exception (go-try-), which is raised as it is; a walk whose go block died
  yields nothing, so the merge holds one mark fewer than the branches: unchecked,
  either one's whole tree is missing from what the sweep keeps (upstream 51cf69d3,
  #1117)."
  [branches marked]
  (let [marked (vec marked)]
    (when-let [e (some #(when (instance? #?(:clj Throwable :cljs js/Error) %) %) marked)]
      (throw e))
    (when-not (and (= (count branches) (count marked)) (every? map? marked))
      (log/raise "Garbage collection mark is incomplete; refusing to sweep."
                 {:type :gc-incomplete-mark :branches (count branches) :marked (count marked)}))
    marked))

(defn- attr-store-refs
  "The object ids named by the VALUES of `attr` in the AEVT index `aevt`. For a
   key-bearing value type THE VALUE IS THE KEY, so this is just the attribute's
   values.

   Slices exactly the attribute's range, so the cost is O(its datoms), not O(the
   database)."
  [aevt attr]
  (into #{}
        (map :v)
        (-slice aevt
                (dd/datom c/e0 attr nil c/tx0)
                (dd/datom c/emax attr nil c/txmax)
                :aevt)))

(defn- store-refs
  "The object ids this record's datom VALUES name — what a `:db.type/store-ref`
   keeps alive (`datahike.schema/key-bearing-value-types`).

   THE MARK DOES NOT SEE THESE OTHERWISE. It walks the index TREES and collects
   node addresses; it never looks inside a datom's value. So an object named only
   by a value is unreachable from the mark's point of view, and the sweep deletes
   it. That is the whole reason this exists.

   ZERO COST WHEN UNUSED: if the schema declares no attribute of a key-bearing
   type — which is every database that does not use the feature — this returns
   immediately, having sliced nothing.

   Under `:keep-history?` the TEMPORAL index is scanned too: a retracted store-ref
   datom is still readable `as-of` an earlier tx, so the object it names must
   outlive the retraction. (`:db/noHistory` attributes are NOT retained there, which
   is why `schema/key-bearing-misuse` refuses to combine the two.)"
  [config schema ident-ref-map aevt taevt]
  (let [attrs (into []
                    (keep (fn [[ident attr-def]]
                            (when (contains? schema/key-bearing-value-types
                                             (:db/valueType attr-def))
                              ;; With :attribute-refs? the datoms hold the attribute's
                              ;; EID, not its ident — slice by what is actually stored.
                              (if (:attribute-refs? config)
                                (get ident-ref-map ident)
                                ident))))
                    schema)]
    (if (empty? attrs)
      #{}
      (reduce (fn [acc attr]
                (cond-> (set/union acc (attr-store-refs aevt attr))
                  taevt (set/union (attr-store-refs taevt attr))))
              #{} attrs))))

(defn- reachable-in-branch
  "The keys `branch` reaches from its head through commits newer than
  `after-date`, and the object ids their datom values name (`store-refs`,
  upstream #881): `{:reachable keys :store-refs refs}`. `visited` (commit keys) and `seen`
  (index node addresses) are atoms shared by every branch of one collection: a commit another branch
  already claimed is walked there, and a node already marked had its whole
  subtree marked, so the mark costs the store's distinct commits and nodes,
  not branches times commits times tree size."
  [store branch after-date config visited seen schema-cache sync?]
  (async+sync sync? *default-sync-translation*
   (go-try-
          (let [head-cid (<?- (k/get-in store [branch :meta :datahike/commit-id] nil {:sync? sync?}))]
            (loop [[to-check & r] [branch]
                   reachable      (hash-set branch head-cid)
                   refs           #{}]
              (if to-check
                (if (contains? (first (swap-vals! visited conj to-check)) to-check) ;; claimed: skip
                  (recur r reachable refs)
                  (if-let [record (<?- (read-head store to-check {:sync? sync?}))]
                    (let [{:keys                         [eavt-key avet-key aevt-key
                                                          temporal-eavt-key temporal-avet-key temporal-aevt-key
                                                          eavt-root aevt-root avet-root
                                                          temporal-eavt-root temporal-aevt-root temporal-avet-root
                                                          schema-meta-key secondary-index-keys]
                           {:keys [datahike/parents
                                   datahike/created-at
                                   datahike/updated-at]} :meta}
                          record
                          in-range? (> (get-time (or updated-at created-at))
                                       (get-time after-date))]
                      (let [sec-reachable (when (seq secondary-index-keys)
                                            (reduce-kv
                                             (fn [acc _idx-ident key-map]
                                               (set/union acc (sec/mark-from-key-map key-map store)))
                                             #{} secondary-index-keys))
                          ;; Stored roots are storage-detached; bind them to
                          ;; this store's storage so -mark can walk the tree.
                          ;; Root fusion: inlined roots aren't separate konserve
                          ;; objects, so -mark on the lazy index would try to
                          ;; restore the root by address and fail. Seed the
                          ;; inlined root into the with-storage COPY (owned,
                          ;; unpublished) — never into the stored record's
                          ;; index, which may be shared through the store's
                          ;; cache (mirrors stored->db) — so walk-addresses
                          ;; uses it and only its children are fetched.
                          ;;
                          ;; `bind` returns that copy so the SAME seeded instance
                          ;; serves both -mark (walk the tree) and -slice (read the
                          ;; datoms, for store-refs).
                            bind (fn [idx root]
                                   (cond-> (with-storage (:index config) idx (:storage store))
                                     root (-seed-root! root)))
                            mark (fn [idx] (-mark idx seen))
                            aevt'  (bind aevt-key aevt-root)
                            taevt' (when (:keep-history? config)
                                     (bind temporal-aevt-key temporal-aevt-root))
                            ;; The schema names which attributes can hold store-refs;
                            ;; content-addressed and rarely changed, so memoized across
                            ;; the whole collection (upstream #881).
                            schema-meta (when schema-meta-key
                                          (if-let [cached (get @schema-cache schema-meta-key)]
                                            cached
                                            (let [sm (<?- (k/get store schema-meta-key nil {:sync? sync?}))]
                                              (swap! schema-cache assoc schema-meta-key sm)
                                              sm)))
                            schema (or (:schema schema-meta) (:schema record))
                            record-refs (if schema
                                          (store-refs config schema
                                                      (:ident-ref-map schema-meta) aevt' taevt')
                                          #{})
                            cid (get-in record [:meta :datahike/commit-id])
                            new-reachable (cond-> (set/union reachable #{to-check}
                                                             ;; the commit's graph record lives and dies with it
                                                             (when cid #{(commit-graph-key cid)})
                                                             (when schema-meta-key #{schema-meta-key})
                                                             (mark (bind eavt-key eavt-root))
                                                             (mark aevt')
                                                             (mark (bind avet-key avet-root)))
                                            (:keep-history? config)
                                            (set/union (mark (bind temporal-eavt-key temporal-eavt-root))
                                                       (mark taevt')
                                                       (mark (bind temporal-avet-key temporal-avet-root)))
                                            sec-reachable
                                            (set/union sec-reachable))]
                        (recur (concat r (when in-range? parents))
                               new-reachable
                               (set/union refs record-refs))))
                    ;; Record absent: already swept by an earlier pass with a
                    ;; narrower window, or the store runs :commit-graph? false
                    ;; and never persisted it. Lineage ends here — nothing to
                    ;; mark. (Without this guard the nil destructure NPEs at
                    ;; get-time.) The seed is the branch's head, which the roster
                    ;; promises: absent, the read failed, and marking the key alone
                    ;; would sweep the head's whole tree (upstream 51cf69d3, #1117).
                    (if (= to-check branch)
                      (log/raise "Branch head unreadable during garbage collection mark; refusing to sweep."
                                 {:type :gc-branch-head-unreadable :branch branch})
                      (recur r reachable refs))))
                {:reachable reachable :store-refs refs}))))))

(defn- graph-keys
  "The store keys of the commit graph records that `seeds` (branch names and commit
  ids) reach: small, and what a merge base is walked over, so they outlive the
  snapshots a cutoff sweeps."
  [store seeds sync?]
  (async+sync sync? *default-sync-translation*
   (go-try-
    (loop [todo (vec seeds) seen #{} keys #{}]
      (if-let [x (peek todo)]
        (let [todo (pop todo)
              c (if (keyword? x) (<?- (k/get-in store [x :meta :datahike/commit-id] nil {:sync? sync?})) x)]
          (if (or (nil? c) (seen c))
            (recur todo seen keys)
            (let [k (commit-graph-key c)
                  record (<?- (k/get store k nil {:sync? sync?}))]
              (recur (into todo (:datahike/parents record)) (conj seen c) (cond-> keys record (conj k))))))
        keys)))))

(defn gc-storage!
  "Invokes garbage collection on the database by whitelisting currently known branches.
  All db snapshots on these branches before remove-before date will also be
  erased (defaults to beginning of time [no erasure]). The branch heads will
  always be retained.

  Opts `:datahike.gc/keep-commits` is a set of commit ids kept whole whatever their
  age. Every commit's graph record stays whatever the cutoff, so a merge base is
  found after any sweep.

  The sweep result is passed through unchanged. In `:datahike.gc/sweep-opts`,
  `:konserve.gc/report? true` requests the candidate and pre-sweep observed
  key sets; without it the result remains the candidate set.

  The sweep deletes an unreachable object only if it was written before this
  store's SAFE POINT (`datahike.gc-guard/safe-point`) — NOT before `now`.

  WHY: a commit writes every value the new head references and only THEN flips the
  branch head (`datahike.writing/commit!`; the barrier invariant). For the duration
  of that sequence the commit's objects are on disk and reachable from NOTHING —
  the head still names the previous snapshot — while ALREADY being older than
  `now`. A mark that runs inside the window calls them garbage and a `now` cutoff
  deletes them; the head then lands pointing at deleted objects. The safe point is
  the instant before which that verdict is final, and it is exact: it is `now` when
  nothing is in flight, and the START of the oldest in-flight sequence otherwise.
  `branch!`, `force-branch!` and `create-database` take the same guard, so this
  holds for them too, though they never touch the writer.

  Note the safe point is NOT `remove-before`. `remove-before` is a MARK-side policy
  (how far back the commit graph stays reachable — how much history you keep) and
  makes GC collect MORE. The safe point is a SWEEP-side bound (how recently written
  an object may be and still be judged) and makes it collect LESS.

  RUN IT WHERE THE WRITERS ARE. This follows from datahike's writer model, not from
  anything specific to GC: ALL WRITERS FOR A DATABASE RUN IN ONE JVM — they coordinate
  in memory, not through the store — and writer-side maintenance runs with them.
  `d/gc-storage` is a writer op, so it is already in the right place; the note is here
  because \"collect from a cron sidecar\" is a tempting shape and it is outside the
  model. Such a collector cannot observe the writer's in-flight sequences, and datahike
  cannot warn you: a second process gets a `:self` writer by default and looks like a
  writer too. (Cross-process writers are outside the model for a more basic reason as
  well — there is no head fencing yet, so they can lose each other's commits regardless
  of GC. See issue #878.) Readers are unconstrained."
  ([db] (gc-storage! db (#?(:clj Date. :cljs js/Date.) 0)))
  ([db remove-before]
   (gc-storage! db remove-before {}))
  ;; opts :sync? true runs the whole collection on the calling thread through the
  ;; store's synchronous operations and returns its result, not a channel: what the
  ;; writer's maintenance proc wants (on Jolt the async path costs a channel hand-off
  ;; per store read)
  ([db remove-before opts]
   (let [sync? (boolean (:sync? opts))]
   (async+sync sync? *default-sync-translation*
   (go-try-
           (let [{:keys [config store]} db
                 store-id (:id (:store config))
                 sweep-permit
                 (<?- (guard/acquire-sweep-permit!
                        store-id
                        {:sync? sync?
                         :datahike.gc-guard/maintenance-receipt
                         (:datahike.gc/maintenance-receipt opts)}))]
             (try
               (let [;; Cutoff from konserve's monotonic write clock, the SAME
                     ;; source that stamps :last-write (upstream #885): strictly
                     ;; increasing, so a wall-clock retreat cannot classify an
                     ;; in-flight commit's objects as old.
                     now (ku/now)
                     ;; Capture `now` before reading the safe point. Ordinary
                     ;; prior-head commits do not take the reachability gate and
                     ;; remain protected by this values-before-pointer cutoff.
                     cutoff (let [sp (guard/safe-point store-id)]
                              (if (< (get-time sp) (get-time now)) sp now))
                     _ (log/debug :datahike/gc-start {:time now :cutoff cutoff})
                     _ (when-not (= :self (:backend (:writer config)))
                         (log/warn :datahike/gc-without-local-writer
                                   {:writer (:backend (:writer config))
                                    :note "collecting a store this process does not write: in-flight commits elsewhere are invisible and may be swept"}))
                     _ (sc/clear-write-cache (:store config))
                     branches (require-branches (roster store))
                     _ (log/trace :datahike/gc-retain-branches {:branches branches})
                     visited (atom #{})
                     seen (atom #{})
                     ;; shared across branches: the schema is content-addressed
                     schema-cache (atom {})
                     keep (<?- (async+sync sync? *default-sync-translation*
                                 (go-try- (loop [[c & r] (seq (:datahike.gc/keep-commits opts)) acc []]
                                            (if c
                                              (recur r (if (<?- (k/exists? store c {:sync? sync?})) (conj acc c) acc))
                                              acc)))))
                     seeds (into (vec branches) keep)
                     marked (require-complete-mark
                             seeds
                             (if sync?
                               (mapv #(reachable-in-branch store % remove-before config visited seen schema-cache true) seeds)
                               (<?- (async/into [] (async/merge
                                                    (map #(reachable-in-branch store % remove-before config visited seen schema-cache false)
                                                         seeds))))))
                     ;; Store-refs (upstream #881) are unioned into the whitelist: an
                     ;; object in THIS store is spared until no datom names it; one
                     ;; elsewhere is a harmless no-op (`reachable-store-refs`).
                     native-reachable (-> (<?- (graph-keys store seeds sync?))
                                          (into (mapcat :reachable) marked)
                                          (into (mapcat :store-refs) marked))
                     extension
                     (if-let [extend-reachable (:datahike.gc/reachable-extension opts)]
                       (or (extend-reachable
                            {:datahike.gc/store store
                             :datahike.gc/store-id store-id
                             :datahike.gc/branches branches
                             :datahike.gc/config config
                             :datahike.gc/remove-before remove-before
                             :datahike.gc/reachable native-reachable})
                           #{})
                       #{})
                     reachable (-> native-reachable (into (roster-store-keys store)) (into extension) (conj collection-key))
                     batch-size (or (:datahike.gc/batch-size opts) 1000)
                     sweep-opts (cond-> (or (:datahike.gc/sweep-opts opts) {})
                                  sync? (assoc :konserve.gc/sync-io? true))]
                 (log/trace :datahike/gc-reachable {:reachable-count (count reachable)
                                                    :cutoff cutoff})
                 (let [swept (if sync?
                               (async/<!! (sweep! store reachable cutoff batch-size sweep-opts))
                               (<?- (sweep! store reachable cutoff batch-size sweep-opts)))]
                   ;; the next generation, after the sweep: a value key a node cache found
                   ;; before it, in any process, is asked of the store again (value-key). A dry
                   ;; run deletes nothing, so no key a cache holds went stale: it writes nothing
                   (when-not (:konserve.gc/dry-run? sweep-opts)
                     (<?- (k/update store collection-key (fnil inc 0) {:sync? sync?})))
                   swept))
               (finally
                 (guard/release-reachability-permit! sweep-permit)))))))))

(defn reachable-store-refs
  "The set of `:db.type/store-ref` values the database still names — its live blob
   ids — across ALL branches, honouring `remove-before` and `:keep-history?`.

   THE MARK, WITHOUT THE SWEEP. A store-ref names an object; it does not say where
   the bytes live, and that is deliberate:

     - IN THIS KONSERVE STORE — `gc-storage!` already spares and reclaims them.
       Portable across every backend, and `delete-database` erases them with the
       database. konserve frames a binary value as [header][meta][payload], so the
       bytes are not raw at that key: reads and writes go through `k/bget`/`k/bassoc`.
       Streamable (e.g. `:file`), so heap stays flat even for large files — but the
       bytes proxy through your JVM (client → server → store) and you forgo S3-native
       range, resumable multipart, and CDN. Fine at moderate size; S3-direct is the
       only thing that scales.

     - ANYWHERE ELSE — a raw S3 prefix the browser PUTs to with a presigned URL and
       a content-type, a CDN, another bucket. The bytes never transit your JVM,
       which is the whole point. Datahike cannot delete from there and does not
       pretend to: it hands you the live set, and you sweep.

   That is the entire contract for external blobs — datahike owns the hard half (what
   is still referenced, including from retained history and other branches); you own
   the easy half (list your prefix, delete what is not in this set):

     (let [live (<?? S (gc/reachable-store-refs @conn))]
       (doseq [obj (list-objects bucket (str \"tenant/\" tid \"/blobs/\"))]
         (when-not (live (blob-id obj))
           (delete-object bucket obj))))

   Retention comes for free: pass `remove-before` and objects named only by commits
   older than it drop out of the set, exactly as index nodes do."
  ([db] (reachable-store-refs db (#?(:clj Date. :cljs js/Date.) 0)))
  ([db remove-before]
   (go-try S
           (let [{:keys [config store]} db
                 ;; the branches are the roster's (ours, c0f9149), not a :branches key
                 branches (require-branches (roster store))
                 visited (atom #{})
                 seen (atom #{})
                 schema-cache (atom {})
                 walked (->> branches
                             (map #(reachable-in-branch store % remove-before config visited seen schema-cache false))
                             async/merge
                             (<<? S))]
             (apply set/union (map :store-refs walked))))))

(defn record-store-refs
  "The store-ref blob keys named by the datom VALUES in a SINGLE stored-db record —
   its AEVT, plus temporal AEVT when history is retained. This is the `store-refs`
   slice `gc-storage!` runs, exposed for one branch head.

   WHY IT IS PUBLIC. A reachability walker — the garbage collector here, and
   `konserve-sync`'s replication walker equally — discovers keys by walking the index
   TREES; it never looks inside a datom's value. So a blob named only by a
   `:db.type/store-ref` value is invisible to it: the collector would sweep it (fixed
   by `store-refs`), and the sync walker would never SHIP it — a subscriber ends up
   with a live datom pointing at an object that never replicated. The datahike sync
   walker unions this in per branch head to close that gap, exactly as the mark does.

   PER-RECORD, not per-history: it returns the refs in THIS head's index — precisely
   the set a walker ships for that head (under `:keep-history?` the temporal index
   already carries the retained ones). `reachable-store-refs` is the other shape — it
   walks the commit graph for GC retention. Don't confuse them: sync wants this one.

   Needs only the store and the record — no connection. The record carries its own
   `:config` (`:index`, `:attribute-refs?`), so nothing is inferred: under
   `:attribute-refs?` the datoms hold attribute EIDs and the slice keys off the
   `:ident-ref-map` accordingly. Schema comes from the record's `:schema-meta-key`
   (falling back to an inline `:schema`); the temporal index is sliced when the record
   retains one. `index-type` overrides the record's index if given."
  ([store stored-db] (record-store-refs store stored-db nil))
  ([store stored-db index-type]
   (go-try S
           (let [rec-config    (:config stored-db)
                 index-type    (or index-type (:index rec-config) dc/*default-index*)
                 schema-meta   (when-let [k (:schema-meta-key stored-db)]
                                 (<? S (k/get store k)))
                 schema        (or (:schema schema-meta) (:schema stored-db))
                 ident-ref-map (:ident-ref-map schema-meta)
                 config        {:index index-type
                                :attribute-refs? (:attribute-refs? rec-config)}
                 storage       (:storage store)
                 bind          (fn [idx root]
                                 (cond-> (with-storage index-type idx storage)
                                   root (-seed-root! root)))
                 aevt          (bind (:aevt-key stored-db) (:aevt-root stored-db))
                 taevt         (when (:temporal-aevt-key stored-db)
                                 (bind (:temporal-aevt-key stored-db)
                                       (:temporal-aevt-root stored-db)))]
             (if schema
               (store-refs config schema ident-ref-map aevt taevt)
               #{})))))

(defn start-background-gc!
  "Runs `gc-storage!` on `conn`'s database periodically in the background and
   returns a zero-arg stop function. EXPERIMENTAL.

   Unlike the freed-address online GC (`datahike.online-gc`), this is a full
   reachability collection: it works with MULTIPLE BRANCHES, is insensitive to
   how garbage was produced (it needs no mutation-time tracking), and also
   prunes commit-graph records when a history window is set. Its cost is a walk
   of all reachable nodes per cycle, so prefer the online GC's freed-tracking for
   single-branch bulk imports where write amplification dominates; the two
   compose (this collector reclaims what freed-tracking cannot see).

   It runs CONCURRENTLY with the writer, and is safe to do so because the sweep
   stops at the store's SAFE POINT rather than at `now`: an in-flight commit's
   objects are written after that point and are spared (see `gc-storage!`). Like all
   writer-side maintenance, it belongs in the process the writers run in — which is
   datahike's writer model, not a GC-specific rule (`gc-storage!`).

   Options:
     :interval-ms       — cycle period (default 300000 = 5 min).
     :history-window-ms — when set, commits older than (now - window) are
                          pruned from the commit graph each cycle (ranged GC);
                          default nil keeps all history.

   Errors in a cycle are logged and the loop continues; stop with the
   returned function, which answers a channel that delivers :stopped once the
   loop, and any cycle it was running, has ended."
  ([conn] (start-background-gc! conn {}))
  ([conn {:keys [interval-ms history-window-ms]
          :or {interval-ms 300000}}]
   (let [stop-ch (async/chan)
         done
     (go-loop []
       (let [[_ ch] (async/alts! [stop-ch (async/timeout interval-ms)])]
         (when-not (= ch stop-ch)
           ;; MERGE-REVIEW(deep-fixes): upstream #885 reads konserve's monotonic write
           ;; clock here (`ku/monotonic-now-ms`, konserve 0.9.362, replikativ/konserve#156).
           ;; Our konserve fork (24be381b) has neither it nor the hybrid logical clock:
           ;; its `ku/now` is the wall clock, so this reads that. Every #885 read in this
           ;; merge (gc cutoff, gc-guard/now, online-gc, persistent-set) is therefore
           ;; still wall-clock until the konserve fork takes #156.
           (let [remove-before (if history-window-ms
                                 (#?(:clj Date. :cljs js/Date.)
                                  (- (long (.getTime ^java.util.Date (ku/now)))
                                     (long history-window-ms)))
                                 (#?(:clj Date. :cljs js/Date.) 0))
                 res (async/<! (gc-storage! @conn remove-before))]
             (if (instance? #?(:clj Throwable :cljs js/Error) res)
               (log/warn :datahike/background-gc-error {:error res})
               (log/debug :datahike/background-gc-cycle {:swept (count res)})))
           (recur))))]
     ;; answers once the loop has ended, so a cycle in flight when stop is called has finished (a caller
     ;; deleting the store after it no longer races the cycle's sweep)
     (fn stop-background-gc! [] (async/close! stop-ch) (async/go (async/<! done) :stopped)))))
