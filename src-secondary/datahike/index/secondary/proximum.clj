(ns datahike.index.secondary.proximum
  "Proximum (vector similarity search) integration with Datahike secondary indices.

   Require this namespace to register the :proximum index type:
     (require 'datahike.index.secondary.proximum)

   Then declare in schema:
     {:idx/embeddings {:db.secondary/type :proximum
                       :db.secondary/attrs [:person/embedding]
                       :db.secondary/config {:dim 384 :distance :cosine
                                             :store-config {...}}}}"
  (:require
   [datahike.index.audit :as audit]
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [konserve.core :as k]
   [proximum.core :as prox]
   [proximum.crypto :as pcrypto]
   [proximum.protocols :as pproto]
   [proximum.writing :as pwr]
   [proximum.versioning :as pver]
   [proximum.hnsw.internal :as phi]
   [replikativ.logging :as log]
   [clojure.core.async :as async]
   [clojure.java.io :as io]))

(def ^:private float-array-class (Class/forName "[F"))

(defn- effective-mmap-dir
  "Return the explicit mmap directory or a durable store-local default."
  [config]
  (or (:mmap-dir config)
      (let [{:keys [backend path]} (:store-config config)]
        (when (and (= :file backend) path)
          ;; Keep mmap files beside, not inside, the konserve directory:
          ;; creating a child would pre-create the file-store path and make
          ;; Proximum's subsequent create-store fail with "already exists".
          (str path "-mmap")))))

(defn- ->float-array
  "Coerce an embedding value to a Java float[] for Proximum. Datahike validates
   a :db.type/tuple value as a Clojure vector, so the value reaching -transact
   is a (vector? ...) of numbers — but Proximum's insert/search want a float[].
   Accept either form; nil if it is neither (e.g. a non-vector value on the
   indexed attr, which we then skip)."
  [v]
  (cond
    (nil? v)                       nil
    (instance? float-array-class v) v
    (sequential? v)                (float-array v)
    :else                          nil))

(defn- await-owner!
  "Await a Proximum owner channel and surface asynchronous failures."
  [channel]
  (let [result (async/<!! channel)]
    (if (instance? Throwable result)
      (throw result)
      result)))

(defn- owner->key-map
  "Describe one already-durable Proximum owner without syncing it."
  [owner config]
  (let [commit-id (pproto/current-commit owner)]
    {:type :proximum
     :branch (name (pproto/current-branch owner))
     :commit-id commit-id
     :merkle-root commit-id
     :mmap-dir (effective-mmap-dir config)
     :store-config (:store-config config)}))

(defn- generation-addressed-owner?
  "True when an owner's immutable commit carries mmap generation evidence."
  [owner]
  (when-let [commit-id (pproto/current-commit owner)]
    (let [snapshot (k/get (pproto/raw-storage owner) commit-id nil {:sync? true})]
      (and (:mmap-generation snapshot)
           (:mmap-generation-bytes snapshot)
           (:mmap-generation-sha256 snapshot)))))

(defn- native-destination-branch
  "Resolve a destination attachment, including the legacy `db`/`main` lie."
  [store current-key-map]
  (let [declared (some-> (:branch current-key-map) keyword)
        declared-head (when declared (k/get store declared nil {:sync? true}))]
    (if declared-head
      declared
      (let [snapshot (k/get store (:commit-id current-key-map) nil {:sync? true})
            historical-branch (:branch snapshot)]
        (when-not (and snapshot (keyword? historical-branch)
                       (k/get store historical-branch nil {:sync? true}))
          (throw (ex-info "Proximum destination key map has no native attachment."
                          {:type :secondary-force/destination-attachment-mismatch
                           :current-key-map current-key-map})))
        historical-branch))))

(defn- load-key-map-owner
  "Load the exact key-map commit attached to its real native branch."
  [key-map mmap-dir]
  (let [store (pwr/connect-store-sync (:store-config key-map))
        branch (native-destination-branch store key-map)]
    (pwr/load-commit (:store-config key-map) (:commit-id key-map)
                     {:branch branch :mmap-dir mmap-dir})))

(defn- make-proximum-index
  "Create an ISecondaryIndex backed by Proximum.
   Entity IDs are used as external keys in the Proximum index.

   The live proximum index value is held in a MUTABLE atom `!idx` (not a
   plain closed-over `prox-idx`). This is load-bearing for durable edge
   persistence across incremental commits, and fixes a SAVE bug that dropped
   the HNSW edge graph on reopen:

     `proximum.protocols/sync!` returns a NEW immutable index value whose
     edge `:address-map` (position -> konserve chunk UUID) was just populated
     by the flush. datahike's commit path calls `-sec-flush` (which runs
     sync!) but then keeps the SAME reify in `:secondary-indices` — it has
     nowhere to install sync!'s return value (flush returns a key-map, not an
     index). If the bridge discards the synced value, the NEXT `-transact`
     forks from the PRE-sync index whose edge `:address-map` is still empty
     and whose dirty chunks were already cleared by the prior sync. The
     follow-on commit therefore persists an EMPTY `edges-addr-pss`, so every
     committed snapshot links zero edge chunks. Vectors survived only because
     proximum's vector chunk-address-map is an atom shared by reference across
     index values; the edge address-map is immutable state lost with the
     discarded value. Result on reopen: `load-commit` restores all vectors but
     an empty edge graph, leaving only the entrypoint reachable (KNN returns 1
     hit) until a full re-embed.

   Fix: `-sec-flush` resets `!idx` to the synced value, so the subsequent
   `-transact` on this same reify forks from the post-sync index (carrying the
   populated edge `:address-map`), and the address-map accumulates across
   commits exactly as a single bulk-insert+sync would. (Verified live: 20
   incremental insert+sync rounds that DISCARD the synced value persist 0 edge
   chunks and reload to 1 reachable node; RETAINING it persists the edge map
   and reloads to all nodes reachable.)"
  [prox-idx config]
  (let [attrs (set (:attrs config))
        ;; Mutable holder for the live index value. -sec-flush installs the
        ;; post-sync value here so the next -transact forks from it (see the
        ;; docstring). -transact/-restore/-branch still build fresh reifies for
        ;; datahike's immutable assoc-in; the atom only bridges the flush gap
        ;; WITHIN one reify's lifetime.
        !idx (atom prox-idx)]
    (reify
      java.io.Closeable
      (close [_]
        ;; Proximum close! is asynchronous.  A Datahike connection release is
        ;; synchronous from the caller's perspective, so do not return while
        ;; the mmap index can still hold file descriptors or locks.
        (await-owner! (pproto/close! @!idx)))

      sec/ISecondaryIndex
      (-search [_ query-spec entity-filter]
        ;; query-spec: {:vector float-array, :k int}
        ;; Returns EntityBitSet of matching entity IDs
        (let [{:keys [vector k]} query-spec
              qv (->float-array vector)
              cur @!idx
              results (if entity-filter
                        (prox/search-filtered cur qv k
                                              (set (es/entity-bitset-seq
                                                    entity-filter)))
                        (prox/search cur qv k))]
          (es/entity-bitset-from-longs (map :id results))))

      (-estimate [_ query-spec]
        ;; For KNN, the result count is exactly k (or less if fewer vectors exist)
        (min (:k query-spec 10) (prox/count-vectors @!idx)))

      (-can-order? [_ _attr direction]
        ;; Proximum results are naturally ordered by distance (ascending)
        (= direction :asc))

      (-slice-ordered [_ query-spec entity-filter _attr _direction limit]
        ;; KNN results are already distance-ordered; limit is just k
        (let [{:keys [vector k]} query-spec
              qv (->float-array vector)
              effective-k (if limit (min k limit) k)
              cur @!idx
              results (if entity-filter
                        (prox/search-filtered cur qv effective-k
                                              (set (es/entity-bitset-seq
                                                    entity-filter)))
                        (prox/search cur qv effective-k))]
          ;; Return as seq of {:entity-id :distance} for the caller to project
          (mapv (fn [{:keys [id distance]}]
                  {:entity-id id :distance distance})
                results)))

      (-indexed-attrs [_] attrs)

      sec/IVersionedSecondaryIndex
      (-sec-flush [_ _store _branch]
        ;; Proximum manages its own konserve store internally. The
        ;; protocol method `proximum.protocols/sync!` returns a chan
        ;; that yields a NEW immutable index value carrying the post-
        ;; sync commit-id; the live `prox-idx` field is the pre-sync
        ;; value and stays that way (the bridge has nowhere to put the
        ;; new one). So we wait, read commit-id off `synced`, and
        ;; surface it under both :commit-id (for restore) and
        ;; :merkle-root (for audit's key-map fallback path b in
        ;; writing.cljc) — IAuditable below can't see the post-sync
        ;; state on the live record.
        ;;
        ;; NOTE on the <!!: writing.cljc invokes -sec-flush from inside
        ;; commit!'s go-try- (the writer's commit loop), so this <!! blocks
        ;; the go-block's dispatch thread until proximum's async sync!
        ;; completes. Whether that is a deadlock hazard depends ENTIRELY on
        ;; the core.async dispatch model:
        ;;   - classic FIXED go-pool (8 threads): blocking here consumes a
        ;;     bounded pool thread; enough concurrent blocked commits could
        ;;     deadlock. (The original concern, written for that model.)
        ;;   - virtual-thread dispatch (core.async >= 1.7 on JDK >= 21): each
        ;;     go-block IS a virtual thread; blocking <!! unmounts it from its
        ;;     carrier — there is NO bounded pool to exhaust, so NO deadlock.
        ;; The wire-server runs core.async 1.9.829-alpha2 on Java 22, where
        ;; go-blocks ARE virtual threads (verified: -sec-flush executes in the
        ;; "VirtualThreads" group, Thread.isVirtual = true). VERIFIED LIVE
        ;; (P2-A.5, tmp/embed-restore/src/deadlock_smoke.clj): 100 concurrent
        ;; embedding writes at concurrency 32 all commit in ~1.3s with NO
        ;; deadlock, and a reopen+restore still passes. So this blocking <!!
        ;; is SAFE on the wire-server runtime. If core.async is ever
        ;; downgraded below the virtual-thread dispatch line (or pinned to the
        ;; fixed go-pool), this becomes a real hazard again and -sec-flush must
        ;; be made async up the chain (db->stored/commit! awaiting a channel) —
        ;; a datahike framework change, deliberately NOT done here to avoid
        ;; destabilizing the universal commit path against a non-problem.
        ;; INSTALL the synced value back into !idx (load-bearing — see the
        ;; make-proximum-index docstring): sync! returns a new index whose edge
        ;; :address-map was just populated. Without resetting !idx, the next
        ;; -transact on this same reify would fork from the pre-sync value with
        ;; an empty edge address-map (dirty chunks already cleared), and the
        ;; follow-on commit would persist an empty edges-addr-pss — losing the
        ;; whole HNSW edge graph on reopen. Resetting !idx makes the address-map
        ;; accumulate across commits.
        (let [current @!idx
              ;; A restored or previously flushed owner already names an exact
              ;; durable generation. Re-export it; syncing would create a new
              ;; root and mutate the source during a guarded Datahike force.
              synced (if (generation-addressed-owner? current)
                       current
                       (await-owner! (pproto/sync! current)))]
          (reset! !idx synced)
          (owner->key-map synced config)))

      (-sec-restore [_ _store key-map]
        ;; Restore from proximum's own store using commit ID
        (let [restored (load-key-map-owner
                        key-map (or (:mmap-dir key-map)
                                    (effective-mmap-dir config)))]
          (make-proximum-index restored config)))

      (-sec-branch [_ _store _from-branch new-branch]
        ;; Fork via proximum's native branching (reflink mmap + konserve COW)
        (let [branched (pver/branch! @!idx (keyword new-branch))]
          (make-proximum-index branched config)))

      (-sec-mark [_]
        ;; Proximum uses its own konserve store, not datahike's
        #{})

      audit/IAuditable
      (-merkle-root [_]
        ;; Proximum's commit-id is a content hash of the HNSW graph
        ;; + vectors state. Returns nil pre-commit; never throws.
        (phi/commit-id @!idx))
      (-recompute-merkle-root [this]
        ;; When proximum.audit (>= the audit-chain release) is on the
        ;; classpath, delegate the live-index walk to it — that gives
        ;; us actual chunk-level tamper detection (the older
        ;; verify-from-cold only checked existence). Older proximum
        ;; versions fall back to the local translation.
        (or (when-let [recompute (try (requiring-resolve 'proximum.audit/-recompute-merkle-root)
                                      (catch Throwable _ nil))]
              (recompute @!idx))
            (let [store-config (:store-config config)
                  branch (or (:branch config) :main)
                  result (pcrypto/verify-from-cold store-config branch)
                  root (audit/-merkle-root this)]
              (if (:valid? result)
                {:status :ok :root root}
                {:status :mismatch :root nil
                 :errors [{:type :audit/merkle-mismatch
                           :address root
                           :expected root
                           :details result}]}))))

      (-transact [_ tx-report]
        ;; tx-report: {:datom datom :added? bool}
        ;; Forks from @!idx — the post-sync value installed by the previous
        ;; -sec-flush (or the value this reify was created with), so the edge
        ;; :address-map carries forward across commits (see docstring).
        (let [{:keys [datom added?]} tx-report
              eid (.-e datom)
              val (.-v datom)
              cur @!idx]
          (if added?
            ;; Insert: coerce the datahike value (a Clojure vector after tuple
            ;; validation, or a raw float[]) to float[] for Proximum.
            (if-let [fa (->float-array val)]
              (make-proximum-index
               (prox/insert cur fa eid)
               config)
              (do (log/warn :datahike/non-vector-embedding {:eid eid :type (type val)})
                  (make-proximum-index cur config)))
            ;; Retract: delete by external entity ID
            (make-proximum-index
             (prox/delete cur eid)
             config)))))))

(defn- committed-index-exists?*
  "True iff `store-config` points at a konserve store that ALREADY holds a
   committed proximum index (i.e. has at least one branch). Used by the factory
   to choose connect-if-exists (restore skeleton) over create. We require a
   committed BRANCH, not merely a store dir, so a genuinely-empty/stale store
   (dir exists but no commit) takes the CREATE path rather than producing a
   passive skeleton that no `-sec-restore` will populate. Defensive: any backend
   error → treat as not-existing and fall through to create (which surfaces the
   real error)."
  [store-config]
  (boolean
   (when store-config
     (try
       (and (k/store-exists? store-config {:sync? true})
            (let [store (pwr/connect-store-sync store-config)]
              (seq (k/get store :branches nil {:sync? true}))))
       (catch Throwable _ false)))))

(defn- make-restore-skeleton
  "A passive placeholder index for datahike's `restore-secondary-indices`
   reopen path (writing.cljc:179). datahike creates a skeleton ONLY to dispatch
   `-sec-restore` on it (the real index materializes from the durable konserve
   store via `pwr/load-commit`), so the skeleton must NOT `create-store` or write
   `:index/config`/`:branches` (that overwrites the very store being restored —
   the P2-A.5 root cause). This reify implements `IVersionedSecondaryIndex` so
   the `(satisfies? ...)` check at writing.cljc:180 passes; only `-sec-restore`
   is ever invoked on it — every other method throws loudly if the reopen flow
   ever changes to call them on the skeleton (we want to know, not silently
   serve an empty index)."
  [config]
  (let [bug! (fn [m] (throw (ex-info (str "proximum restore-skeleton: " m
                                          " — only -sec-restore should be called on a skeleton")
                                     {:config config})))]
    (reify
      sec/IVersionedSecondaryIndex
      (-sec-flush [_ _store _branch] (bug! "-sec-flush"))
      (-sec-restore [_ _store key-map]
        ;; The real restore: load the committed HNSW from proximum's own
        ;; durable store. Identical to the live index's `-sec-restore`.
        (let [restored (load-key-map-owner
                        key-map (or (:mmap-dir key-map)
                                    (effective-mmap-dir config)))]
          (make-proximum-index restored config)))
      (-sec-branch [_ _store _from _new] (bug! "-sec-branch"))
      (-sec-mark [_] #{}))))

(sec/register-index-type!
 :proximum
 (fn [config _db]
   ;; CONNECT-IF-EXISTS. `create-index` (this factory) is called from two
   ;; datahike paths, BOTH with db = nil (so we cannot branch on db):
   ;;   1. fresh create / re-instantiate (transaction.cljc:218) — the store does
   ;;      NOT yet exist; we must `prox/create-index` to allocate + write config.
   ;;   2. reopen restore skeleton (writing.cljc:179) — the store DOES exist
   ;;      (durable file/jdbc, or the JVM-global :memory registry); we must NOT
   ;;      `create-index`, or `create-store-sync` throws "store already exists"
   ;;      and the real `-sec-restore`→`load-commit` never runs (the bug).
   ;; Branching on a committed store is correct for both: no committed index →
   ;; create; a committed index → passive skeleton, let `-sec-restore` populate.
   (let [mmap-dir (effective-mmap-dir config)
         config (cond-> config mmap-dir (assoc :mmap-dir mmap-dir))
         prox-config (merge {:type :hnsw}
                            ;; Keys must match proximum.hnsw/create-index's destructure
                            ;; (hnsw.clj:1159): it reads :M (uppercase), not :m — the old
                            ;; :m here was silently dropped, pinning M at the default 16.
                            (select-keys config [:dim :distance :store-config :mmap-dir
                                                 :capacity :M :ef-construction :ef-search]))]
     (if (committed-index-exists?* (:store-config config))
       (make-restore-skeleton config)
       (let [_ (when mmap-dir (.mkdirs (io/file mmap-dir)))
             prox-idx (prox/create-index prox-config)]
         (make-proximum-index prox-idx config))))))

;; GC: proximum uses its own store, not datahike's konserve
(defmethod sec/mark-from-key-map :proximum [_ _] #{})

(defmethod sec/check-force-from-key-map :proximum
  [source-key-map current-key-map _store branch]
  (when-not (= :proximum (:type current-key-map))
    (throw (ex-info "Existing Proximum destination state is required."
                    {:type :secondary-force/destination-not-found
                     :index-type :proximum
                     :branch branch})))
  (when-not (and (uuid? (:commit-id source-key-map))
                 (= (:commit-id source-key-map) (:merkle-root source-key-map))
                 (uuid? (:commit-id current-key-map))
                 (= (:commit-id current-key-map) (:merkle-root current-key-map))
                 (string? (:branch source-key-map))
                 (string? (:branch current-key-map)))
    (throw (ex-info "Proximum force requires committed source and destination key maps."
                    {:type :secondary-force/invalid-key-map
                     :branch branch
                     :source-key-map source-key-map
                     :current-key-map current-key-map})))
  (when-not (and (= (:store-config source-key-map)
                    (:store-config current-key-map))
                 (= (effective-mmap-dir source-key-map)
                    (effective-mmap-dir current-key-map)))
    (throw (ex-info "Proximum source and destination belong to different storage."
                    {:type :secondary-force/storage-mismatch
                     :branch branch
                     :source-store-config (:store-config source-key-map)
                     :current-store-config (:store-config current-key-map)
                     :source-mmap-dir (effective-mmap-dir source-key-map)
                     :current-mmap-dir (effective-mmap-dir current-key-map)})))
  (let [store (pwr/connect-store-sync (:store-config source-key-map))
        source-snapshot (k/get store (:commit-id source-key-map) nil {:sync? true})
        current-snapshot (k/get store (:commit-id current-key-map) nil {:sync? true})
        destination (native-destination-branch store current-key-map)
        destination-head (k/get store destination nil {:sync? true})
        destination-commit (:commit-id destination-head)]
    (when-not (and source-snapshot current-snapshot)
      (throw (ex-info "Proximum force key maps do not resolve to native commits."
                      {:type :secondary-force/destination-attachment-mismatch
                       :branch branch
                       :source-commit (:commit-id source-key-map)
                       :current-commit (:commit-id current-key-map)})))
    (when-not (or (= (:commit-id current-key-map) destination-commit)
                  (= (:commit-id source-key-map) destination-commit))
      (throw (ex-info "Proximum destination branch moved before guarded force."
                      {:type :force-branch/stale-destination
                       :branch branch
                       :native-branch destination
                       :expected-current-commit (:commit-id current-key-map)
                       :source-commit (:commit-id source-key-map)
                       :current-commit destination-commit}))))
  nil)

(defmethod sec/force-from-key-map :proximum
  [source-key-map current-key-map _store _branch]
  (let [source-owner
        (load-key-map-owner source-key-map
                            (effective-mmap-dir source-key-map))
        destination (native-destination-branch (pproto/raw-storage source-owner)
                                               current-key-map)
        forced-owner (volatile! nil)]
    (try
      (let [forced (pver/force-branch!
                    source-owner
                    destination
                    (:commit-id current-key-map))]
        (vreset! forced-owner forced)
        (assoc (owner->key-map forced source-key-map)
               :store-config (:store-config source-key-map)))
      (finally
        (when-let [owner @forced-owner]
          (await-owner! (pproto/close! owner)))
        (await-owner! (pproto/close! source-owner))))))

;; Branch: load from stored commit, branch via proximum native
(defmethod sec/branch-from-key-map :proximum [key-map _store _from-branch new-branch]
  (let [idx (load-key-map-owner key-map (effective-mmap-dir key-map))
        fork-owner (volatile! nil)]
    (try
      (let [branched (pver/branch! idx (keyword new-branch))
            _ (vreset! fork-owner branched)
            synced (await-owner! (pproto/sync! branched))
            _ (vreset! fork-owner synced)
            new-commit-id (phi/commit-id synced)]
        (assoc key-map
               :branch (name new-branch)
               :commit-id new-commit-id
               :merkle-root new-commit-id))
      (finally
        ;; sync! returns the new resource owner. Close exactly that value (or
        ;; the pre-sync fork when sync failed), plus the independent source.
        (when-let [owner @fork-owner]
          (await-owner! (pproto/close! owner)))
        (await-owner! (pproto/close! idx))))))
