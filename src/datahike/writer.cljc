(ns ^:no-doc datahike.writer
  (:require [superv.async :refer [S <?- go-try]]
            [replikativ.logging :as log]
            [datahike.core]
            [datahike.writing :as w]
            [datahike.query :as q]
            [datahike.committed-report :as committed-report]
            [datahike.gc :as gc]
            [datahike.tools :as dt :refer [throwable-promise get-time-ms]]
            [clojure.core.async :refer [chan close! promise-chan put! go go-loop <! >! poll! buffer timeout #?(:clj thread)]]
            #?(:cljs [cljs.core.async.impl.channels :refer [ManyToManyChannel]]))
  #?(:clj (:import [clojure.core.async.impl.channels ManyToManyChannel]
                   [java.lang.management ManagementFactory])))

(defn chan? [x]
  (instance? ManyToManyChannel x))

(defprotocol PWriter
  (-dispatch! [_ arg-map] "Returns a channel that resolves when the transaction finalizes.")
  (-shutdown [_] "Returns a channel that resolves when the writer has shut down.")
  (-streaming? [_] "Returns whether the transactor is streaming updates directly into the connection, so it does not need to fetch from store on read."))

(defn- writer-shut-down-error []
  (ex-info "Writer is shut down; release and reconnect."
           {:type :writer-shut-down}))

(defn- fail-pending-transactions!
  "Resolve every accepted invocation still buffered in the transaction queue."
  [transaction-queue error]
  (loop []
    (when-let [{:keys [callback applied]} (poll! transaction-queue)]
      (put! callback error)
      (some-> applied (put! error))
      (recur))))

(defn- fail-pending-commits!
  "Resolve every transaction report already staged for a commit that cannot run."
  [commit-queue error]
  (loop []
    (when-let [[_ callback] (poll! commit-queue)]
      (put! callback error)
      (recur))))

(defrecord LocalWriter [processing-thread commit-thread streaming?
                        transaction-queue-size commit-queue-size
                        transaction-queue commit-queue inflight-ops]
  PWriter
  (-dispatch! [_ arg-map]
    (let [p (promise-chan)]
      ;; Admission and shutdown can race after put! returns. Its completion
      ;; callback is the authoritative result: false means the queue closed
      ;; without accepting this invocation, so always resolve the caller.
      (put! transaction-queue (assoc arg-map :callback p)
            (fn [accepted?]
              (when-not accepted?
                (put! p (writer-shut-down-error)))))
      p))
  (-shutdown [_]
    (close! transaction-queue)
    (go-try S
            (let [;; Retain and join both loops explicitly.  The processing
                  ;; loop's finally closes commit-queue even when it fails. Do
                  ;; not let that first failure skip joining the commit loop.
                  processing-error
                  (try (<?- processing-thread) nil
                       (catch #?(:clj Throwable :cljs js/Error) e e))
                  commit-error
                  (try (<?- commit-thread) nil
                       (catch #?(:clj Throwable :cljs js/Error) e e))]
              ;; Channel-returning writer ops run outside the commit loop. Once
              ;; both loops have stopped, the set is closed to new members;
              ;; join every accepted op before surfacing a loop failure.
              (doseq [done @inflight-ops]
                (<?- done))
              (when-let [error (or processing-error commit-error)]
                (throw error))
              true)))
  (-streaming? [_] streaming?))

(def ^:const DEFAULT_QUEUE_SIZE 120000)

;; minimum wait time between commits in ms
;; this reduces write pressure on the storage
;; at the cost of higher latency
(def ^:const DEFAULT_COMMIT_WAIT_TIME 0) ;; in ms

(defn- write-error-log
  "Log exception structure and identities, never invocation arguments,
  exception data or the stack trace. The caller receives the error itself as
  the transaction's result; the log names each cause link and where it was
  thrown (`:via`'s `:at`), and a whole trace here was an unbounded copy
  printed to whatever `*out*` the writer thread inherited."
  [database invocation error]
  (let [identity-keys [:seon.cluster/name :seon.test/sym :seon.test.run/id
                       :seon.error/id]
        identities (into {}
                         (comp (filter map?)
                               (mapcat #(select-keys % identity-keys)))
                         [(get-in (ex-data error) [:seon.error/data :seon.error/diagnostic-evidence])
                          (ex-data error)
                          (get-in invocation [:args 0 :tx-meta])
                          invocation])]
    (merge identities
           {:op (:op invocation)
            :branch (get-in database [:config :branch])
            :datahike/commit-id (get-in database [:meta :datahike/commit-id])
            :error #?(:clj (-> (Throwable->map error)
                               (dissoc :data :trace)
                               (update :via #(mapv (fn [cause] (dissoc cause :data)) %)))
                      :cljs {:type (.-name error)
                             :cause (.-message error)})})))

#?(:clj
   (defn- thread-counters
     "This thread's allocated bytes (-1 where the JVM keeps no count) and CPU
     nanoseconds."
     []
     (let [threads (ManagementFactory/getThreadMXBean)]
       [(if #?(:jolt false ;; jolt: no per-thread allocation counter
               :clj (instance? com.sun.management.ThreadMXBean threads))
          (.getCurrentThreadAllocatedBytes ^com.sun.management.ThreadMXBean threads)
          -1)
        (.getCurrentThreadCpuTime threads)])))

#?(:clj
   (defn- measured-commit!
     "Commit `db` synchronously on the calling platform thread. Returns
     `[committed-db cost]`: every storage write the commit makes runs on this
     thread, so its counters are the commit's exact allocation and CPU."
     [db merge-parents last-cid revision-change]
     (let [[allocated cpu] (thread-counters)
           committed (w/commit! db merge-parents true last-cid revision-change)
           [allocated' cpu'] (thread-counters)]
       [committed (cond-> {:cpu-ns (- cpu' cpu)}
                    (<= 0 allocated allocated') (assoc :alloc-bytes (- allocated' allocated)))])))

;; jolt: core.async buffers do not support count; pressure warnings are skipped.
(defn- buf-count [b] #?(:jolt 0 :default (count b)))

(defn create-thread
  "Creates new transaction thread"
  [connection write-fn-map transaction-queue-size commit-queue-size commit-wait-time]
  (let [transaction-queue-buffer    (buffer transaction-queue-size)
        transaction-queue           (chan transaction-queue-buffer)
        commit-queue-buffer         (buffer commit-queue-size)
        commit-queue                (chan commit-queue-buffer)
        inflight-ops                (atom #{})
        ;; processing loop
        processing-thread
        (go-try S
                (try
                  ;; delay processing until the writer we are part of in connection is set
                  (while (not (:writer @(:wrapped-atom connection)))
                    (<! (timeout 0))) ; proto: was 10 ms, which every first transact after a connect waited out
                  (loop [old @(:wrapped-atom connection)]
                    (if-let [{:keys [op args callback applied] :as invocation} (<?- transaction-queue)]
                    (do
                      (when (> (buf-count transaction-queue-buffer) (* 0.9 transaction-queue-size))
                        (log/warn :datahike/tx-queue-pressure "Transaction queue buffer >90% full" {:count (buf-count transaction-queue-buffer) :size transaction-queue-size}))
                      (let [;; TODO remove this after import is ported to writer API.
                            ;; An external writer may advance the committed basis,
                            ;; but the connection remains behind while this writer's
                            ;; own reports await batch commit. Never rewind the
                            ;; threaded uncommitted basis in that interval.
                            committed-max-tx (:max-tx @(:wrapped-atom connection))
                            old (if (> committed-max-tx (:max-tx old))
                                  (assoc old :max-tx committed-max-tx)
                                  old)

                            op-fn (write-fn-map op)
                            res   (try
                                    (apply op-fn old args)
                            ;; Catch all Throwables to handle AssertionError and other Errors
                            ;; These should crash the writer, but we deliver to callback first to prevent hangs
                                    (catch #?(:clj Throwable :cljs js/Error) e
                              ;; take a guess that a NPE was triggered by an invalid connection
                              ;; short circuit on errors
                                      #?(:cljs (put! callback e)
                                         :clj
                                         (let [failure (if (= (type e) NullPointerException)
                                                         (ex-info "Null pointer encountered in invocation. Connection may have been invalidated, e.g. through db deletion, and needs to be released everywhere."
                                                                  {:type   :writer-error-during-invocation
                                                                   :op     op
                                                                   :branch (get-in old [:config :branch])
                                                                   :datahike/commit-id (get-in old [:meta :datahike/commit-id])}
                                                                  e)
                                                         e)]
                                           (put! callback failure)
                                           (some-> applied (put! failure))))
                                      ;; The accepted caller must receive its failure even
                                      ;; when formatting or delivering the diagnostic throws.
                                      (log/error :datahike/write-error
                                                 (write-error-log old invocation e))
                              ;; Re-throw Errors (AssertionError, OutOfMemoryError, etc.) to crash the writer
                              ;; Only Exceptions should be handled and allow the writer to continue.
                              ;; CLOSE the queues first: a dead loop with open queues would accept
                              ;; further transacts whose callbacks can never be delivered — every
                              ;; subsequent transact would hang silently instead of failing loudly.
                                      #?(:clj (when (instance? Error e)
                                                (close! transaction-queue)
                                                (close! commit-queue)
                                                (throw e)))
                                      :error))]
                        (cond (chan? res)
                              ;; async op, run in parallel in background, no sequential commit handling needed
                              (let [done (promise-chan)]
                                (swap! inflight-ops conj done)
                                (go
                                  (try
                                    (if-some [result (<! res)]
                                      (>! callback result)
                                      (>! callback
                                          (ex-info "Asynchronous writer operation closed without a result."
                                                   {:type :async-writer-op-closed
                                                    :op op})))
                                    (finally
                                      (swap! inflight-ops disj done)
                                      (put! done true))))
                                (recur old))

                              (not= res :error)
                              (do
                                (when (> (buf-count commit-queue-buffer) (/ commit-queue-size 2))
                                  (log/warn :datahike/commit-queue-pressure "Commit queue buffer >50% full" {:count (buf-count commit-queue-buffer) :size commit-queue-size})
                                  (<! (timeout 50)))
                                ;; A transaction is not admitted to the commit
                                ;; stage until core.async confirms this put.
                                ;; Closing a full queue wakes a parked put with
                                ;; false; ignoring that result strands the
                                ;; callback forever.
                                ;; An `:applied` port answers now, in writer
                                ;; order, with the report on the uncommitted
                                ;; value; the callback still answers after the
                                ;; batch commits it.
                                (some-> applied (put! res))
                                (if (>! commit-queue [res callback])
                                  ;; Merge parents belong to the report that
                                  ;; set them, never to the next operation.
                                  (recur (update (:db-after res) :meta dissoc :datahike/merge-parents))
                                  (let [error (writer-shut-down-error)]
                                    (put! callback error)
                                    (close! transaction-queue)
                                    (fail-pending-transactions! transaction-queue error))))
                              :else
                              (recur old))))
                      (log/debug :datahike/writer-closed "Writer thread gracefully closed")))
                  (catch #?(:clj Throwable :cljs js/Error) e
                    ;; The current invocation was resolved at the point where
                    ;; it failed. Closing does not discard a core.async
                    ;; buffer, so explicitly fail every other invocation that
                    ;; dispatch accepted before this fatal exit.
                    (close! transaction-queue)
                    (close! commit-queue)
                    (fail-pending-transactions! transaction-queue e)
                    (throw e))
                  (finally
                    (close! commit-queue))))
        ;; commit loop
        commit-thread
        (go-try S
                (loop [tx (<?- commit-queue)
                       ;; last committed cid of OUR branch: nil on the first
                       ;; iteration (commit! falls back to the storage read),
                       ;; threaded through afterwards so ordinary commits skip
                       ;; the per-commit branch-head read (S3: 3 requests)
                       last-cid nil]
                  (when tx
                    (let [txs (into [tx] (take-while some?) (repeatedly #(poll! commit-queue)))]
              ;; empty channel of pending transactions
                      (log/trace :datahike/batch-commit {:batch-size (count txs)})
              ;; commit latest tx to disk
                      (let [parent-db @connection
                            db (:db-after (first (peek txs)))
                            ;; Merge parents of every report in the batch
                            ;; (set by merge-writer!); the batch commits once.
                            merge-parents (not-empty (into #{} (mapcat #(get-in (first %) [:db-after :meta :datahike/merge-parents])) txs))
                            ;; Clear merge-parents from db meta before persisting
                            db (if merge-parents
                                 (update db :meta dissoc :datahike/merge-parents)
                                 db)]
                        (try
                          (let [start-ts (get-time-ms)
                                modified-attrs
                                (w/batch-cache-revision-attributes
                                 (mapv first txs))
                                ;; The commit stores its revisions, so a later
                                ;; load of it knows them without this connection.
                                revision-change {:prior (:cache-context parent-db)
                                                 :modified-attrs modified-attrs
                                                 :unsafe? (boolean merge-parents)}
                                ;; One platform thread runs the whole commit,
                                ;; so its cost is exact (on the go dispatcher
                                ;; konserve's writes would scatter over the pool).
                                [{{:keys [datahike/commit-id]} :meta
                                  :as stored-commit-db}
                                 commit-cost]
                                #?(:clj (<?- (thread (try (measured-commit! db merge-parents last-cid revision-change)
                                                          (catch Throwable e e))))
                                   :cljs [(<?- (w/commit! db merge-parents false last-cid revision-change)) nil])
                                commit-db
                                (assoc stored-commit-db :cache-context
                                       (some-> (:cache-context parent-db)
                                               (q/advance-query-cache-context
                                                commit-id modified-attrs
                                                (boolean merge-parents))))
                                commit-time (- (get-time-ms) start-ts)]
                            (log/trace :datahike/commit-time {:duration-ms commit-time})
                            #?(:jolt (reset! (.-wrapped-atom connection) commit-db) :clj (reset! connection commit-db)) ;; jolt: reset! takes only jolt atoms
                    ;; notify all processes that transaction is complete
                            ;; Every report of the batch carries the one
                            ;; commit that made it durable: the batch's
                            ;; transactions share its cost.
                            (doseq [[tx-report callback] txs]
                              (let [tx-report (-> tx-report
                                                  (assoc-in [:tx-meta :db/commitId] commit-id)
                                                  (assoc :db-after commit-db)
                                                  (assoc :datahike/commit
                                                         (merge commit-cost
                                                                {:commit-id commit-id
                                                                 :tx-count (count txs)
                                                                 :duration-ms commit-time})))]
                                (committed-report/offer-committed! commit-db tx-report)
                                (>! callback tx-report))))
                          (catch #?(:clj Throwable :cljs js/Error) e
                            ;; Close the queues BEFORE delivering the failed
                            ;; callbacks. Delivering first unblocks the caller
                            ;; while the queues are still open, so a subsequent
                            ;; transact could race into the still-open queue and
                            ;; commit AFTER the fatal error (writer_error_test
                            ;; saw the "dead" writer accept a further write).
                            ;; Closing first makes that transact observe the
                            ;; closed queue and fail loudly (:writer-shut-down).
                            (close! commit-queue)
                            (close! transaction-queue)
                            (doseq [[_ callback] txs]
                              (put! callback e))
                            (fail-pending-commits! commit-queue e)
                            (log/error :datahike/writer-shutdown {:error e})
                            ;; A failed commit is terminal even when the cause
                            ;; is an Exception. Continuing would apply staged
                            ;; reports after a durability failure and would
                            ;; leave the writer's in-memory chain ambiguous.
                            (throw e)))
                        (<! (timeout commit-wait-time))
                        (recur (<?- commit-queue)
                               (get-in @connection [:meta :datahike/commit-id])))))))]
    [transaction-queue commit-queue inflight-ops processing-thread commit-thread]))

;; public API to internal mapping
(def default-write-fn-map {'transact!     w/transact!
                           'load-entities w/load-entities
                           ;; async operations that run in background
                           'gc-storage!   gc/gc-storage!
                           ;; secondary index backfill (async, runs in background)
                           #?@(:clj ['build-secondary-index! w/build-secondary-index!
                                     'install-secondary-index! w/install-secondary-index!])
                           ;; merge with multi-parent commit tracking
                           'merge! w/merge-writer!})

(defmulti create-writer
  (fn [writer-config _]
    (:backend writer-config)))

(defmethod create-writer :self
  [{:keys [transaction-queue-size commit-queue-size write-fn-map commit-wait-time]} connection]
  (let [transaction-queue-size (or transaction-queue-size DEFAULT_QUEUE_SIZE)
        commit-queue-size (or commit-queue-size DEFAULT_QUEUE_SIZE)
        commit-wait-time (or commit-wait-time DEFAULT_COMMIT_WAIT_TIME)
        [transaction-queue commit-queue inflight-ops processing-thread commit-thread]
        (create-thread connection
                       (merge default-write-fn-map
                              write-fn-map)
                       transaction-queue-size
                       commit-queue-size
                       commit-wait-time)]
    (map->LocalWriter
     {:transaction-queue transaction-queue
      :transaction-queue-size transaction-queue-size
      :commit-queue commit-queue
      :commit-queue-size commit-queue-size
      :processing-thread processing-thread
      :commit-thread commit-thread
      :inflight-ops inflight-ops
      :streaming? true})))

;; proto (lane F1): the caller is the serializer. A :sync writer applies the operation
;; and commits it on the calling thread, so a single serial caller (proto's Flow
;; :writer proc) is THE writer: no transaction queue, no commit loop, no thread or go
;; block per transaction. The commit is the same one the :self commit loop makes for
;; a batch of one (w/commit! with the revision change and the advanced query cache
;; context). Callers must be serial: two concurrent callers would both commit on the
;; same parent. Channel-returning ops (gc, index backfill) are not supported here.
(defn- sync-apply! [connection write-fn-map last-cid {:keys [op args]}]
  (let [old @(:wrapped-atom connection)
        res (apply (write-fn-map op) old args)
        parent-db old
        db (:db-after res)
        merge-parents (get-in db [:meta :datahike/merge-parents])
        db (if merge-parents (update db :meta dissoc :datahike/merge-parents) db)
        start-ts (get-time-ms)
        modified-attrs (w/batch-cache-revision-attributes [res])
        stored (w/commit! db (not-empty merge-parents) true @last-cid
                          {:prior (:cache-context parent-db) :modified-attrs modified-attrs
                           :unsafe? (boolean (seq merge-parents))})
        commit-id (get-in stored [:meta :datahike/commit-id])
        commit-db (assoc stored :cache-context
                         (some-> (:cache-context parent-db)
                                 (q/advance-query-cache-context commit-id modified-attrs (boolean (seq merge-parents)))))
        report (-> res
                   (assoc-in [:tx-meta :db/commitId] commit-id)
                   (assoc :db-after commit-db)
                   (assoc :datahike/commit {:commit-id commit-id :tx-count 1 :duration-ms (- (get-time-ms) start-ts)}))]
    (vreset! last-cid commit-id)
    #?(:jolt (reset! (.-wrapped-atom connection) commit-db) :clj (reset! connection commit-db))
    (committed-report/offer-committed! commit-db report)
    report))

(defrecord SyncWriter [connection write-fn-map last-cid pending]
  PWriter
  (-dispatch! [_ arg-map]
    (let [p (promise-chan)]
      (put! p (try (sync-apply! connection write-fn-map last-cid arg-map)
                   (catch #?(:clj Throwable :cljs js/Error) e e)))
      p))
  (-shutdown [_] (go true))
  (-streaming? [_] false))

(defn transact-batch!
  "proto (lane F2): group commit at the serial caller. Applies each arg-map with
  `w/transact!` in order, threading the db value (a failing transaction answers its
  exception and leaves the value as it was), then commits ONCE: one `w/commit!` for the
  whole batch, as the :self commit loop does for the reports queued behind a commit.
  Answers a vector, one report or exception per arg-map; every report carries the one
  commit (`:datahike/commit` `:tx-count`). Only on a :sync writer: its caller is serial."
  [connection arg-maps]
  (let [writer (:writer @(:wrapped-atom connection))
        _ (assert (instance? SyncWriter writer) "transact-batch! needs a :sync writer")
        tx! (get (:write-fn-map writer) 'transact!)
        parent-db @(:wrapped-atom connection)
        [db rs] (reduce (fn [[db rs] am]
                          (let [r (try (tx! db am) (catch #?(:clj Throwable :cljs js/Error) e e))]
                            (if (map? r) [(:db-after r) (conj rs r)] [db (conj rs r)])))
                        [parent-db []] arg-maps)
        ok (filterv map? rs)]
    (if (empty? ok)
      rs
      (let [start-ts (get-time-ms)
            modified-attrs (w/batch-cache-revision-attributes ok)
            stored (w/commit! db nil true @(:last-cid writer)
                              {:prior (:cache-context parent-db) :modified-attrs modified-attrs :unsafe? false})
            commit-id (get-in stored [:meta :datahike/commit-id])
            commit-db (assoc stored :cache-context
                             (some-> (:cache-context parent-db)
                                     (q/advance-query-cache-context commit-id modified-attrs false)))
            commit {:commit-id commit-id :tx-count (count ok) :duration-ms (- (get-time-ms) start-ts)}]
        (vreset! (:last-cid writer) commit-id)
        #?(:jolt (reset! (.-wrapped-atom connection) commit-db) :clj (reset! connection commit-db))
        (mapv (fn [r]
                (if (map? r)
                  (let [r (-> r (assoc-in [:tx-meta :db/commitId] commit-id) (assoc :datahike/commit commit))]
                    (committed-report/offer-committed! commit-db r)
                    r)
                  r))
              rs)))))

(defmethod create-writer :sync
  [{:keys [write-fn-map]} connection]
  (->SyncWriter connection (merge default-write-fn-map write-fn-map) (volatile! nil) (volatile! nil)))

;; seon (lane A1): run-ahead writes at the serial caller. The caller owns its branch alone (an
;; agent process): each transaction is applied to the value ahead of the committed one and
;; answered at once; the applied value is committed once, later (when the caller is idle, at
;; a turn's end, or at the flush window). Entity ids come from the store's allocator as in
;; any transact, so the ids a caller saw when it applied are the ids committed.

(defn apply!
  "Apply arg-map to the connection's run-ahead value (its pending value, else its committed
  one) with `w/transact!`, without committing. Answers the report (its :db-after is the new
  run-ahead value), or the exception, leaving the value as it was. Only on a :sync writer."
  [connection arg-map]
  (let [writer (:writer @(:wrapped-atom connection))
        _ (assert (instance? SyncWriter writer) "apply! needs a :sync writer")
        tx! (get (:write-fn-map writer) 'transact!)
        {:keys [db reports]} (or @(:pending writer) {:db @(:wrapped-atom connection) :reports []})
        r (try (tx! db arg-map) (catch #?(:clj Throwable :cljs js/Error) e e))]
    (when (map? r) (vreset! (:pending writer) {:db (:db-after r) :reports (conj reports r)}))
    r))

(defn pending?
  "Has the connection applied transactions not committed yet?"
  [connection]
  (some? @(:pending (:writer @(:wrapped-atom connection)))))

(defn commit-pending!
  "Commit the connection's run-ahead value once (as transact-batch! commits a batch) and make
  it the committed value. Answers {:commit-id :tx-count :db}, or nil when nothing is pending."
  [connection]
  (let [writer (:writer @(:wrapped-atom connection))]
    (when-let [{:keys [db reports]} @(:pending writer)]
      (let [parent-db @(:wrapped-atom connection)
            modified-attrs (w/batch-cache-revision-attributes reports)
            stored (w/commit! db nil true @(:last-cid writer)
                              {:prior (:cache-context parent-db) :modified-attrs modified-attrs :unsafe? false})
            commit-id (get-in stored [:meta :datahike/commit-id])
            commit-db (assoc stored :cache-context
                             (some-> (:cache-context parent-db)
                                     (q/advance-query-cache-context commit-id modified-attrs false)))]
        (vreset! (:last-cid writer) commit-id)
        (vreset! (:pending writer) nil)
        #?(:jolt (reset! (.-wrapped-atom connection) commit-db) :clj (reset! connection commit-db))
        {:commit-id commit-id :tx-count (count reports) :db commit-db}))))

;; Note: :kabel backend is implemented in datahike.kabel.writer
;; Require that namespace to register the defmethod

(defn dispatch! [writer arg-map]
  (-dispatch! writer arg-map))

(defn shutdown [writer]
  (-shutdown writer))

(defn streaming? [writer]
  (-streaming? writer))

(defn backend-dispatch [& args]
  (get-in (first args) [:writer :backend] :self))

(defmulti create-database backend-dispatch)

(defmethod create-database :self [& args]
  (let [p (throwable-promise)]
    (go
      (#?(:jolt (fn [p v] (p v)) :clj deliver :cljs put!) p (<! (apply w/create-database args))))
    p))

(defmulti delete-database backend-dispatch)

(defmethod delete-database :self [& args]
  (let [p (throwable-promise)]
    (go
      (let [res (<! (apply w/delete-database args))]
        #?(:jolt (p res) :clj (deliver p res) :cljs (if (nil? res) (close! p) (put! p res)))))
    p))

(defn- detect-new-building-indices
  "Detect secondary indices that *transitioned* into :building in this tx,
   i.e. they are :building in db-after but were not already :building in
   db-before. Returns a seq of idx-idents that need a one-time backfill.

   Comparing against db-before is essential: any transaction applied while
   an index is still building would otherwise re-dispatch a full backfill,
   and a second backfill that runs after the first one's
   install-secondary-index! has dissoc'd :db.secondary/building-since-tx
   loses the snapshot guard and re-delivers post-creation datoms that were
   already applied live — double-counting them in the index."
  [tx-report]
  (let [before (get-in tx-report [:db-before :schema])
        after  (get-in tx-report [:db-after :schema])]
    (when after
      (keep (fn [[ident entry]]
              (when (and (map? entry)
                         (:db.secondary/type entry)
                         (= :building (:db.secondary/status entry))
                         (not= :building (get-in before [ident :db.secondary/status])))
                ident))
            after))))

(defn- retire-listener!
  "Remove `listener-key` only while it still names the captured `callback`.
   True when this call removed it; a replacement registered under the same
   key survives."
  [listeners listener-key callback]
  (let [[before _] (swap-vals! listeners
                               (fn [registered]
                                 (if (identical? (get registered listener-key) callback)
                                   (dissoc registered listener-key)
                                   registered)))]
    (identical? (get before listener-key) callback)))

(defn- notify-listeners!
  "Notify every listener independently after a committed result is settled.
   A failing callback with a `:listener-failure` handler in the connection's
   meta (installed with `alter-meta!` on its `:wrapped-atom`) is retired by identity
   first, then the handler receives the failure on this thread. Without a
   handler the failure is logged and the listener stays registered."
  [connection listeners tx-report]
  (doseq [[listener-key callback] listeners]
    (try
      (callback tx-report)
      (catch #?(:clj Throwable :cljs :default) exception
        (if-let [handler (:listener-failure (meta connection))]
          (let [retired? (retire-listener! (:listeners (meta connection))
                                           listener-key callback)]
            (handler {:listener-key listener-key
                      :callback callback
                      :exception exception
                      :tx-report tx-report
                      :retired? retired?}))
          (log/error :datahike/listener-error
                     {:listener-key listener-key
                      :exception exception}))))))

(defn transact!
  "Queue `arg-map` on the connection's writer; the returned promise delivers
   the committed report. With `applied`, a port, that port first receives the
   report the writer applied on its uncommitted value (or the invocation's
   failure), in writer order, before the batch commits."
  ([connection arg-map] (transact! connection arg-map nil))
  ([connection arg-map applied]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (if (instance? SyncWriter writer)
      (let [tx-report (try (sync-apply! connection (:write-fn-map writer) (:last-cid writer) {:op 'transact! :args [arg-map]})
                           (catch #?(:clj Throwable :cljs js/Error) e e))]
        (#?(:jolt (fn [p v] (p v)) :clj deliver :cljs put!) p tx-report)
        (when (map? tx-report) (notify-listeners! connection (some-> (:listeners (meta connection)) deref) tx-report))
        p)
    (do (go
      (let [tx-report (<! (dispatch! writer
                                     (cond-> {:op 'transact!
                                              :args [arg-map]}
                                       applied (assoc :applied applied))))
            listeners (some-> (:listeners (meta connection)) deref)]
        (#?(:jolt (fn [p v] (p v)) :clj deliver :cljs put!) p tx-report)
        (when (map? tx-report) ;; not error
          ;; Dispatch backfill for any newly created secondary indices
          #?(:clj
             (doseq [idx-ident (detect-new-building-indices tx-report)]
               (log/trace :datahike/dispatch-backfill {:idx-ident idx-ident})
               ;; build-secondary-index! is async (returns channel).
               ;; When it completes, dispatch install to swap in the result.
               (go
                 (let [build-result (<! (dispatch! writer {:op 'build-secondary-index!
                                                           :args [idx-ident]}))]
                   (when (map? build-result)
                     (dispatch! writer {:op 'install-secondary-index!
                                        :args [build-result]}))))))
          (notify-listeners! connection listeners tx-report))))
    p)))))

(defn load-entities [connection entities]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (dispatch! writer
                                     {:op 'load-entities
                                      :args [entities]}))]
        (#?(:jolt (fn [p v] (p v)) :clj deliver :cljs put!) p tx-report)))
    p))

(defn merge-db!
  "Merge parent branches/commits into the current branch through the writer.
   Parents is a set of branch keywords or commit UUIDs.
   tx-data contains the merged changes."
  [connection {:keys [parents tx-data tx-meta] :as arg-map}]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (if (instance? SyncWriter writer)
      ;; a :sync writer merges on the calling thread, as its transact! does: no go block, so
      ;; it works in a process with no scheduler threads (a forked agent; seon lane A1)
      (let [tx-report (try (sync-apply! connection (:write-fn-map writer) (:last-cid writer) {:op 'merge! :args [arg-map]})
                           (catch #?(:clj Throwable :cljs js/Error) e e))]
        (#?(:jolt (fn [p v] (p v)) :clj deliver :cljs put!) p tx-report)
        (when (map? tx-report) (notify-listeners! connection (some-> (:listeners (meta connection)) deref) tx-report))
        p)
      (do (go
            (let [tx-report (<! (dispatch! writer
                                           {:op 'merge!
                                            :args [arg-map]}))
                  listeners (some-> (:listeners (meta connection)) deref)]
              (#?(:jolt (fn [p v] (p v)) :clj deliver :cljs put!) p tx-report)
              (when (map? tx-report)
                (notify-listeners! connection listeners tx-report))))
          p))))

(defn gc-storage! [conn & args]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom conn))]
    (go
      (let [result (<! (dispatch! writer
                                  {:op 'gc-storage!
                                   :args (vec args)}))]
        (#?(:jolt (fn [p v] (p v)) :clj deliver :cljs put!) p result)))
    p))

;; proto (lane F1): a :sync writer's database is created and deleted as :self's is
(defmethod create-database :sync [& args] (apply (get-method create-database :self) args))
(defmethod delete-database :sync [& args] (apply (get-method delete-database :self) args))
