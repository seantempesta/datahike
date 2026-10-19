(ns ^:no-doc datahike.committed-report
  #?(:clj (:import [java.util.concurrent ArrayBlockingQueue])))

(def ^:private empty-queue #?(:clj clojure.lang.PersistentQueue/EMPTY
                              :cljs cljs.core/PersistentQueue.EMPTY))

(defonce ^:private sources (atom {}))
(def ^:private maximum-active-sources 4096)
(defonce ^:private ready-sources
  #?(:clj (ArrayBlockingQueue. maximum-active-sources)
     :cljs (atom empty-queue)))

(defn- with-source-lock [source f]
  #?(:clj (locking source (f))
     :cljs (f)))

(defn- public-evidence [state]
  {:datahike.committed-report/status
   (keyword "datahike.committed-report.status" (name (:status state)))
   :datahike.committed-report/queued (:queued state)
   :datahike.committed-report/offered (:offered state)
   :datahike.committed-report/delivered (:delivered state)
   :datahike.committed-report/overflowed (:overflowed state)
   :datahike.committed-report/stale-rejected (:stale-rejected state)
   :datahike.committed-report/abandoned (:abandoned state)})

(defn- enqueue-ready! [source]
  ;; One entry per active source makes this bound structural: a source becomes
  ;; ready again only after its prior entry has been removed.
  #?(:clj
     (when-not (.offer ^ArrayBlockingQueue ready-sources source)
       (throw (ex-info "Committed-report readiness capacity exhausted."
                       {:type :datahike.committed-report/readiness-capacity
                        :maximum maximum-active-sources})))
     :cljs
     (swap! ready-sources conj source)))

(defn- remove-ready! [source]
  #?(:clj
     (let [iterator (.iterator ^ArrayBlockingQueue ready-sources)]
       (loop []
         (when (.hasNext iterator)
           (if (identical? source (.next iterator))
             (.remove iterator)
             (recur)))))
     :cljs
     (swap! ready-sources
            (fn [ready]
              (into empty-queue (remove #(identical? source %)) ready)))))

(defn- registered-ready-source [source]
  (with-source-lock
    source
    (fn []
      (let [state @(:state source)]
        (when (and (:readiness-queued? state)
                   (identical? source (get @sources (:scope source))))
          (swap! (:state source) assoc :readiness-queued? false)
          source)))))

(defn- scope-of [db]
  (let [{:datahike.cache/keys [connection-id generation]}
        (:cache-context db)]
    (when (and connection-id generation)
      [connection-id generation])))

(defn open!
  "Open one bounded committed-report source for an exact connection generation."
  [connection-id generation capacity]
  (when-not (pos-int? capacity)
    (throw (ex-info "Committed-report capacity must be positive."
                    {:type :datahike.committed-report/invalid-capacity
                     :capacity capacity})))
  (let [scope [connection-id generation]
        candidate {:scope scope
                   :capacity capacity
                   :state (atom {:status :open
                                 :queue empty-queue
                                 :queued 0
                                 :ready? false
                                 :readiness-queued? false
                                 :offered 0
                                 :delivered 0
                                 :overflowed 0
                                 :stale-rejected 0
                                 :abandoned 0})}]
    (get (swap! sources
                (fn [current]
                  (cond
                    (contains? current scope) current
                    (< (count current) maximum-active-sources)
                    (assoc current scope candidate)
                    :else
                    (throw (ex-info "Too many active committed-report sources."
                                    {:type :datahike.committed-report/source-limit
                                     :maximum maximum-active-sources})))))
         scope)))

(defn offer!
  "Offer one committed report without blocking or invoking downstream work."
  [source generation report]
  (let [[_ expected-generation] (:scope source)
        outcome (volatile! :rejected)
        became-ready? (volatile! false)]
    (with-source-lock
      source
      (fn []
        (swap! (:state source)
               (fn [{:keys [status queued ready?] :as state}]
                 (cond
                   (not= expected-generation generation)
                   (do (vreset! outcome :stale)
                       (update state :stale-rejected inc))

                   (not= :open status)
                   (do (vreset! outcome :closed)
                       (update state :stale-rejected inc))

                   (< queued (:capacity source))
                   (do (vreset! outcome :accepted)
                       (when-not ready? (vreset! became-ready? true))
                       (cond-> (-> state
                                   (assoc :ready? true)
                                   (update :queue conj report)
                                   (update :queued inc)
                                   (update :offered inc))
                         (not ready?) (assoc :readiness-queued? true)))

                   :else
                   (do (vreset! outcome :overflow)
                       (when-not ready? (vreset! became-ready? true))
                       (cond-> (-> state
                                   (assoc :status :gapped :ready? true)
                                   (update :overflowed inc))
                         (not ready?) (assoc :readiness-queued? true))))))
        (when @became-ready?
          (enqueue-ready! source))))
    @outcome))

(defn offer-committed!
  "Offer a durable report to its demand-opened generation source, if any."
  [db report]
  (when-let [[_ generation :as scope] (scope-of db)]
    (when-let [source (get @sources scope)]
      (offer! source generation report))))

(defn poll!
  "Take the next accepted report from a source without blocking."
  [source]
  (let [report (volatile! nil)
        remove-readiness? (volatile! false)]
    (with-source-lock
      source
      (fn []
        (swap! (:state source)
               (fn [{:keys [queue queued] :as state}]
                 (if (pos? queued)
                   (do (vreset! report (peek queue))
                       (when (= 1 queued)
                         (vreset! remove-readiness?
                                  (:readiness-queued? state)))
                       (cond-> (-> state
                                   (assoc :queue (pop queue))
                                   (update :queued dec)
                                   (update :delivered inc))
                         (= 1 queued)
                         (assoc :ready? false :readiness-queued? false)))
                   state)))
        (when (and @report
                   (not (:ready? @(:state source)))
                   @remove-readiness?)
          (remove-ready! source))))
    @report))

(defn poll-batch!
  "Take at most `limit` accepted reports from a source without blocking.

  This operation never requeues the source. The source owner calls
  `requeue-ready!` after its bounded delivery job completes, or before
  delivery when admission is rejected without polling a batch."
  [source limit]
  (when-not (pos-int? limit)
    (throw (ex-info "Committed-report batch limit must be positive."
                    {:type :datahike.committed-report/invalid-batch-limit
                     :limit limit})))
  (let [result (volatile! nil)
        remove-readiness? (volatile! false)]
    (with-source-lock
      source
      (fn []
        (swap! (:state source)
               (fn [{:keys [status queue queued readiness-queued?] :as state}]
                 (let [[remaining reports]
                       (loop [remaining limit
                              queue queue
                              reports []]
                         (if (and (pos? remaining) (seq queue))
                           (recur (dec remaining)
                                  (pop queue)
                                  (conj reports (peek queue)))
                           [queue reports]))
                       delivered (count reports)
                       queued-after (- queued delivered)
                       more? (or (pos? queued-after) (= :gapped status))
                       ready-after? (and (not= :closed status) more?)
                       state-after (cond-> (-> state
                                                (assoc :queue remaining
                                                       :queued queued-after
                                                       :ready? ready-after?)
                                                (update :delivered + delivered))
                                     (not ready-after?)
                                     (assoc :readiness-queued? false))]
                   (when (and readiness-queued? (not ready-after?))
                     (vreset! remove-readiness? true))
                   (vreset! result
                            {:datahike.committed-report/status
                             (keyword "datahike.committed-report.status"
                                      (name status))
                             :datahike.committed-report/reports reports
                             :datahike.committed-report/more? more?})
                   state-after)))
        (when @remove-readiness?
          (remove-ready! source))))
    @result))

(defn requeue-ready!
  "Put one still-registered ready source at the readiness queue tail.

  Requeue is identity-fenced and idempotent. It does not inspect or consume
  reports."
  [source]
  (let [outcome (volatile! :stale)]
    (with-source-lock
      source
      (fn []
        (let [{:keys [status ready? readiness-queued?]} @(:state source)]
          (cond
            (not (identical? source (get @sources (:scope source))))
            (vreset! outcome :stale)

            (= :closed status)
            (vreset! outcome :closed)

            (not (or ready? (= :gapped status)))
            (vreset! outcome :not-ready)

            readiness-queued?
            (vreset! outcome :already-queued)

            :else
            (do
              (swap! (:state source) assoc
                     :ready? true
                     :readiness-queued? true)
              (enqueue-ready! source)
              (vreset! outcome :requeued))))))
    @outcome))

(defn poll-ready!
  "Take one source whose accepted reports or gap are ready to inspect."
  []
  (loop []
    (when-let [source
               #?(:clj (.poll ^ArrayBlockingQueue ready-sources)
                  :cljs
                  (let [source (volatile! nil)]
                    (swap! ready-sources
                           (fn [ready]
                             (if (seq ready)
                               (do (vreset! source (peek ready))
                                   (pop ready))
                               ready)))
                    @source))]
      (or (registered-ready-source source)
          (recur)))))

#?(:clj
   (defn take-ready!
     "Block until one registered source has accepted reports or a gap."
     []
     (loop []
       (let [source (.take ^ArrayBlockingQueue ready-sources)]
         (or (registered-ready-source source)
             (recur))))))

(defn readiness-evidence
  "Return bounded process-wide committed-report readiness evidence."
  []
  {:datahike.committed-report.readiness/queued
   #?(:clj (.size ^ArrayBlockingQueue ready-sources)
      :cljs (count @ready-sources))
   :datahike.committed-report.readiness/capacity maximum-active-sources
   :datahike.committed-report.readiness/active-sources (count @sources)})

(defn close!
  "Fence one exact source and either retain or abandon its accepted reports."
  [source drain?]
  (let [scope (:scope source)]
    (with-source-lock
      source
      (fn []
        (swap! sources
               (fn [current]
                 (if (identical? source (get current scope))
                   (dissoc current scope)
                   current)))
        (remove-ready! source)
        (swap! (:state source)
               (fn [{:keys [queued] :as state}]
                 (cond-> (assoc state
                                :status :closed
                                :ready? false
                                :readiness-queued? false)
                   (not drain?) (-> (assoc :queue empty-queue :queued 0)
                                    (update :abandoned + queued)))))))
    (public-evidence @(:state source))))

(defn close-scope!
  "Fence and close the source for one exact connection generation, if open."
  [connection-id generation drain?]
  (if-let [source (get @sources [connection-id generation])]
    (close! source drain?)
    {:datahike.committed-report/status
     :datahike.committed-report.status/absent
     :datahike.committed-report/queued 0
     :datahike.committed-report/offered 0
     :datahike.committed-report/delivered 0
     :datahike.committed-report/overflowed 0
     :datahike.committed-report/stale-rejected 0
     :datahike.committed-report/abandoned 0}))

(defn evidence
  "Return ordinary bounded-source state without reports or runtime objects."
  [source]
  (public-evidence @(:state source)))

(defn active-source-count
  "Return the number of demand-opened committed-report sources."
  []
  (count @sources))

(defn clear!
  "Abandon and remove every committed-report source."
  []
  (let [current (vals (swap! sources (constantly {})))]
    (doseq [source current]
      (close! source false))
    #?(:clj (.clear ^ArrayBlockingQueue ready-sources)
       :cljs (reset! ready-sources empty-queue))
    nil))
