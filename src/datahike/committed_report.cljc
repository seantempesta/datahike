(ns ^:no-doc datahike.committed-report)

(def ^:private empty-queue #?(:clj clojure.lang.PersistentQueue/EMPTY
                              :cljs cljs.core/PersistentQueue.EMPTY))

(defonce ^:private sources (atom {}))

(defn- public-evidence [state]
  {:datahike.committed-report/status
   (keyword "datahike.committed-report.status" (name (:status state)))
   :datahike.committed-report/queued (:queued state)
   :datahike.committed-report/offered (:offered state)
   :datahike.committed-report/delivered (:delivered state)
   :datahike.committed-report/overflowed (:overflowed state)
   :datahike.committed-report/stale-rejected (:stale-rejected state)
   :datahike.committed-report/abandoned (:abandoned state)})

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
                                 :offered 0
                                 :delivered 0
                                 :overflowed 0
                                 :stale-rejected 0
                                 :abandoned 0})}]
    (get (swap! sources
                (fn [current]
                  (if (contains? current scope)
                    current
                    (assoc current scope candidate))))
         scope)))

(defn offer!
  "Offer one committed report without blocking or invoking downstream work."
  [source generation report]
  (let [[_ expected-generation] (:scope source)
        outcome (volatile! :rejected)]
    (swap! (:state source)
           (fn [{:keys [status queued] :as state}]
             (cond
               (not= expected-generation generation)
               (do (vreset! outcome :stale)
                   (update state :stale-rejected inc))

               (not= :open status)
               (do (vreset! outcome :closed)
                   (update state :stale-rejected inc))

               (< queued (:capacity source))
               (do (vreset! outcome :accepted)
                   (-> state
                       (update :queue conj report)
                       (update :queued inc)
                       (update :offered inc)))

               :else
               (do (vreset! outcome :overflow)
                   (-> state
                       (assoc :status :gapped)
                       (update :overflowed inc))))))
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
  (let [report (volatile! nil)]
    (swap! (:state source)
           (fn [{:keys [queue queued] :as state}]
             (if (pos? queued)
               (do (vreset! report (peek queue))
                   (-> state
                       (assoc :queue (pop queue))
                       (update :queued dec)
                       (update :delivered inc)))
               state)))
    @report))

(defn close!
  "Fence one exact source and either retain or abandon its accepted reports."
  [source drain?]
  (let [scope (:scope source)]
    (swap! sources
           (fn [current]
             (if (identical? source (get current scope))
               (dissoc current scope)
               current)))
    (swap! (:state source)
           (fn [{:keys [queued] :as state}]
             (cond-> (assoc state :status :closed)
               (not drain?) (-> (assoc :queue empty-queue :queued 0)
                                (update :abandoned + queued)))))
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
    nil))
