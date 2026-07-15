(ns ^:no-doc datahike.query.single-flight)

(def ^:dynamic *max-active-flights*
  "Maximum distinct cold computations coordinated in one process."
  1024)

(defonce ^:private flights (atom {}))
(def ^:private counter-names
  [:owners :waiter-hits :successes :failures :overflows
   :reentrant-bypasses :cancellations :last-waiters :stale-finishes
   :compute-nanos :wait-nanos :cache-stores :cache-skips])

(defonce ^:private counters
  #?(:clj (into {} (map (fn [counter]
                          [counter (java.util.concurrent.atomic.LongAdder.)]))
                        counter-names)
     :cljs (atom (zipmap counter-names (repeat 0)))))

(defn- counter-values []
  #?(:clj (into {} (map (fn [[counter value]]
                          [counter (.sum ^java.util.concurrent.atomic.LongAdder value)]))
                        counters)
     :cljs @counters))

(defn- increment! [counter]
  #?(:clj (.increment ^java.util.concurrent.atomic.LongAdder (get counters counter))
     :cljs (swap! counters update counter (fnil inc 0))))

(defn- add! [counter n]
  #?(:clj (.add ^java.util.concurrent.atomic.LongAdder (get counters counter) n)
     :cljs (swap! counters update counter (fnil + 0) n)))

#?(:clj
   (defn acquire!
     "Acquire one owner or waiter handle for an exact in-flight key."
     [flight-key]
     (let [waiter-id (random-uuid)
           owner-thread (Thread/currentThread)]
       (loop []
         (let [before @flights]
           (if-let [entry (get before flight-key)]
             (if (identical? owner-thread (:owner-thread entry))
               (do (increment! :reentrant-bypasses)
                   {:state :reentrant})
               (let [after (assoc before flight-key
                                  (update entry :waiters conj waiter-id))]
                 (if (compare-and-set! flights before after)
                   (do (increment! :waiter-hits)
                       {:state :waiter :waiter-id waiter-id
                        :wait-start (System/nanoTime)
                        :completion (:completion entry)})
                   (recur))))
             (if (>= (count before) *max-active-flights*)
               (do (increment! :overflows) {:state :overflow})
               (let [completion (promise)
                     entry {:completion completion
                            :owner-thread owner-thread
                            :waiters #{waiter-id}
                            :cancel (volatile! false)}]
                 (if (compare-and-set! flights before
                                       (assoc before flight-key entry))
                   (do (increment! :owners)
                       {:state :owner :waiter-id waiter-id
                        :completion completion :entry entry})
                   (recur))))))))))

#?(:clj
   (defn complete!
     "Deliver one tagged completion and compare-remove its exact entry."
     [flight-key entry completion]
     (loop []
       (let [before @flights]
         (if (identical? (:completion entry)
                         (get-in before [flight-key :completion]))
           (if (compare-and-set! flights before (dissoc before flight-key))
             (deliver (:completion entry) completion)
             (recur))
           (increment! :stale-finishes))))))

(defn execute!
  "Compute once per exact key on CLJ; preserve direct synchronous CLJS semantics."
  [flight-key cache-read compute cache-success!]
  #?(:cljs
     (if-some [cached (cache-read)]
       (:value cached)
       (let [value (compute nil)]
         (cache-success! value)
         value))
     :clj
     (if-some [cached (cache-read)]
       (:value cached)
       (let [{:keys [state completion entry wait-start]} (acquire! flight-key)]
         (case state
           (:overflow :reentrant)
           (if-some [cached (cache-read)]
             (:value cached)
             (let [value (compute nil)]
               (cache-success! value)
               value))

           :waiter
           (let [{:keys [status value throwable]} @completion]
             (add! :wait-nanos (- (System/nanoTime) wait-start))
             (if (= :ok status) value (throw throwable)))

           :owner
           (try
             (if-some [cached (cache-read)]
               (do (increment! :successes)
                   (complete! flight-key entry {:status :ok :value (:value cached)})
                   (:value cached))
               (let [started (System/nanoTime)
                     value (compute (:cancel entry))
                     ;; Release, shutdown, or explicit cache clearing may have
                     ;; removed this owner while it was computing. Only a still
                     ;; admitted owner may populate the completed cache.
                     admitted? (identical?
                                (:completion entry)
                                (get-in @flights [flight-key :completion]))
                     cached? (and admitted? (cache-success! value))]
                 (add! :compute-nanos (- (System/nanoTime) started))
                 (increment! (if cached? :cache-stores :cache-skips))
                 (increment! :successes)
                 (complete! flight-key entry {:status :ok :value value})
                 value))
             (catch Throwable throwable
               (increment! :failures)
               (complete! flight-key entry
                          {:status :error :throwable throwable})
               (throw throwable))))))))

(defn cancel-waiter!
  "Detach one waiter; signal cooperative cancellation when it was the last."
  [flight-key waiter-id]
  #?(:cljs false
     :clj
     (loop []
       (let [before @flights
             entry (get before flight-key)]
         (if-not (contains? (:waiters entry) waiter-id)
           false
           (let [waiters (disj (:waiters entry) waiter-id)
                 after (assoc before flight-key (assoc entry :waiters waiters))]
             (if (compare-and-set! flights before after)
               (do
                 (increment! :cancellations)
                 (when (empty? waiters)
                   (vreset! (:cancel entry) true)
                   (increment! :last-waiters))
                 true)
               (recur))))))))

(defn close-scope!
  "Remove and fail every in-flight computation for one exact cache scope."
  [connection-id generation]
  #?(:cljs nil
     :clj
     (let [removed (volatile! nil)]
       (swap! flights
              (fn [current]
                (let [[drop keep]
                      ((juxt filter remove)
                       (fn [[[[cached-id cached-generation _] _] _]]
                         (and (= connection-id cached-id)
                              (= generation cached-generation)))
                       current)]
                  (vreset! removed (mapv second drop))
                  (into {} keep))))
       (doseq [{:keys [completion cancel]} @removed]
         (vreset! cancel true)
         (deliver completion
                  {:status :error
                   :throwable (ex-info "Query cache scope closed."
                                       {:type :datahike/query-cache-scope-closed})}))
       nil)))

(defn clear!
  "Fail and remove every process-local in-flight computation."
  []
  #?(:cljs nil
     :clj
     (let [current (first (reset-vals! flights {}))]
       (doseq [{:keys [completion cancel]} (vals current)]
         (vreset! cancel true)
         (deliver completion
                  {:status :error
                   :throwable (ex-info "Query coordinator cleared."
                                       {:type :datahike/query-coordinator-cleared})}))))
  nil)

(defn metrics
  "Return non-retaining single-flight counters and current gauges."
  []
  (let [current @flights
        totals (counter-values)]
    (merge totals
           {:active-flights (count current)
            :active-callers (reduce + 0 (map (comp count :waiters val) current))
            :active-waiters (reduce + 0
                                    (map (fn [[_ {:keys [waiters]}]]
                                           (max 0 (dec (count waiters))))
                                         current))
            :saved-computations (:waiter-hits totals)
            :active-by-database
            (reduce-kv
             (fn [result [database-key _] {:keys [waiters]}]
               (-> result
                   (update-in [database-key :flights] (fnil inc 0))
                   (update-in [database-key :callers] (fnil + 0)
                              (count waiters))
                   (update-in [database-key :waiters] (fnil + 0)
                              (max 0 (dec (count waiters))))))
             {} current)})))
