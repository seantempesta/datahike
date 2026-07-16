(ns ^:no-doc datahike.query.single-flight)

(def ^:dynamic *max-active-flights*
  "Maximum distinct cold computations coordinated in one process."
  1024)

;; One atom makes flight admission and waiter reverse lookup one atomic fact.
(defonce ^:private coordinator (atom {:flights {} :waiters {}}))
(def ^:private counter-names
  [:owners :waiter-hits :successes :failures :overflows
   :reentrant-bypasses :cancellations :last-waiters :stale-finishes
   :compute-nanos :wait-nanos :cache-stores :cache-skips])

(defonce ^:private counters
  #?(:clj (into {} (map (fn [counter]
                          [counter (java.util.concurrent.atomic.LongAdder.)]))
                counter-names)
     :cljs (atom (zipmap counter-names (repeat 0)))))

#?(:clj
   (deftype CompletionCell [completion state]
     clojure.lang.IDeref
     (deref [_] @completion)

     clojure.lang.IBlockingDeref
     (deref [_ timeout-ms timeout-value]
       (deref completion timeout-ms timeout-value))))

#?(:clj
   (defn- completion-cell []
     (CompletionCell. (promise) (atom {:completed? false
                                       :callback-registered? false
                                       :callback nil}))))

#?(:clj
   (defn- complete-cell!
     [^CompletionCell cell value]
     (let [callback (volatile! nil)
           completed? (volatile! false)]
       (swap! (.-state cell)
              (fn [state]
                (if (:completed? state)
                  (do (vreset! completed? true) state)
                  (do (vreset! callback (:callback state))
                      (assoc state
                             :completed? true
                             :value value
                             :callback nil)))))
       (when-not @completed?
         (deliver (.-completion cell) value)
         (when-let [f @callback]
           (try (f value) (catch Throwable _))))
       (not @completed?))))

#?(:clj
   (defn on-complete!
     "Register one nonblocking callback on an opaque completion cell."
     [^CompletionCell cell callback]
     (let [completed (volatile! nil)
           registered? (volatile! false)]
       (swap! (.-state cell)
              (fn [state]
                (cond
                  (:callback-registered? state) state
                  (:completed? state)
                  (do (vreset! completed (:value state))
                      (vreset! registered? true)
                      (assoc state :callback-registered? true))
                  :else
                  (do (vreset! registered? true)
                      (assoc state
                             :callback-registered? true
                             :callback callback)))))
       (when (some? @completed)
         (try (callback @completed) (catch Throwable _))
         nil)
       @registered?)))

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
     ([flight-key] (acquire! flight-key (random-uuid)))
     ([flight-key request-id]
      (let [owner-thread (Thread/currentThread)]
        (loop []
          (let [{:keys [flights waiters] :as before} @coordinator]
            (when (contains? waiters request-id)
              (throw (ex-info "Query request identity is already active."
                              {:type :datahike/duplicate-query-request
                               :datahike.query/request-id request-id})))
            (if-let [entry (get flights flight-key)]
              (if (and (= :running (:phase entry))
                       (identical? owner-thread (:owner-thread entry)))
                (do (increment! :reentrant-bypasses)
                    {:state :reentrant})
                (let [completion (completion-cell)
                      after (-> before
                                (assoc-in [:flights flight-key :waiters request-id]
                                          completion)
                                (assoc-in [:waiters request-id] flight-key))]
                  (if (compare-and-set! coordinator before after)
                    (do (increment! :waiter-hits)
                        {:state :waiter :request-id request-id
                         :wait-start (System/nanoTime)
                         :completion completion})
                    (recur))))
              (if (>= (count flights) *max-active-flights*)
                (do (increment! :overflows) {:state :overflow})
                (let [completion (completion-cell)
                      entry {:owner-thread nil
                             :phase :acquired
                             :owner-request-id request-id
                             :waiters {request-id completion}
                             :cancel (volatile! false)}
                      after (-> before
                                (assoc-in [:flights flight-key] entry)
                                (assoc-in [:waiters request-id] flight-key))]
                  (if (compare-and-set! coordinator before after)
                    (do (increment! :owners)
                        {:state :owner :request-id request-id
                         :completion completion :entry entry})
                    (recur)))))))))))

#?(:clj
   (defn start!
     "Bind an acquired owner to its actual execution thread exactly once."
     [flight-key entry]
     (loop []
       (let [{:keys [flights] :as before} @coordinator
             current (get flights flight-key)]
         (if (and (identical? (:cancel entry) (:cancel current))
                  (= :acquired (:phase current)))
           (let [after (assoc-in before [:flights flight-key]
                                 (assoc current
                                        :phase :running
                                        :owner-thread (Thread/currentThread)))]
             (if (compare-and-set! coordinator before after)
               true
               (recur)))
           false)))))

#?(:clj
   (defn complete!
     "Deliver one tagged completion and compare-remove its exact entry."
     [flight-key entry completion]
     (loop []
       (let [{:keys [flights] :as before} @coordinator
             current (get flights flight-key)]
         (if (identical? (:cancel entry) (:cancel current))
           (let [waiter-ids (keys (:waiters current))
                 after (-> before
                           (update :flights dissoc flight-key)
                           (update :waiters #(apply dissoc % waiter-ids)))]
             (if (compare-and-set! coordinator before after)
               (doseq [cell (vals (:waiters current))]
                 (complete-cell! cell completion))
               (recur)))
           (increment! :stale-finishes))))))

#?(:clj
   (deftype ExecutionCall [state completion run finish started]))

#?(:clj
   (defn call-state
     "Return bounded ordinary state for an opaque acquired execution."
     [^ExecutionCall call]
     (.-state call)))

#?(:clj
   (defn- finish-completion
     [^ExecutionCall call completion]
     ((.-finish call) completion)))

#?(:clj
   (defn await-call!
     "Wait for one acquired execution and return its caller-specific value."
     [^ExecutionCall call]
     (finish-completion call @(.-completion call))))

#?(:clj
   (defn on-call-complete!
     "Register one bounded nonblocking handoff for an acquired execution."
     [^ExecutionCall call callback]
     (on-complete!
      (.-completion call)
      (fn [completion]
        (try
          (callback {:status :ok :value (finish-completion call completion)})
          (catch Throwable throwable
            (callback {:status :error :throwable throwable})))))))

#?(:clj
   (defn run-call!
     "Run an acquired owner exactly once on the current thread."
     [^ExecutionCall call]
     (if-let [run (.-run call)]
       (if (compare-and-set! (.-started call) false true)
         (run)
         (throw (ex-info "Query call has already started."
                         {:type :datahike/query-call-already-started})))
       (throw (ex-info "Query call does not own computation."
                       {:type :datahike/query-call-not-runnable
                        :datahike.query.call/state (.-state call)})))))

#?(:clj
   (defn- completed-call
     [state completion-value finish]
     (let [completion (completion-cell)]
       (complete-cell! completion completion-value)
       (ExecutionCall. state completion nil finish (atom false)))))

#?(:clj
   (defn acquire-completed!
     "Return an opaque already-completed execution."
     [value]
     (completed-call
      :completed {:status :ok :value value}
      (fn [{:keys [status value throwable]}]
        (if (= :ok status) value (throw throwable))))))

#?(:clj
   (defn acquire-direct!
     "Return an opaque exactly-once direct computation owner."
     [compute]
     (let [completion (completion-cell)
           finish (fn [{:keys [status value throwable]}]
                    (if (= :ok status) value (throw throwable)))
           run (fn []
                 (try
                   (let [value (compute)]
                     (complete-cell! completion {:status :ok :value value})
                     value)
                   (catch Throwable throwable
                     (complete-cell! completion
                                     {:status :error :throwable throwable})
                     (throw throwable))))]
       (ExecutionCall. :run completion run finish (atom false)))))

#?(:clj
   (defn- acquire-cache-aware-direct!
     [cache-read compute cache-success! evidence! miss-outcome]
     (acquire-direct!
      (fn []
        (if-some [cached (cache-read)]
          (do
            (when evidence!
              (evidence! {:datahike.cache/outcome
                          :datahike.cache.outcome/hit-after-acquire
                          :datahike.cache/stored? true
                          :datahike.cache/saved-computation? false}))
            (:value cached))
          (let [value (compute nil)]
            (cache-success! value)
            (when evidence!
              (evidence! {:datahike.cache/outcome miss-outcome
                          :datahike.cache/stored? false
                          :datahike.cache/saved-computation? false}))
            value))))))

#?(:clj
   (defn acquire-execution!
     "Acquire one opaque owner, waiter, completed value, or rejection."
     [flight-key request-id cache-read compute cache-success! evidence!
      completion-data joined! reject-overflow?]
     (let [finish (fn [{:keys [status value throwable]}]
                    (if (= :ok status) value (throw throwable)))]
       (if-some [cached (cache-read)]
         (do
           (when evidence!
             (evidence! {:datahike.cache/outcome :datahike.cache.outcome/hit
                         :datahike.cache/stored? true
                         :datahike.cache/saved-computation? false}))
           (completed-call :completed {:status :ok :value (:value cached)} finish))
         (let [{:keys [state completion entry wait-start]}
               (if request-id
                 (acquire! flight-key request-id)
                 (acquire! flight-key))]
           (case state
             :overflow
             (if reject-overflow?
               (completed-call
                :rejected
                {:status :error
                 :throwable
                 (ex-info "Query single-flight capacity is full."
                          {:type :datahike/query-coordinator-full})}
                finish)
               (acquire-cache-aware-direct!
                cache-read compute cache-success! evidence!
                :datahike.cache.outcome/miss-overflow))

             :reentrant
             (acquire-cache-aware-direct!
              cache-read compute cache-success! evidence!
              :datahike.cache.outcome/miss-reentrant)

             :waiter
             (ExecutionCall.
              :waiting completion nil
              (fn [{:keys [status value throwable stored?] :as completed}]
                (add! :wait-nanos (- (System/nanoTime) wait-start))
                (when joined! (joined! completed))
                (when evidence!
                  (evidence! {:datahike.cache/outcome
                              :datahike.cache.outcome/miss-joined
                              :datahike.cache/stored? (boolean stored?)
                              :datahike.cache/saved-computation? true}))
                (if (= :ok status) value (throw throwable)))
              (atom false))

             :owner
             (let [run
                   (fn []
                     (if-not (start! flight-key entry)
                       (await-call!
                        (ExecutionCall. :waiting completion nil finish
                                        (atom false)))
                       (try
                         (if-some [cached (cache-read)]
                           (do
                             (increment! :successes)
                             (when evidence!
                               (evidence!
                                {:datahike.cache/outcome
                                 :datahike.cache.outcome/hit-after-acquire
                                 :datahike.cache/stored? true
                                 :datahike.cache/saved-computation? false}))
                             (complete!
                              flight-key entry
                              (merge {:status :ok
                                      :value (:value cached)
                                      :stored? true}
                                     (when completion-data
                                       (completion-data))))
                             (:value cached))
                           (let [started (System/nanoTime)
                                 value (compute (:cancel entry))
                                 admitted?
                                 (identical?
                                  (:cancel entry)
                                  (get-in @coordinator
                                          [:flights flight-key :cancel]))
                                 cached? (and admitted?
                                              (cache-success! value))]
                             (add! :compute-nanos
                                   (- (System/nanoTime) started))
                             (increment! (if cached? :cache-stores
                                             :cache-skips))
                             (increment! :successes)
                             (when evidence!
                               (evidence!
                                {:datahike.cache/outcome
                                 :datahike.cache.outcome/miss-owner
                                 :datahike.cache/stored? (boolean cached?)
                                 :datahike.cache/saved-computation? false}))
                             (complete!
                              flight-key entry
                              (merge {:status :ok :value value
                                      :stored? (boolean cached?)}
                                     (when completion-data
                                       (completion-data))))
                             value))
                         (catch Throwable throwable
                           (increment! :failures)
                           (complete! flight-key entry
                                      {:status :error
                                       :throwable throwable})
                           (throw throwable)))))]
               (ExecutionCall. :run completion run finish (atom false)))))))))

(defn execute!
  "Compute once per exact key on CLJ; preserve direct synchronous CLJS semantics."
  ([flight-key cache-read compute cache-success!]
   (execute! flight-key nil cache-read compute cache-success! nil))
  ([flight-key cache-read compute cache-success! evidence!]
   (execute! flight-key nil cache-read compute cache-success! evidence!))
  ([flight-key request-id cache-read compute cache-success! evidence!]
   (execute! flight-key request-id cache-read compute cache-success! evidence!
             nil nil))
  ([flight-key request-id cache-read compute cache-success! evidence!
    completion-data joined!]
   #?(:cljs
      (if-some [cached (cache-read)]
        (do (when evidence!
              (evidence! {:datahike.cache/outcome :datahike.cache.outcome/hit
                          :datahike.cache/stored? true
                          :datahike.cache/saved-computation? false}))
            (:value cached))
        (let [value (compute nil)
              stored? (boolean (cache-success! value))]
          (when evidence!
            (evidence! {:datahike.cache/outcome
                        :datahike.cache.outcome/miss-owner
                        :datahike.cache/stored? stored?
                        :datahike.cache/saved-computation? false}))
          value))
      :clj
      (let [call (acquire-execution!
                  flight-key request-id cache-read compute cache-success!
                  evidence! completion-data joined! false)]
        (if (= :run (call-state call))
          (run-call! call)
          (await-call! call))))))

(defn cancel!
  "Detach and wake one caller by its public request identity."
  [request-id]
  #?(:cljs {:datahike.query.cancel/request-id request-id
            :datahike.query.cancel/found? false
            :datahike.query.cancel/detached? false
            :datahike.query.cancel/last-waiter? false
            :datahike.query.cancel/cooperative-signal-set? false}
     :clj
     (loop []
       (let [{:keys [flights waiters] :as before} @coordinator
             flight-key (get waiters request-id)
             entry (get flights flight-key)
             completion (get-in entry [:waiters request-id])]
         (if-not completion
           {:datahike.query.cancel/request-id request-id
            :datahike.query.cancel/found? false
            :datahike.query.cancel/detached? false
            :datahike.query.cancel/last-waiter? false
            :datahike.query.cancel/cooperative-signal-set? false}
           (let [remaining (dissoc (:waiters entry) request-id)
                 last? (empty? remaining)
                 unstarted-owner? (and last? (= :acquired (:phase entry)))
                 after (if unstarted-owner?
                         (-> before
                             (update :flights dissoc flight-key)
                             (update :waiters dissoc request-id))
                         (-> before
                             (assoc-in [:flights flight-key :waiters] remaining)
                             (update :waiters dissoc request-id)))]
             (if (compare-and-set! coordinator before after)
               (do
                 (increment! :cancellations)
                 (when last?
                   (vreset! (:cancel entry) true)
                   (increment! :last-waiters))
                 (complete-cell!
                  completion
                  {:status :error
                   :throwable
                   (ex-info "Query waiter canceled."
                            {:type :datahike/query-canceled
                             :datahike.query/request-id request-id})})
                 {:datahike.query.cancel/request-id request-id
                  :datahike.query.cancel/found? true
                  :datahike.query.cancel/detached? true
                  :datahike.query.cancel/last-waiter? last?
                  :datahike.query.cancel/cooperative-signal-set? last?})
               (recur))))))))

(defn cancel-waiter!
  "Compatibility wrapper; the exact flight key is no longer required."
  ([request-id]
   (:datahike.query.cancel/detached? (cancel! request-id)))
  ([_flight-key request-id]
   (:datahike.query.cancel/detached? (cancel! request-id))))

(defn- remove-flights! [selected error]
  #?(:cljs nil
     :clj
     (let [removed (volatile! nil)]
       (swap! coordinator
              (fn [{:keys [flights waiters] :as state}]
                (let [drop (into {} (filter selected) flights)
                      waiter-ids (mapcat (comp keys :waiters val) drop)]
                  (vreset! removed (vals drop))
                  (-> state
                      (update :flights #(apply dissoc % (keys drop)))
                      (assoc :waiters (apply dissoc waiters waiter-ids))))))
       (doseq [{:keys [waiters cancel]} @removed]
         (vreset! cancel true)
         (doseq [completion (vals waiters)]
           (complete-cell! completion {:status :error :throwable error})))
       {:datahike.single-flight.release/flights (count @removed)
        :datahike.single-flight.release/callers
        (reduce + 0 (map (comp count :waiters) @removed))})))

(defn close-scope!
  "Remove and fail every in-flight computation for one exact cache scope."
  [connection-id generation]
  (remove-flights!
   (fn [[[[cached-id cached-generation _] _] _]]
     (and (= connection-id cached-id)
          (= generation cached-generation)))
   (ex-info "Query cache scope closed."
            {:type :datahike/query-cache-scope-closed})))

(defn clear!
  "Fail and remove every process-local in-flight computation."
  []
  (remove-flights! (constantly true)
                   (ex-info "Query coordinator cleared."
                            {:type :datahike/query-coordinator-cleared})))

(defn metrics
  "Return non-retaining single-flight counters and current gauges."
  []
  (let [current (:flights @coordinator)
        totals (counter-values)]
    (merge totals
           {:active-flights (count current)
            :active-callers (reduce + 0 (map (comp count :waiters val) current))
            :active-waiters (reduce + 0
                                    (map (fn [[_ {:keys [waiters
                                                         owner-request-id]}]]
                                           (count (dissoc waiters
                                                          owner-request-id)))
                                         current))
            :saved-computations (:waiter-hits totals)
            :active-by-database
            (reduce-kv
             (fn [result [database-key _]
                  {:keys [waiters owner-request-id]}]
               (-> result
                   (update-in [database-key :flights] (fnil inc 0))
                   (update-in [database-key :callers] (fnil + 0)
                              (count waiters))
                   (update-in [database-key :waiters] (fnil + 0)
                              (count (dissoc waiters owner-request-id)))))
             {} current)})))
