(ns ^:no-doc datahike.resource)

(def ^:dynamic *budget*
  "The resource budget active for one synchronous database operation."
  nil)

(def ^:dynamic *evidence-sink*
  "Optional volatile receiving bounded evidence for one operation."
  nil)

(declare scalar-weight)

(defn budget-exceeded!
  "Raises a structured resource-budget error."
  [budget-name observed allowed]
  (throw (ex-info (str "datahike " (name budget-name) " budget exceeded")
                  {:datahike/budget-exceeded true
                   :datahike.budget/name budget-name
                   :datahike.budget/observed observed
                   :datahike.budget/allowed allowed})))

(defn make-budget
  "Creates mutable counters for one synchronous database operation."
  [{:keys [max-work max-results max-result-weight evidence?]}]
  (when (or evidence? max-work max-results max-result-weight)
    {:max-work max-work
     :max-results max-results
     :max-result-weight max-result-weight
     :work (when (or evidence? max-work) (volatile! 0))
     :results (when (or evidence? max-results) (volatile! 0))
     :result-weight (when (or evidence? max-result-weight) (volatile! 0))}))

(defn budget-evidence
  "Returns bounded ordinary data for one operation's resource counters."
  [budget]
  {:datahike.resource/work (if-let [counter (:work budget)] @counter 0)
   :datahike.resource/result-count
   (if-let [counter (:results budget)] @counter 0)
   :datahike.resource/result-weight
   (if-let [counter (:result-weight budget)] @counter 0)
   :datahike.resource/limits
   (cond-> {}
     (:max-work budget) (assoc :datahike.resource/max-work (:max-work budget))
     (:max-results budget) (assoc :datahike.resource/max-results
                                  (:max-results budget))
     (:max-result-weight budget)
     (assoc :datahike.resource/max-result-weight
            (:max-result-weight budget)))})

(defn publish-evidence!
  "Publishes one bounded resource snapshot when an evidence owner is bound."
  [budget]
  (when *evidence-sink*
    (vreset! *evidence-sink* (budget-evidence budget))))

(defn- charge!
  [counter allowed budget-name amount]
  (when counter
    (let [observed (+ @counter amount)]
      (vreset! counter observed)
      (when (and allowed (> observed allowed))
        (budget-exceeded! budget-name observed allowed)))))

(defn charge-work!
  "Charges synchronous execution work against the active budget."
  ([] (charge-work! 1))
  ([amount]
   (when-let [budget *budget*]
     (charge! (:work budget) (:max-work budget) :query-work amount))))

(defn charge-result-weight!
  "Charges retained result weight against the active budget."
  [amount]
  (when-let [budget *budget*]
    (charge! (:result-weight budget) (:max-result-weight budget)
             :result-weight amount)))

(defn charge-result!
  "Charges one result and its immediate scalar slots before retention."
  [value]
  (when-let [budget *budget*]
    (charge! (:results budget) (:max-results budget) :query-results 1)
    (charge-result-weight!
     (if (sequential? value)
       (reduce (fn [weight item] (+ weight (scalar-weight item))) 1 value)
       (scalar-weight value))))
  value)

(defn charge-result-node!
  "Charges one result node before it is retained."
  []
  (when-let [budget *budget*]
    (charge! (:results budget) (:max-results budget) :query-results 1)))

(defn scalar-weight
  "Returns the O(1) shallow weight of a scalar value."
  [value]
  (cond
    (nil? value) 0
    (string? value) (inc (count value))
    #?(:clj (and value (.isArray ^Class (class value))) :cljs (array? value))
    (inc (count value))
    :else 1))

(defn charge-value!
  "Charges shallow scalar weight before a value is retained."
  [value]
  (charge-result-weight! (scalar-weight value))
  value)

(defn- pop-pending
  [stack]
  (loop [stack stack]
    (if-let [items (seq (first stack))]
      [true (first items) (cons (next items) (next stack))]
      (if (seq stack)
        (recur (next stack))
        [false nil nil]))))

(defn shallow-weight-within
  "Returns a bounded non-serializing weight, or nil when uncertifiable."
  [value limit]
  (when (and limit (not (neg? limit)))
    (loop [present? true value value stack nil weight 0]
      (if-not present?
        weight
        (let [scalar? (or (nil? value)
                          (string? value)
                          #?(:clj (and value (.isArray ^Class (class value)))
                             :cljs (array? value))
                          (not (coll? value)))]
          (if scalar?
            (let [weight (+ weight (scalar-weight value))]
              (when (<= weight limit)
                (let [[present? value stack] (pop-pending stack)]
                  (recur present? value stack weight))))
            ;; Cache admission never realizes an uncounted/lazy collection.
            (when (counted? value)
              (let [entry-count (count value)
                    lower-bound (+ 1 (if (map? value)
                                       (* 2 entry-count)
                                       entry-count))]
                (when (<= (+ weight lower-bound) limit)
                  (let [items (if (map? value) (mapcat identity value) (seq value))
                        [present? value stack] (pop-pending (cons items stack))]
                    (recur present? value stack (inc weight))))))))))))

(defn result-count
  "Returns the O(1) top-level cardinality of a query result."
  [result]
  (cond
    (nil? result) 0
    (counted? result) (count result)
    #?@(:clj [(instance? java.util.Collection result) (count result)])
    :else 1))

(defn certify-result!
  "Checks result cardinality and shallow weight without serializing it."
  [result {:keys [max-results max-result-weight]}]
  (when max-results
    (let [observed (result-count result)]
      (when (> observed max-results)
        (budget-exceeded! :query-results observed max-results))))
  (when max-result-weight
    (when-not (shallow-weight-within result max-result-weight)
      (budget-exceeded! :result-weight (inc max-result-weight)
                        max-result-weight)))
  result)

(defn work-signal
  "Returns an IDeref that charges work and delegates cancellation."
  [cancel budget]
  (if-not budget
    cancel
    #?(:clj
       (reify clojure.lang.IDeref
         (deref [_]
           (binding [*budget* budget] (charge-work!))
           (boolean (and cancel @cancel))))
       :cljs
       (reify IDeref
         (-deref [_]
           (binding [*budget* budget] (charge-work!))
           (boolean (and cancel @cancel)))))))
