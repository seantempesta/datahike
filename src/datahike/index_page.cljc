(ns ^:no-doc datahike.index-page
  (:require [datahike.constants :as const]
            [datahike.datom :as dd]
            [datahike.db.interface :as dbi]
            [datahike.db.utils :as dbu]
            [datahike.resource :as resource]))

(defn- request-error [message reason data]
  (throw (ex-info message
                  (merge {:type :datahike.index-page/invalid-request
                          :datahike.index-page/reason reason}
                         data))))

(defn- validate-options!
  [{:keys [index components direction limit cursor max-result-weight]}]
  (when-not (#{:eavt :aevt :avet} index)
    (request-error "Invalid index-page index." :index {:index index}))
  (when-not (and (vector? components) (<= 0 (count components) 4))
    (request-error "Index-page components must be a vector of 0 to 4 values."
                   :components {:components components}))
  (when-not (#{:forward :reverse} direction)
    (request-error "Invalid index-page direction."
                   :direction {:direction direction}))
  (when-not (and (int? limit) (<= 1 limit 200))
    (request-error "Index-page limit must be between 1 and 200."
                   :limit {:limit limit}))
  (when (and cursor
             (not (and (vector? cursor)
                       (= 5 (count cursor))
                       (pos-int? (nth cursor 3))
                       (boolean? (nth cursor 4)))))
    (request-error "Index-page cursor must contain e, a, v, tx, and added?."
                   :cursor {:cursor cursor}))
  (when (and max-result-weight (not (pos-int? max-result-weight)))
    (request-error "Index-page max-result-weight must be positive."
                   :max-result-weight
                   {:max-result-weight max-result-weight})))

(defn- temporal-index? [db]
  (dbi/context-temporal? (dbi/-search-context db)))

(defn- index-comparator [db index]
  (dd/index-type->cmp-quick index (not (temporal-index? db))))

(defn- restore-temporal-index-order
  "Restores native order after an as-of or since reconstruction."
  [db comparator direction candidates]
  (if (dbi/context-time-pred (dbi/-search-context db))
    (sort (if (= :forward direction)
            comparator
            (fn [left right] (comparator right left)))
          candidates)
    candidates))

(defn- with-added [datom added?]
  (dd/datom (:e datom) (:a datom) (:v datom) (dd/datom-tx datom) added?))

(def ^:private index-fields
  {:eavt [:e :a :v :tx]
   :aevt [:a :e :v :tx]
   :avet [:a :v :e :tx]})

(defn- field-value [datom field]
  (case field
    :e (:e datom)
    :a (:a datom)
    :v (:v datom)
    :tx (dd/datom-tx datom)))

(defn- equal-field? [field left right]
  (zero? ((case field
            :e dd/long-cmp
            :a dd/cmp-attr-quick
            :v dd/compare-value
            :tx dd/long-cmp)
          left right)))

(defn- resolved-prefix [db index components]
  (let [pattern (dbu/components->pattern db index components
                                          const/e0 const/tx0)]
    (mapv #(field-value pattern %)
          (take (count components) (index-fields index)))))

(defn- within-prefix? [index prefix datom]
  (every? true?
          (map (fn [field expected]
                 (equal-field? field expected (field-value datom field)))
               (index-fields index)
               prefix)))

(defn- resolve-cursor [db [e a v tx added?]]
  (with-added (dbu/resolve-datom db e a v tx const/e0 const/tx0) added?))

(defn- cursor-components [index datom]
  (let [e (:e datom)
        a (:a datom)
        v (:v datom)
        tx (dd/datom-tx datom)
        added? (dd/datom-added datom)]
    (case index
      :eavt [e a v tx added?]
      :aevt [a e v tx added?]
      :avet [a v e tx added?])))

(defn- prefix-seek-components [components direction]
  (into components
        (concat (repeat (- 4 (count components)) nil)
                [(= :reverse direction)])))

(defn- cursor-data [datom]
  [(:e datom) (:a datom) (:v datom)
   (dd/datom-tx datom) (dd/datom-added datom)])

(defn index-page
  "Returns one eager bounded page in native Datahike index order."
  [db {:keys [index components direction limit cursor max-result-weight]
       :as options}]
  (validate-options! options)
  (let [comparator (index-comparator db index)
        prefix (resolved-prefix db index components)
        resolved-cursor (when cursor (resolve-cursor db cursor))
        _ (when (and resolved-cursor
                     (not (within-prefix? index prefix resolved-cursor)))
            (request-error "Index-page cursor is outside the requested prefix."
                           :cursor-prefix {:cursor cursor
                                           :components components}))
        seek-components (if resolved-cursor
                          (cursor-components index resolved-cursor)
                          (prefix-seek-components components direction))
        candidates ((if (= :forward direction)
                      dbi/seek-datoms
                      dbi/rseek-datoms)
                    db index seek-components)
        ;; Time-filtered temporal reads reconstruct cardinality-one state by
        ;; entity/attribute. That grouping is eager and does not retain AVET
        ;; (or another requested index's) order. Restore dependency-native
        ;; comparator order before a foreign datom can terminate this prefix.
        candidates (restore-temporal-index-order db comparator direction
                                                  candidates)
        candidates (take-while #(within-prefix? index prefix %) candidates)
        candidates
        (if resolved-cursor
          (let [candidate (first candidates)]
            (when-not (and candidate
                           (zero? (comparator resolved-cursor candidate))
                           (= (dd/datom-added resolved-cursor)
                              (dd/datom-added candidate)))
              (request-error "Index-page cursor is absent from this index view."
                             :cursor-absent {:cursor cursor}))
            (rest candidates))
          candidates)
        realized (vec (take (inc limit) candidates))
        complete? (<= (count realized) limit)
        datoms (if complete? realized (subvec realized 0 limit))
        result (cond-> {:datahike.index-page/datoms datoms
                        :datahike.index-page/complete? complete?}
                 (not complete?)
                 (assoc :datahike.index-page/cursor
                        (cursor-data (peek datoms))))]
    (resource/certify-result! result
                              {:max-result-weight max-result-weight})))
