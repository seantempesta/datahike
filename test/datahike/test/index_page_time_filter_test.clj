(ns datahike.test.index-page-time-filter-test
  "Index pages on time-filtered views (as-of, since, history of as-of) equal
   the pages of the open seek over the same view, and read only the prefix."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.datom :as dd]
            [datahike.db :as db]
            [datahike.db.interface :as dbi]))

(def ^:private schema
  [{:db/ident :item/rank
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :zz/tail
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/index true}])

(def ^:private tail-size 400)

(defn- with-history
  "Calls f with the database value and the as-of point: ranks asserted,
   changed and retracted before the point, and again after it; a tail
   attribute sorting after the prefix in every index."
  [f]
  (let [configuration {:store {:backend :memory :id (random-uuid)}
                       :schema-flexibility :write
                       :keep-history? true
                       :initial-tx schema}]
    (d/create-database configuration)
    (let [connection (d/connect configuration)]
      (try
        (let [ranks (mapv (fn [n] {:db/id (- n) :item/rank (* 10 n)})
                          (range 1 10))
              ids (vals (:tempids (d/transact connection {:tx-data ranks})))
              ids (vec (sort (filter pos-int? ids)))
              _ (d/transact connection
                            {:tx-data (concat
                                       [[:db/add (ids 0) :item/rank 55]
                                        [:db/add (ids 3) :item/rank 5]
                                        [:db/retract (ids 5) :item/rank 60]]
                                       (map (fn [n] {:zz/tail n})
                                            (range tail-size)))})
              point (:max-tx @connection)]
          (d/transact connection
                      {:tx-data [[:db/add (ids 1) :item/rank 1]
                                 [:db/retract (ids 2) :item/rank 30]
                                 [:db/add (ids 5) :item/rank 61]]})
          (f @connection point))
        (finally
          (d/release connection)
          (d/delete-database configuration))))))

(defn- views [current point]
  {:as-of (d/as-of current point)
   :since (d/since current point)
   :history-as-of (db/->HistoricalDB (d/as-of current point))})

(defn- datom-data [datom]
  [(:e datom) (:a datom) (:v datom) (dd/datom-tx datom)
   (dd/datom-added datom)])

(defn- index-comparator [view index direction]
  (let [compare-datoms (dd/index-type->cmp-quick
                        index
                        (not (dbi/context-temporal? (dbi/-search-context view))))]
    (if (= :forward direction)
      compare-datoms
      (fn [left right] (compare-datoms right left)))))

(defn- seek-pages
  "The pages the open seek over the view gives: every datom from the
   prefix on, in native order, cut at the prefix and split by limit."
  [view index attribute direction limit]
  (let [datoms (->> (d/seek-datoms view {:index index :components [attribute]})
                    (sort (index-comparator view index direction))
                    (filter #(= attribute (:a %))))
        chunks (or (seq (partition-all limit datoms)) [[]])]
    (map-indexed (fn [n datoms]
                   (let [last? (= n (dec (count chunks)))]
                     (cond-> {:datoms (mapv datom-data datoms)
                              :complete? last?}
                       (not last?) (assoc :cursor (datom-data (last datoms))))))
                 chunks)))

(defn- index-pages [view index attribute direction limit]
  (loop [cursor nil
         pages []]
    (let [page (d/index-page view (cond-> {:index index
                                           :components [attribute]
                                           :direction direction
                                           :limit limit}
                                    cursor (assoc :cursor cursor)))
          pages (conj pages
                      (cond-> {:datoms (mapv datom-data
                                             (:datahike.index-page/datoms page))
                               :complete? (:datahike.index-page/complete? page)}
                        (:datahike.index-page/cursor page)
                        (assoc :cursor (:datahike.index-page/cursor page))))]
      (if (:datahike.index-page/complete? page)
        pages
        (recur (:datahike.index-page/cursor page) pages)))))

(deftest time-filtered-pages-equal-the-open-seeks-pages
  (with-history
    (fn [current point]
      (doseq [[view-name view] (views current point)
              index [:avet :aevt]
              direction [:forward :reverse]
              limit [1 3 200]]
        (testing (str view-name " " index " " direction " limit " limit)
          (let [expected (seek-pages view index :item/rank direction limit)]
            (is (seq (:datoms (first expected))))
            (is (= expected
                   (index-pages view index :item/rank direction limit)))))))))

(defn- realized-by-time-filter
  "Counts the datoms the time filter receives while f runs."
  [f]
  (let [realized (atom 0)
        post-process db/post-process-datoms]
    (with-redefs [db/post-process-datoms
                  (fn [datoms db context]
                    (let [datoms (vec datoms)]
                      (swap! realized + (count datoms))
                      (post-process datoms db context)))]
      (f))
    @realized))

(deftest time-filtered-page-reads-only-its-prefix
  (with-history
    (fn [current point]
      ;; The time filter reads the prefix's datoms and one transaction
      ;; instant per transaction among them.
      (let [prefix (d/datoms (d/history current)
                             {:index :avet :components [:item/rank]})
            bound (+ (count prefix) (count (set (map dd/datom-tx prefix))))]
        (doseq [[view-name view] (views current point)
                direction [:forward :reverse]]
          (testing (str view-name " " direction)
            (let [first-page (d/index-page view {:index :avet
                                                 :components [:item/rank]
                                                 :direction direction
                                                 :limit 3})
                  cursor (:datahike.index-page/cursor first-page)]
              (is (<= (realized-by-time-filter
                       #(d/index-page view {:index :avet
                                            :components [:item/rank]
                                            :direction direction
                                            :limit 3
                                            :cursor cursor}))
                      bound)
                  "a page realizes at most the prefix, never the tail"))))))))
