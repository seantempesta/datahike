(ns datahike.test.data-vector-retract-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]))

(defn- with-conn [f]
  (let [cfg {:store {:backend :memory :id (random-uuid)} :keep-history? true :schema-flexibility :write}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                          {:db/ident :t/failures :db/valueType :db.type/data :db/cardinality :db.cardinality/one}])
        (f conn)
        (finally (d/release conn) (d/delete-database cfg))))))

(deftest a-data-attributes-vector-value-retracts
  ;; A :db.type/data value is whole plain data, a vector included; search's pattern check let a
  ;; vector through only as a lookup ref or a tuple's value, so retracting one threw.
  (with-conn
    (fn [conn]
      (let [v [{:type :fail}]
            e (-> (d/transact conn [{:db/id -1 :name "x" :t/failures v}]) :tempids (get -1))]
        (is (= v (:t/failures (d/pull @conn [:t/failures] e))))
        (is (= [[e v]] (vec (d/q '[:find ?e ?v :where [?e :t/failures ?v]] @conn))))
        (is (= 1 (count (d/datoms @conn :eavt e :t/failures v))) "the exact vector finds its datom")
        (d/transact conn [[:db/retract e :t/failures v]])
        (is (nil? (:t/failures (d/pull @conn [:t/failures] e))) "[:db/retract e a v] removed it")
        (is (= [true false] (mapv :added (sort-by :tx (d/datoms (d/history @conn) :eavt e :t/failures v))))
            "history keeps the add and the retract of the exact vector")))))

(deftest a-data-attributes-vector-value-retracts-by-attribute
  (with-conn
    (fn [conn]
      (let [e (-> (d/transact conn [{:db/id -1 :name "x" :t/failures [{:type :fail} 2 3]}]) :tempids (get -1))]
        (d/transact conn [[:db.fn/retractAttribute e :t/failures]])
        (is (nil? (:t/failures (d/pull @conn [:t/failures] e))))))))

(deftest a-data-attributes-map-value-still-retracts
  (with-conn
    (fn [conn]
      (let [v {:type :fail}
            e (-> (d/transact conn [{:db/id -1 :name "x" :t/failures v}]) :tempids (get -1))]
        (d/transact conn [[:db/retract e :t/failures v]])
        (is (nil? (:t/failures (d/pull @conn [:t/failures] e))))))))
