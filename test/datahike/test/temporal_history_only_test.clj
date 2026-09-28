(ns datahike.test.temporal-history-only-test
  "The temporal indexes hold only datoms that left the current index: a
   card-one write copies nothing there until it replaces or retracts a value.
   History, as-of and since read the same datoms as when every assertion was
   copied, including over a store whose temporal index still holds copies."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.index.interface :as di]))

(def ^:private schema
  [{:db/ident :t/id :db/valueType :db.type/string :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}
   {:db/ident :t/n :db/valueType :db.type/long :db/index true :db/cardinality :db.cardinality/one}
   {:db/ident :t/tags :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}])

(defn- temporal [db]
  (into {} (for [k [:temporal-eavt :temporal-aevt :temporal-avet]]
             [k (mapv (juxt :e :a :v :added) (di/-all (get db k)))])))

(defn- history [db e]
  (set (map (juxt :a :v :added) (d/datoms (d/history db) :eavt e))))

(deftest a-card-one-write-copies-only-what-leaves-the-current-index
  (let [cfg {:store {:backend :memory :id (random-uuid)} :keep-history? true :schema-flexibility :write}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      (d/transact conn schema)
      (let [t1 (:max-tx (:db-after (d/transact conn [{:t/id "a" :t/n 1 :t/tags :x}])))
            e (:db/id (d/entity @conn [:t/id "a"]))]
        (testing "a fresh assertion adds nothing to the temporal indexes"
          (is (every? empty? (vals (temporal @conn)))))
        (let [db2 (:db-after (d/transact conn [{:t/id "a" :t/n 2}]))
              t2 (:max-tx db2)]
          (testing "a replaced value moves in: its assertion and its retraction"
            (is (= {:temporal-eavt [[e :t/n 1 true] [e :t/n 1 false]]
                    :temporal-aevt [[e :t/n 1 true] [e :t/n 1 false]]
                    :temporal-avet [[e :t/n 1 true] [e :t/n 1 false]]}
                   (temporal @conn))))
          (d/transact conn [[:db/retract e :t/n 2] [:db/retract e :t/tags :x]])
          (let [db @conn]
            (testing "history holds every assertion and retraction exactly once"
              (is (= #{[:t/id "a" true] [:t/n 1 true] [:t/n 1 false] [:t/n 2 true] [:t/n 2 false]
                       [:t/tags :x true] [:t/tags :x false]}
                     (history db e))))
            (testing "as-of and since read each point in time"
              (is (= 1 (:t/n (d/entity (d/as-of db t1) e))))
              (is (= 2 (:t/n (d/entity (d/as-of db t2) e))))
              (is (nil? (:t/n (d/entity db e))))
              (is (= #{[:t/n 2]} (set (map (juxt :a :v) (d/datoms (d/since db2 t1) :eavt e))))))
            (testing "an index range over history reads the current and the moved values"
              (is (= [1 1 2 2] (mapv :v (d/index-range (d/history db) {:attrid :t/n :start 0 :end 10}))))))))
      (finally (d/release conn) (d/delete-database cfg)))))

(deftest a-temporal-index-that-still-copies-current-datoms-reads-the-same
  (let [cfg {:store {:backend :memory :id (random-uuid)} :keep-history? true :schema-flexibility :write}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      (d/transact conn schema)
      (d/transact conn [{:t/id "a" :t/n 1}])
      (d/transact conn [{:t/id "a" :t/n 2}])
      (let [db @conn
            e (:db/id (d/entity db [:t/id "a"]))
            ;; what a store written before the rule holds: every current
            ;; assertion copied into each temporal index as well
            copied (reduce (fn [db k]
                             (let [index-type (keyword (subs (name k) (count "temporal-")))
                                   current (get db index-type)]
                               (update db k #(reduce (fn [t datom] (di/-temporal-insert t datom index-type 0))
                                                     % (di/-all current)))))
                           db [:temporal-eavt :temporal-aevt :temporal-avet])]
        (is (< (count (di/-all (:temporal-eavt db))) (count (di/-all (:temporal-eavt copied)))))
        (is (= (history db e) (history copied e)))
        (is (= (d/q '[:find ?n ?tx ?added :in $ ?e :where [?e :t/n ?n ?tx ?added]] (d/history db) e)
               (d/q '[:find ?n ?tx ?added :in $ ?e :where [?e :t/n ?n ?tx ?added]] (d/history copied) e)))
        (is (= (vec (d/datoms (d/history db) :aevt :t/n))
               (vec (d/datoms (d/history copied) :aevt :t/n)))))
      (finally (d/release conn) (d/delete-database cfg)))))
