(ns datahike.test.as-of-resolution-test
  "as-of agrees with the current indexes for a value asserted and retracted in one
  transaction (upstream cbdbd242, #950, adapted to current-datoms-xform). Each read
  is made three ways: an ascending datoms read, a descending rseek-datoms read and a
  query, since the xform folds a run of one [e a v] in either order."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.test.utils :as utils]))

(defn- with-conn [f]
  (let [conn (utils/setup-db {:store {:backend :memory :id (random-uuid)}
                              :keep-history? true :schema-flexibility :write})]
    (try
      (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                        {:db/ident :tag :db/valueType :db.type/keyword
                         :db/cardinality :db.cardinality/many}
                        {:db/ident :score :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:db/id -1 :name "a"}])
      (f conn (:e (first (d/datoms @conn :avet :name "a"))))
      (finally (d/release conn)))))

(defn- present? [db e a v]
  (let [asc (boolean (seq (d/datoms db :eavt e a v)))
        desc (boolean (some #(and (= e (:e %)) (= a (:a %)) (= v (:v %)))
                            (take 20 (d/rseek-datoms db :eavt e a v))))
        q (boolean (seq (d/q '[:find ?e :in $ ?e ?a ?v :where [?e ?a ?v]] db e a v)))]
    (is (= asc desc q) (str "ascending, descending and query agree on " [e a v]))
    asc))

(defn- tx-of [conn ops] (:max-tx (:db-after (d/transact conn ops))))

(deftest a-value-asserted-and-retracted-in-one-transaction-is-absent
  (doseq [[a v] [[:tag :x] [:score 1]]]
    (testing (str a)
      (with-conn
        (fn [conn e]
          (d/transact conn [[:db/add e a v] [:db/retract e a v]])
          (let [db @conn]
            (is (false? (present? db e a v)) "current")
            (is (false? (present? (d/as-of db (:max-tx db)) e a v)) "as-of")))))))

(deftest the-opposite-order-keeps-the-re-assertion
  (with-conn
    (fn [conn e]
      (d/transact conn [[:db/add e :tag :x]])
      (d/transact conn [[:db/retract e :tag :x] [:db/add e :tag :x]])
      (let [db @conn]
        (is (true? (present? db e :tag :x)) "current")
        (is (true? (present? (d/as-of db (:max-tx db)) e :tag :x)) "as-of")))))

(deftest a-re-assertion-retracted-later-is-held-in-between
  (with-conn
    (fn [conn e]
      (d/transact conn [[:db/add e :tag :x]])
      (let [t1 (tx-of conn [[:db/retract e :tag :x] [:db/add e :tag :x]])]
        (d/transact conn [[:db/retract e :tag :x]])
        (let [db @conn]
          (is (false? (present? db e :tag :x)) "current")
          (is (true? (present? (d/as-of db t1) e :tag :x)) "as-of the re-assertion"))))))

(deftest a-retraction-of-one-value-does-not-suppress-another
  (with-conn
    (fn [conn e]
      (d/transact conn [[:db/add e :score 1]])
      (d/transact conn [[:db/retract e :score 1] [:db/add e :score 2]])
      (let [db @conn as-of (d/as-of db (:max-tx db))]
        (is (true? (present? db e :score 2)))
        (is (true? (present? as-of e :score 2)))
        (is (false? (present? as-of e :score 1)))))))

(deftest history-is-readable-at-every-point
  (with-conn
    (fn [conn e]
      (let [t1 (tx-of conn [[:db/add e :tag :x]])
            t2 (tx-of conn [[:db/retract e :tag :x]])
            t3 (tx-of conn [[:db/add e :tag :x]])
            t4 (tx-of conn [[:db/add e :tag :y] [:db/retract e :tag :y]])
            db @conn]
        (is (true? (present? (d/as-of db t1) e :tag :x)))
        (is (false? (present? (d/as-of db t2) e :tag :x)))
        (is (true? (present? (d/as-of db t3) e :tag :x)))
        (is (false? (present? (d/as-of db t4) e :tag :y)))
        (is (true? (present? db e :tag :x)))))))

(deftest churn-after-a-prior-life
  (with-conn
    (fn [conn e]
      (d/transact conn [[:db/add e :tag :x]])
      (d/transact conn [[:db/retract e :tag :x]])
      (d/transact conn [[:db/add e :tag :x] [:db/retract e :tag :x]])
      (let [db @conn]
        (is (false? (present? db e :tag :x)) "current")
        (is (false? (present? (d/as-of db (:max-tx db)) e :tag :x)) "as-of")))))

(deftest a-card-one-overwrite-chain-reads-back
  (with-conn
    (fn [conn e]
      (let [[t1 t2 t3] (mapv #(tx-of conn [[:db/add e :score %]]) [1 2 3])
            db @conn]
        (is (true? (present? (d/as-of db t1) e :score 1)))
        (is (true? (present? (d/as-of db t2) e :score 2)))
        (is (false? (present? (d/as-of db t2) e :score 1)))
        (is (true? (present? (d/as-of db t3) e :score 3)))))))

(deftest a-value-asserted-retracted-and-asserted-in-one-transaction-is-held
  (with-conn
    (fn [conn e]
      (d/transact conn [[:db/add e :tag :x] [:db/retract e :tag :x] [:db/add e :tag :x]])
      (let [db @conn]
        (is (= (present? db e :tag :x) (present? (d/as-of db (:max-tx db)) e :tag :x))
            "as-of agrees with the current index")))))
