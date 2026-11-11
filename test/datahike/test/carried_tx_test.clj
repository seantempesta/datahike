(ns datahike.test.carried-tx-test
  "A datom in tx-data that carries its own transaction (a Datom, or an op's fifth
  element, as a merge carries a branch's datoms) names a transaction id already
  issued: one in [tx0, txmax], and no later than the transaction writing it."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.constants :refer [tx0 txmax]]
            [datahike.datom :as dd]))

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true
             :initial-tx [{:db/ident :n :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}]}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- refusal [f]
  (try (f) nil
       (catch Exception e (some (comp :error ex-data) (take-while some? (iterate ex-cause e))))))

(deftest a-carried-tx-is-an-issued-transaction
  (let [conn (fresh-conn)
        r (d/transact conn [{:db/id "e" :n 1}])
        e (get-in r [:tempids "e"])
        t (get-in r [:tempids :db/current-tx])]
    (testing "a carried tx not yet issued is refused"
      (is (= :tx-id/range (refusal #(d/transact conn [(dd/datom e :n 2 (- txmax 1) true)]))))
      (is (= :tx-id/range (refusal #(d/transact conn [[:db/add e :n 2 (- txmax 1)]])))
          "an op's fifth element is a carried tx too")
      (is (= :tx-id/range (refusal #(d/transact conn [(dd/datom e :n 1 (- txmax 1) false)])))
          "a carried retraction"))
    (testing "a carried tx outside the transaction ids is refused"
      (is (= :tx-id/range (refusal #(d/transact conn [(dd/datom e :n 2 e true)]))) "an entity id")
      (is (= :tx-id/range (refusal #(d/transact conn [(dd/datom e :n 2 (inc txmax) true)]))) "above txmax")
      (is (= :tx-id/range (refusal #(d/with @conn [(dd/datom e :n 2 (dec tx0) true)])))
          "a speculation holds the band too"))
    (testing "nothing refused was written"
      (is (= [[e :n 1 t]] (mapv (juxt :e :a :v :tx) (d/datoms @conn :eavt e :n)))))
    (testing "an issued tx is kept as it is"
      (d/transact conn [(dd/datom e :n 1 t false) (dd/datom e :n 3 t true)])
      (is (= [[e :n 3 t]] (mapv (juxt :e :a :v :tx) (d/datoms @conn :eavt e :n)))))
    (testing "a speculation takes no id from the store, so a tx in the band is not bounded by its own"
      (let [later (+ (:max-tx @conn) 100)
            r (d/with @conn [(dd/datom e :n 4 later true)])]
        (is (some #(= [e :n 4 later] ((juxt :e :a :v :tx) %)) (:tx-data r)))))))
