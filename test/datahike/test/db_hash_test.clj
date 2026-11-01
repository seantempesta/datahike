(ns datahike.test.db-hash-test
  "Without history, :hash is the sum over the current index (upstream 7c849d15, #940):
  a card-one overwrite left the superseded value counted, a value that survives nowhere."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.test.utils :as utils]))

(def ^:private schema
  [{:db/ident :name :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :score :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :tag :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/many}
   {:db/ident :note :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/noHistory true}])

(defn- sum-over-current [db]
  (reduce #(+ %1 (hash %2)) 0 (d/datoms db :eavt)))

(defn- with-conn [keep-history? f]
  (let [conn (utils/setup-db {:store {:backend :memory :id (random-uuid)}
                              :keep-history? keep-history? :schema-flexibility :write})]
    (try (d/transact conn schema) (f conn)
         (finally (d/release conn)))))

(defn- hash-after [keep-history? txs]
  (with-conn keep-history?
    (fn [conn]
      (doseq [tx txs] (d/transact conn tx))
      [(:hash @conn) (sum-over-current @conn)])))

;; ---------------------------------------------------------------------------

(deftest without-history-the-sum-is-over-the-current-index
  (testing "nothing survives a retraction or an overwrite, so nothing may stay
            counted. Every shape, because the failing one was a single branch."
    (doseq [[label txs]
            [["plain assertions"       [[{:db/id -1 :name "a" :score 1 :tag :x}]]]
             ["card-one overwrite"     [[{:db/id -1 :name "a" :score 1}]
                                        [{:db/id [:name "a"] :score 2}]]]
             ["repeated overwrite"     [[{:db/id -1 :name "a" :score 1}]
                                        [{:db/id [:name "a"] :score 2}]
                                        [{:db/id [:name "a"] :score 3}]]]
             ["overwrite, same value"  [[{:db/id -1 :name "a" :score 1}]
                                        [{:db/id [:name "a"] :score 1}]]]
             ["revert to an old value" [[{:db/id -1 :name "a" :score 1}]
                                        [{:db/id [:name "a"] :score 2}]
                                        [{:db/id [:name "a"] :score 1}]]]
             ["explicit retraction"    [[{:db/id -1 :name "a" :tag :x}]
                                        [[:db/retract [:name "a"] :tag :x]]]]
             ["retract then re-assert" [[{:db/id -1 :name "a" :tag :x}]
                                        [[:db/retract [:name "a"] :tag :x]]
                                        [{:db/id [:name "a"] :tag :x}]]]
             ["retractEntity"          [[{:db/id -1 :name "a" :score 1 :tag :x}]
                                        [[:db/retractEntity [:name "a"]]]]]
             [":db/noHistory overwrite" [[{:db/id -1 :name "a" :note "first"}]
                                         [{:db/id [:name "a"] :note "second"}]]]
             ["unique-identity upsert" [[{:db/id -1 :name "a" :score 1}]
                                        [{:name "a" :score 2}]]]
             ["schema attribute change" [[{:db/id -1 :name "a"}]
                                         [{:db/ident :score :db/doc "x"}]
                                         [{:db/ident :score :db/doc "y"}]]]]]
      (let [[h s] (hash-after false txs)]
        (is (= s h) (str label ": :hash must equal the sum over :eavt"))))))


(deftest with-history-a-same-value-overwrite-leaves-the-hash
  (testing "an equal value replaces the datom in place, so :hash moves by the
            transaction's own :db/txInstant datom and nothing else"
    (with-conn true
      (fn [conn]
        (d/transact conn [{:db/id -1 :name "a" :score 1}])
        (let [{:keys [db-before db-after tx-data]} (d/transact conn [{:db/id [:name "a"] :score 1}])
              tx-instant (reduce + 0 (map hash (filter #(= :db/txInstant (:a %)) tx-data)))]
          (is (= tx-instant (- (:hash db-after) (:hash db-before)))))))))
