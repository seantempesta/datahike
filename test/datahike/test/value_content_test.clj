(ns datahike.test.value-content-test
  "A transaction decides whether a value is the one held by its content, as the
  index orders it: two byte arrays of one content are one value, though `=` on
  them is false."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]))

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true
             :initial-tx [{:db/ident :one :db/valueType :db.type/bytes
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :many :db/valueType :db.type/bytes
                           :db/cardinality :db.cardinality/many}
                          {:db/ident :pair :db/valueType :db.type/tuple
                           :db/tupleTypes [:db.type/keyword :db.type/bytes]
                           :db/cardinality :db.cardinality/one}]}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- bytes-of [& xs] (byte-array xs))

(defn- data-datoms
  "The datoms of a report's tx-data that are not its transaction's own facts."
  [report]
  (remove #(= (:e %) (:max-tx (:db-after report))) (:tx-data report)))

(defn- history-count [conn e a]
  (count (d/datoms (d/history @conn) :eavt e a)))

(deftest equal-bytes-are-the-value-held
  (let [conn (fresh-conn)
        ;; a card-many value in an entity map is read as its values: bytes go by :db/add
        e (get-in (d/transact conn [{:db/id "e" :one (bytes-of 1 2) :pair [:k (bytes-of 4)]}
                                    [:db/add "e" :many (bytes-of 3)]])
                  [:tempids "e"])]
    (testing "re-asserting the bytes a card-one attribute holds changes nothing"
      (is (empty? (data-datoms (d/transact conn [[:db/add e :one (bytes-of 1 2)]]))))
      (is (= 1 (history-count conn e :one)) "no retraction and re-assertion in history"))
    (testing "re-asserting bytes a card-many attribute holds changes nothing"
      (is (empty? (data-datoms (d/transact conn [[:db/add e :many (bytes-of 3)]]))))
      (is (= 1 (history-count conn e :many))))
    (testing "re-asserting a tuple holding the same bytes changes nothing"
      (is (empty? (data-datoms (d/transact conn [[:db/add e :pair [:k (bytes-of 4)]]]))))
      (is (= 1 (history-count conn e :pair))))
    (testing "compare-and-swap expecting equal bytes succeeds"
      (d/transact conn [[:db/cas e :one (bytes-of 1 2) (bytes-of 5)]])
      (is (= [5] (vec (:one (d/pull @conn [:one] e)))))
      (d/transact conn [[:db/cas e :many (bytes-of 3) (bytes-of 6)]])
      (is (some #(= [6] (vec %)) (:many (d/pull @conn [:many] e)))))
    (testing "retracting by equal bytes retracts"
      (d/transact conn [[:db/retract e :one (bytes-of 5)]])
      (is (empty? (d/datoms @conn :eavt e :one))))
    (testing "different bytes still change the value"
      (d/transact conn [[:db/add e :one (bytes-of 7)]])
      (let [r (d/transact conn [[:db/add e :one (bytes-of 8)]])]
        (is (= [[false [7]] [true [8]]]
               (mapv (juxt :added (comp vec :v)) (data-datoms r))))
        (is (= [8] (vec (:v (first (d/datoms @conn :eavt e :one))))))))))

(deftest equal-bytes-are-one-identity
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true
             :initial-tx [{:db/ident :key :db/valueType :db.type/bytes
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity :db/contentId true}
                          {:db/ident :n :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}]}
        _ (d/create-database cfg)
        conn (d/connect cfg)
        e (get-in (d/transact conn [{:db/id "k" :key (bytes-of 1 2) :n 1}]) [:tempids "k"])]
    (testing "an entity's content id written again by equal bytes is the entity it names"
      (d/transact conn [[:db/add e :key (bytes-of 1 2)]])
      (d/transact conn [{:key (bytes-of 1 2) :n 2}])
      (is (= 2 (:n (d/pull @conn [:n] e))))
      (is (= 1 (count (d/datoms (d/history @conn) :eavt e :key)))))))
