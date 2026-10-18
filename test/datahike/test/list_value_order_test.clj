(ns datahike.test.list-value-order-test
  "Vector values keep one order across a storage round trip."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.datom :as dd]))

(defn- stored-list
  "What Fressian reads a stored vector back as: a java.util.List (Arrays$ArrayList)."
  [& xs]
  (java.util.Arrays/asList (object-array xs)))

(deftest lists-order-like-the-vectors-they-were
  ;; Two lists threw from compare-value (Arrays$ArrayList is not Comparable), and
  ;; a vector against a list fell back to class-name order, so a value's place in
  ;; an index changed once its node was read back from storage.
  (is (neg? (dd/compare-value (stored-list :x 1) (stored-list :x 2))))
  (is (zero? (dd/compare-value (stored-list :x 1) (stored-list :x 1))))
  (is (= (compare [:x 1] [:x 2]) (dd/compare-value [:x 1] (stored-list :x 2))))
  (is (= (compare [:x 2] [:x 1]) (dd/compare-value (stored-list :x 2) [:x 1])))
  (is (= (compare [:x 1] [:x 1 0]) (dd/compare-value (stored-list :x 1) (stored-list :x 1 0)))
      "a shorter list orders first, as vectors do"))

(defn- counts
  "A set of `[class count]` pairs built as map entries, as `(set (frequencies ...))`
  builds them: Fressian stores a map entry as a list and reads it back as one."
  [prefix n]
  (set (frequencies (mapcat (fn [i] (repeat (inc i) (keyword (str prefix i)))) (range n)))))

(deftest tuple-sets-read-back-from-storage-take-more-values
  (let [path (str (System/getProperty "java.io.tmpdir") "/datahike-list-order-" (random-uuid))
        config {:store {:backend :file :path path :id (random-uuid)}
                :schema-flexibility :write
                :keep-history? true
                :fuse-index-roots? true
                :index-config {:branching-factor 128 :diff-buf-size 32}
                :initial-tx [{:db/ident :pair :db/cardinality :db.cardinality/many
                              :db/valueType :db.type/tuple :db/tupleTypes [:db.type/keyword :db.type/long]}]}]
    (d/create-database config)
    (try
      (let [conn (d/connect config)]
        (doseq [e (range 1 31)] (d/transact conn [{:db/id e :pair (counts "c" 20)}]))
        ;; later values of the same entities stay buffered in their branches' slots
        (doseq [r (range 5) e (range 1 31)] (d/transact conn [{:db/id e :pair (counts (str "d" r) 1)}]))
        (d/release conn))
      (let [conn (d/connect config)]
       (try
        (is (= 25 (count (d/datoms @conn :eavt 1 :pair))) "the stored values read back")
        (d/transact conn [[:db/retract 1 :pair [:c3 4]] [:db/add 1 :pair [:c3 100]]])
        (is (= #{[:c3 100]} (into #{} (comp (map (comp vec :v)) (filter #(= :c3 (first %)))) (d/datoms @conn :eavt 1 :pair)))
            "a vector retracts the stored list it equals and adds beside it")
        (finally (d/release conn))))
      (finally (d/delete-database config)))))
