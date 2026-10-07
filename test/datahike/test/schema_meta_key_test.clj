(ns datahike.test.schema-meta-key-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.writing :as dw]
            [hasch.core :as hasch]
            [konserve.core :as k])
  (:import [java.lang.management ManagementFactory]))

(defn- allocated
  "Bytes the calling thread allocates while running `f`."
  [f]
  (let [mx ^com.sun.management.ThreadMXBean (ManagementFactory/getThreadMXBean)
        id (.getId (Thread/currentThread))
        before (.getThreadAllocatedBytes mx id)]
    (f)
    (- (.getThreadAllocatedBytes mx id) before)))

(defn- schema-meta [db]
  (select-keys db [:schema :rschema :system-entities :ident-ref-map :ref-ident-map]))

(defn- connection-with-attributes [n]
  (let [cfg {:store {:backend :memory :id (random-uuid)} :keep-history? false}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn (vec (for [i (range n)]
                              {:db/ident (keyword "a" (str "attr-" i))
                               :db/valueType :db.type/string
                               :db/cardinality :db.cardinality/one})))
      [cfg conn])))

(deftest a-commit-that-changes-no-schema-hashes-no-schema
  (let [[cfg conn] (connection-with-attributes 1000)]
    (try
      (let [db @conn
            stored-key #(:schema-meta-key (second (dw/db->stored % false)))
            hashed (allocated #(hasch/uuid (schema-meta db)))]
        (is (= (hasch/uuid (schema-meta db)) (stored-key db)) "the key is the schema-meta's content uuid")
        (let [next-db (:db-after (d/with db [{:a/attr-0 "x"}]))]
          (is (every? (fn [[k value]] (identical? value (get next-db k))) (schema-meta db))
              "a data transaction keeps every schema-meta member identical")
          (is (< (* 10 (allocated #(stored-key next-db))) hashed)
              "the first serialization of the new database does not hash the schema")
          (is (= (hasch/uuid (schema-meta next-db)) (stored-key next-db))))
        (d/transact conn [{:db/ident :a/added :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
        (is (= (hasch/uuid (schema-meta @conn)) (stored-key @conn)) "a schema change is keyed by its new content")
        (is (not= (hasch/uuid (schema-meta db)) (stored-key @conn))))
      (finally
        (d/release conn)
        (d/delete-database cfg)))))

(deftest mutable-schema-values-are-rehashed
  (doseq [[value mutate!] [[(java.util.Date. 1000) #(.setTime ^java.util.Date % 2000)]
                           [(byte-array [1 2]) #(aset-byte ^bytes % 0 (byte 3))]]]
    (let [[cfg conn] (connection-with-attributes 1)]
      (try
        (d/transact conn [{:db/ident :a/mutable
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one
                          :db.secondary/config {:value value}}])
        (let [before @conn
              stored-key #(:schema-meta-key (second (dw/db->stored % false)))
              original (stored-key before)]
          (is (identical? value (get-in before [:schema :a/mutable :db.secondary/config :value])))
          (mutate! value)
          (d/transact conn [{:a/attr-0 "x"}])
          (is (identical? (:schema before) (:schema @conn)))
          (is (not= original (hasch/uuid (schema-meta @conn))))
          (is (= (hasch/uuid (schema-meta @conn)) (stored-key @conn)))
          (is (= (hasch/uuid (schema-meta @conn))
                 (:schema-meta-key (k/get (:store @conn) :db nil {:sync? true})))))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))
