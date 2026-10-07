(ns datahike.test.schema-meta-key-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.writing :as dw]
            [hasch.core :as hasch])
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
        (d/transact conn [{:a/attr-0 "x"}])
        (is (identical? (:schema db) (:schema @conn)) "a data commit keeps the identical schema")
        (is (< (* 10 (allocated #(stored-key @conn))) hashed)
            "keying an unchanged schema allocates a small fraction of hashing it")
        (d/transact conn [{:db/ident :a/added :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
        (is (= (hasch/uuid (schema-meta @conn)) (stored-key @conn)) "a schema change is keyed by its new content")
        (is (not= (hasch/uuid (schema-meta db)) (stored-key @conn))))
      (finally
        (d/release conn)
        (d/delete-database cfg)))))
