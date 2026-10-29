(ns datahike.test.schema-key-seed-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.writing :as dw]
            [hasch.core :as hasch]))

(deftest a-db-loaded-from-its-store-hashes-no-schema
  ;; A process that only opens a db (a forked agent) has hashed no schema yet. stored->db holds the
  ;; key the store names, so serializing the db, or one a data transaction makes from it, calls the
  ;; hash no time: counted in calls (work done), where hashing a schema is one call over all of it.
  (let [cfg {:store {:backend :memory :id (random-uuid)} :keep-history? false}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :a/attr :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
        (let [db @conn
              record (dw/read-head (:store db) :db)
              _ (reset! @#'dw/last-schema-meta nil)
              loaded (dw/stored->db record (:store db))
              next-db (:db-after (d/with loaded [{:a/attr "x"}]))
              calls (atom 0)
              real hasch/uuid
              key (with-redefs [hasch/uuid (fn [& args] (swap! calls inc) (apply real args))]
                    (:schema-meta-key (second (dw/db->stored next-db false))))]
          (is (= (:schema-meta-key record) key) "the key is the one the store holds")
          (is (zero? @calls) (str "serializing a loaded db hashed the schema " @calls " times")))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))
