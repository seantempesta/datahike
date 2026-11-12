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

(deftest commits-that-change-no-schema-hash-none-whichever-connection-committed-last
  ;; Two connections in one process, as a server holds them (its main and a branch, or two
  ;; stores), commit in turn with no schema change: each commit's key is its schema's own, so
  ;; neither hashes a schema, whatever the other committed in between. A key kept for the
  ;; process's last schema was replaced by each other connection's, and every commit hashed
  ;; its whole schema again.
  (let [cfgs (for [attr [:a/one :b/two]]
               {:store {:backend :memory :id (random-uuid)} :keep-history? false
                :initial-tx [{:db/ident attr :db/valueType :db.type/string :db/cardinality :db.cardinality/one}]})
        _ (run! d/create-database cfgs)
        [x y] (mapv d/connect cfgs)]
    (try
      (d/transact x [{:a/one "0"}])
      (d/transact y [{:b/two "0"}])
      (let [calls (atom 0)
            real hasch/uuid]
        (with-redefs [hasch/uuid (fn [& args] (when (map? (first args)) (swap! calls inc)) (apply real args))]
          (dotimes [i 4]
            (d/transact x [{:a/one (str i)}])
            (d/transact y [{:b/two (str i)}])))
        (is (zero? @calls) (str "8 commits that changed no schema hashed one " @calls " times")))
      (finally
        (run! d/release [x y])
        (run! d/delete-database cfgs)))))
