(ns datahike.test.cross-connection-revision-test
  "An attribute's revision, or else the conservative revision, names a commit
   whose datoms of that attribute equal the value's, on every connection."
  (:require
   [clojure.test :refer [is deftest]]
   [datahike.api :as d]))

(def ^:private schema
  [{:db/ident :s/id :db/valueType :db.type/string :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}
   {:db/ident :s/a :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :s/b :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(defn- with-store [f]
  (let [cfg {:store {:backend :memory :id (random-uuid)} :schema-flexibility :write :keep-history? false}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      (d/transact conn schema)
      (d/transact conn [{:s/id "x" :s/a 1 :s/b 1}])
      (f cfg conn)
      (finally
        (d/release conn)
        (d/delete-database cfg)))))

(defn- context [database] (:cache-context database))

(defn- revision
  "The commit `attribute`'s datoms on `database` equal: its own revision, else the conservative one."
  [database attribute]
  (let [{:datahike.cache/keys [attribute-revisions conservative-revision]} (context database)]
    (get attribute-revisions attribute conservative-revision)))

(deftest a-branch-connection-names-the-commit-it-opened-at
  (with-store
    (fn [cfg conn]
      (d/transact conn [{:s/id "x" :s/a 2}])
      (let [head @conn]
        (d/branch! conn :db :lane)
        (let [lane (d/connect (assoc cfg :branch :lane))]
          (try
            (is (= (d/commit-id head) (:datahike.cache/conservative-revision (context @lane)))
                "a connection opens with its commit as the conservative revision")
            (is (empty? (:datahike.cache/attribute-revisions (context @lane))))
            (is (= (revision head :s/a) (d/commit-id head)))
            (d/transact lane [{:s/id "x" :s/b 3}])
            (is (= (d/commit-id head) (revision @lane :s/a)) "an untouched attribute still names the opening commit")
            (is (= (d/commit-id @lane) (revision @lane :s/b)) "a written attribute names the commit that wrote it")
            (finally (d/release lane))))))))

(deftest a-conservative-advance-clears-the-attribute-revisions
  (with-store
    (fn [_ conn]
      (d/transact conn [{:s/id "x" :s/a 2}])
      (is (seq (:datahike.cache/attribute-revisions (context @conn))))
      (d/transact conn [{:db/ident :s/c :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
      (is (= (d/commit-id @conn) (:datahike.cache/conservative-revision (context @conn))))
      (is (empty? (:datahike.cache/attribute-revisions (context @conn)))))))
