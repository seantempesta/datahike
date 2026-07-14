(ns datahike.test.query-input-pull-test
  (:require
   #?(:cljs [cljs.test :refer-macros [deftest is testing]]
      :clj [clojure.test :refer [deftest is testing]])
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query :as q]))

(deftest find-pull-preserves-scalar-and-collection-entity-inputs
  (testing "planned and legacy execution project a const-bound pull entity"
    (let [db (-> (db/empty-db)
                 (d/db-with [{:db/id 1 :name "Ivan" :age 15}]))
          scalar-query '[:find (pull ?e [:name :age])
                         :in $ ?e
                         :where [?e :name]]
          collection-query '[:find (pull ?e [:name :age])
                             :in $ [?e ...]
                             :where [?e :name]]
          expected [[{:name "Ivan" :age 15}]]]
      (doseq [disable-planner? [false true]]
        (binding [q/*disable-planner* disable-planner?]
          (is (= expected (d/q scalar-query db 1)))
          (is (= expected (d/q collection-query db [1]))))))))
