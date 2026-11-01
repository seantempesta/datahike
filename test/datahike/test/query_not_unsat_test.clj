(ns datahike.test.query-not-unsat-test
  "A negation whose body has no solution excludes nothing (upstream 7f39cccc, #1092).
  The planner's not-join projected its body to the join vars before deciding whether
  the body had a solution; the empty relation that made it unsolvable was one the join
  vars do not reach, so it was dropped and the clause excluded every row. Planner and
  base engine are both asked."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.db :as db]
            [datahike.query :as dq]))

(defn- not-join-unsat-db []
  (d/db-with (db/empty-db)
             [{:db/id 1 :name "Ivan"}
              {:db/id 2 :name "Oleg"}
              {:db/id 3 :city "Kyiv"}]))

(deftest test-not-join-unsatisfiable-body
  (let [db (not-join-unsat-db)
        answer (fn [query planner?]
                 (binding [dq/*disable-planner* (not planner?)
                           dq/*query-result-cache?* false]
                   (set (d/q query db))))
        both (fn [query] {:planner (answer query true) :base (answer query false)})]

    (testing "an unsatisfiable body excludes nothing, nested in an or-join"
      (is (= {:planner #{[1] [2]} :base #{[1] [2]}}
             (both '[:find ?e
                     :where [?e :name]
                     (or-join [?e]
                              (not-join [?e] [?e :name] [?o :city "nowhere"]))]))))

    (testing "a satisfiable body still excludes, nested in an or-join"
      (is (= {:planner #{} :base #{}}
             (both '[:find ?e
                     :where [?e :name]
                     (or-join [?e]
                              (not-join [?e] [?e :name] [?o :city "Kyiv"]))]))))

    (testing "an unsatisfiable body excludes nothing at the top level"
      (is (= {:planner #{[1] [2]} :base #{[1] [2]}}
             (both '[:find ?e
                     :where [?e :name]
                     (not-join [?e] [?e :name] [?o :city "nowhere"])]))))

    (testing "a join var bound by a function rather than a pattern"
      (is (= {:planner #{[1 false] [2 false]} :base #{[1 false] [2 false]}}
             (both '[:find ?e ?m
                     :where [?e :name]
                     [(ground false) ?m]
                     (not-join [?e ?m]
                               [(= ?m false)]
                               [?e :name]
                               [?o :city "nowhere"])]))))

    (testing "the same, with a body that does have a solution"
      (is (= {:planner #{} :base #{}}
             (both '[:find ?e ?m
                     :where [?e :name]
                     [(ground false) ?m]
                     (not-join [?e ?m]
                               [(= ?m false)]
                               [?e :name]
                               [?o :city "Kyiv"])]))))))
