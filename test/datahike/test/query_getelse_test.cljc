(ns datahike.test.query-getelse-test
  "Regression tests for `get-else` default binding — CLJC so the same
   assertions pin both the JVM and the CLJS/Node query engines.

   Bug (found live in the seon CLJS pod, 2026-06-11): on CLJS, queries
   using `(get-else $ ?e :attr default)` DROPPED rows lacking the
   attribute instead of binding the default value, silently turning a
   left-outer join into an inner join."
  (:require
   #?(:cljs [cljs.test :refer [is deftest testing]]
      :clj  [clojure.test :refer [is deftest testing]])
   [datahike.api :as d]
   [datahike.db :as db]))

(def test-db
  (-> (db/empty-db)
      (d/db-with [{:db/id 1, :name "Ivan", :age 15}
                  {:db/id 2, :name "Petr", :age 22, :height 240}
                  {:db/id 3, :name "Slava"}])))

(deftest test-get-else-binds-default-for-missing-attr
  (testing "rows lacking the attr survive with the default bound"
    (is (= #{[1 "Ivan" 300] [2 "Petr" 240] [3 "Slava" 300]}
           (d/q '[:find ?e ?name ?height
                  :where
                  [?e :name ?name]
                  [(get-else $ ?e :height 300) ?height]]
                test-db))))

  (testing "get-else clause before the pattern clause"
    (is (= #{[1 "Ivan" 300] [2 "Petr" 240] [3 "Slava" 300]}
           (d/q '[:find ?e ?name ?height
                  :where
                  [(get-else $ ?e :height 300) ?height]
                  [?e :name ?name]]
                test-db))))

  (testing "two get-else clauses on the same entity"
    (is (= #{[1 15 300] [2 22 240] [3 -1 300]}
           (d/q '[:find ?e ?age ?height
                  :where
                  [?e :name _]
                  [(get-else $ ?e :age -1) ?age]
                  [(get-else $ ?e :height 300) ?height]]
                test-db))))

  (testing "default of a different type than present values"
    (is (= #{[1 :none] [2 240] [3 :none]}
           (set (map (fn [[e h]] [e h])
                     (d/q '[:find ?e ?height
                            :where
                            [?e :name _]
                            [(get-else $ ?e :height :none) ?height]]
                          test-db)))))))

(def msg-db
  "Mirrors the seon agent-message shape that surfaced the bug live:
   messages with :to/:from/:at always present and :hops only sometimes."
  (-> (db/empty-db)
      (d/db-with [{:db/id 10, :to 1, :from 2, :at 100}          ;; no :hops
                  {:db/id 11, :to 1, :from 2, :at 200, :hops 3}
                  {:db/id 12, :to 1, :from 1, :at 300}          ;; self — filtered
                  {:db/id 13, :to 2, :from 1, :at 400}])))      ;; other recipient

(deftest test-get-else-with-in-const-and-predicate
  (testing "multi-pattern join + :in const + not= predicate + get-else (waking-hops shape)"
    (is (= #{[100 0] [200 3]}
           (d/q '[:find ?at ?h
                  :in $ ?me
                  :where
                  [?m :to ?me]
                  [?m :from ?f]
                  [(not= ?f ?me)]
                  [?m :at ?at]
                  [(get-else $ ?m :hops 0) ?h]]
                msg-db 1))))

  (testing "same query with get-else feeding a comparison predicate"
    (is (= #{[100] [200]}
           (d/q '[:find ?at
                  :in $ ?me ?cap
                  :where
                  [?m :to ?me]
                  [?m :from ?f]
                  [(not= ?f ?me)]
                  [(get-else $ ?m :hops 0) ?h]
                  [(< ?h ?cap)]
                  [?m :at ?at]]
                msg-db 1 5)))))

(def ref-db
  "get-else on a var bound in the VALUE position of a pattern clause
   (ref traversal). The entity var of the optional scan is never the
   entity of a sibling scan, so it lowers to a standalone
   LOptionalScan — the second live failure shape (seon pod: messages
   whose :from ref lacked :seon.user/id vanished from conversations)."
  (-> (db/empty-db {:from {:db/valueType :db.type/ref}})
      (d/db-with [{:db/id 1, :uid "alice"}
                  {:db/id 2, :aname "agent-x"}     ;; no :uid
                  {:db/id 10, :at 100, :from 1}
                  {:db/id 11, :at 200, :from 2}])))

(deftest test-get-else-on-ref-traversal
  (testing "entity var bound in value position of a ref pattern clause"
    (is (= #{[10 1 "alice"] [11 2 "none"]}
           (d/q '[:find ?m ?f ?u
                  :where
                  [?m :from ?f]
                  [(get-else $ ?f :uid "none") ?u]]
                ref-db)))))

(deftest test-get-else-standalone-entity-source
  (testing "entity var fed from :in collection, not a pattern clause"
    (is (= #{[1 300] [3 300] [2 240]}
           (d/q '[:find ?e ?height
                  :in $ [?e ...]
                  :where
                  [(get-else $ ?e :height 300) ?height]]
                test-db [1 2 3]))))

  (testing "entity var fed from a scalar :in const"
    (is (= #{[3 300]}
           (d/q '[:find ?e ?height
                  :in $ ?e
                  :where
                  [(get-else $ ?e :height 300) ?height]]
                test-db 3)))))
