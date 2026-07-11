(ns datahike.test.cljs-pattern-scan-test
  "Regression tests for the CLJS bug fixed by this PR.

   In CLJS, `execute-pattern-scan` previously materialized each datom
   as a Clojure vector `[(.-e d) (.-a d) (.-v d) (.-tx d) true]`. The
   downstream join machinery in `query.cljc` / `query/relation.cljc`
   read tuple elements via `da/aget`, which on CLJS expands to
   `(arr[i])`. PersistentVectors don't expose elements as
   integer-indexed JS properties, so every tuple access returned nil.

   Symptom: multi-clause queries such as
       [:find [?var ...] :where [?a ...] [?b ?join ?a]]
   would join on nil keys (cross-product), and FindColl extracts
   would return `[nil]` instead of the expected results.

   The fix has two parts:
     1) execute-pattern-scan now produces JS arrays on CLJS.
     2) tuple access sites use `get` (works uniformly on JS arrays,
        Object[], and PersistentVectors) instead of `da/aget`.

   On CLJ this code path was historically masked when the base engine ran;
   the planner is now the default (opt out with DATAHIKE_QUERY_PLANNER=false),
   and on CLJS the planner is always the default."
  (:require
   #?(:clj  [clojure.test :as t :refer [is deftest]]
      :cljs [cljs.test :as t :refer-macros [is deftest]])
   [clojure.core.async :as a :refer [<!]]
   ;; <p! bridges Promise → channel inside a go block; needed since
   ;; datahike.api on CLJS now returns js/Promise (see
   ;; datahike.api.async). Channel-returning libs (konserve etc.) keep
   ;; using plain <!.
   #?(:cljs [cljs.core.async.interop :refer-macros [<p!]])
   [datahike.api :as d]
   [datahike.db]
   [datahike.query]
   [datahike.test.async #?(:clj :refer :cljs :refer-macros) [deftest-async <!?]]))

(defn- mem-cfg []
  {:store {:backend :memory
           :id #?(:clj (java.util.UUID/randomUUID)
                  :cljs (random-uuid))}
   :keep-history? false
   :schema-flexibility :read})

(defn- setup [cfg]
  (a/go
    #?(:clj  (do (d/create-database cfg)
                 (d/connect cfg))
       :cljs (do (<p! (d/create-database cfg))
                 (<p! (d/connect cfg))))))

(defn- teardown [conn cfg]
  (a/go
    #?(:clj  (do (d/release conn)
                 (d/delete-database cfg))
       :cljs (do (<p! (d/release conn))
                 (<p! (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; The failing query from the PR description:
;;   [:find [?var ...] :where [?a ...] [?b ?join ?a]]
;; Before the fix, FindColl returns [nil] on CLJS.

(deftest-async multi-clause-find-coll
  (let [cfg  (mem-cfg)
        conn (<! (setup cfg))]
    ;; A handful of entities with two attributes; the query joins
    ;; on a value that's also an attribute name elsewhere.
    (<!? (d/transact! conn [{:db/id -1 :a 1 :b 10}
                            {:db/id -2 :a 2 :b 20}
                            {:db/id -3 :a 3 :b 30}]))
    (let [;; Simplest multi-clause FindColl: project entity ids whose
          ;; :a value matches some entity's :b. Triggers hash-join
          ;; over scan-produced tuples.
          result (d/q '[:find [?e ...]
                        :where
                        [?e :a ?v]
                        [_ :b ?v]]
                      @conn)]
      ;; Pre-fix on CLJS: result was [nil] (single nil from
      ;; cross-product fed through nil-extracting join).
      ;; Post-fix: the query has no matches (no :b value equals
      ;; any :a value in this dataset) → result is empty.
      (is (not (= [nil] result))
          (str "Multi-clause FindColl must not return [nil]; got: "
               (pr-str result))))
    (<! (teardown conn cfg))))

;; ---------------------------------------------------------------------------
;; A positive case: matched join. Pre-fix: produced [nil] (or wrong
;; cardinality) due to nil tuple keys. Post-fix: returns the matched
;; entity ids.

(deftest-async two-clause-join-finds-matches
  (let [cfg  (mem-cfg)
        conn (<! (setup cfg))]
    (<!? (d/transact! conn [{:db/id -1 :name "Alice" :friend "Bob"}
                            {:db/id -2 :name "Bob"   :friend "Carol"}
                            {:db/id -3 :name "Carol" :friend "Dave"}]))
    (let [;; Find names of entities someone is a friend of.
          result (set (d/q '[:find [?name ...]
                             :where
                             [?e :friend ?fname]
                             [?f :name ?fname]
                             [?f :name ?name]]
                           @conn))]
      (is (= #{"Bob" "Carol"} result)
          (str "Expected {Bob, Carol}; got: " (pr-str result))))
    (<! (teardown conn cfg))))

;; ---------------------------------------------------------------------------
;; Single-clause FindColl as a control: this path doesn't exercise
;; hash-join, so it should pass even before the fix. Acts as a
;; sanity check that the test infrastructure itself is correct.

(deftest-async single-clause-find-coll-works
  (let [cfg  (mem-cfg)
        conn (<! (setup cfg))]
    (<!? (d/transact! conn [{:db/id -1 :name "Ivan"}
                            {:db/id -2 :name "Petr"}]))
    (let [result (set (d/q '[:find [?name ...]
                             :where [_ :name ?name]]
                           @conn))]
      (is (= #{"Ivan" "Petr"} result)))
    (<! (teardown conn cfg))))

;; ---------------------------------------------------------------------------
;; Regression (2026-06-10, found by seon's gym lane): a query joining TWO
;; identity-attr clauses through one message row —
;;   [?ag :seon.agent/id ?bid] [?m :from ?ag] [?m :to ?u] [?u :seon.user/id "user"]
;; with `:in $ ?bid` — IGNORED the ?bid binding and returned the
;; inverse-direction (user→agent) rows. Root cause: execute-plan-direct's
;; multi-group hash-probe loop only supports a single producer→consumer
;; edge; this plan has ONE producer (the ?m entity-group) feeding TWO
;; consumer pattern-scans (?ag and ?u). The producer built one probe-map
;; keyed by ?ag, the ?u consumer probed it with ?u values (joining through
;; the wrong key), and its combine step clobbered the ?ag join's results.
;; Fixed conservatively: can-direct-fuse? now rejects multi-edge /
;; multi-consumer / chained group-join topologies, falling back to the
;; Relation path. On CLJ this was historically masked by the planner being
;; opt-in; with the planner default-ON the CLJ engine had the same bug.

(deftest two-identity-joins-through-one-row-honor-in-binding
  (let [schema {:seon.agent/id     {:db/unique :db.unique/identity}
                :seon.user/id      {:db/unique :db.unique/identity}
                :seon.message/from {:db/valueType :db.type/ref}
                :seon.message/to   {:db/valueType :db.type/ref}}
        db (-> (datahike.db/empty-db schema)
               (d/db-with
                [{:db/id -1 :seon.user/id "user"}
                 {:db/id -2 :seon.agent/id "a"}
                 {:db/id -3 :seon.agent/id "b"}
                 {:seon.message/from -1 :seon.message/to -2
                  :seon.message/content "alpha question"}
                 {:seon.message/from -2 :seon.message/to -1
                  :seon.message/content "ALPHA-ANSWER"}
                 {:seon.message/from -1 :seon.message/to -3
                  :seon.message/content "beta question"}
                 {:seon.message/from -3 :seon.message/to -1
                  :seon.message/content "BETA-ANSWER"}]))
        query '[:find ?c
                :in $ ?bid
                :where
                [?ag :seon.agent/id ?bid]
                [?m :seon.message/from ?ag]
                [?m :seon.message/to ?u]
                [?u :seon.user/id "user"]
                [?m :seon.message/content ?c]]
        run (fn [bid]
              #?(:clj  (binding [datahike.query/*disable-planner* false]
                         (d/q query db bid))
                 :cljs (d/q query db bid)))]
    (is (= #{["BETA-ANSWER"]} (run "b"))
        "?bid binding must scope the from-agent join (pre-fix: returned the inverse user→agent rows)")
    (is (= #{["ALPHA-ANSWER"]} (run "a")))
    ;; clause order must not matter
    (is (= #{["BETA-ANSWER"]}
           #?(:clj (binding [datahike.query/*disable-planner* false]
                     (d/q '[:find ?c
                            :in $ ?bid
                            :where
                            [?u :seon.user/id "user"]
                            [?m :seon.message/to ?u]
                            [?m :seon.message/from ?ag]
                            [?ag :seon.agent/id ?bid]
                            [?m :seon.message/content ?c]]
                          db "b"))
              :cljs (d/q '[:find ?c
                           :in $ ?bid
                           :where
                           [?u :seon.user/id "user"]
                           [?m :seon.message/to ?u]
                           [?m :seon.message/from ?ag]
                           [?ag :seon.agent/id ?bid]
                           [?m :seon.message/content ?c]]
                         db "b"))))))

(comment
  ;; CLJ REPL:
  (require 'datahike.test.cljs-pattern-scan-test :reload)
  (clojure.test/run-tests 'datahike.test.cljs-pattern-scan-test)
  ;; CLJS REPL (after `npx shadow-cljs watch cljs-tests`):
  ;; (cljs.test/run-tests 'datahike.test.cljs-pattern-scan-test)
  )
