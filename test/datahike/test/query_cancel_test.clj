(ns datahike.test.query-cancel-test
  "Mid-query cancellation via the :cancel channel.

   Covers:
   - Pre-set flag raises at the first check point (fast path)
   - Concurrent flip from a watchdog thread interrupts a live scan
   - :cancel nil and :cancel (volatile! false) are free — results flow
   - Direct-HashSet path, relation path (predicate forces fallback),
     and the adaptive execute-plan outer loop each observe the flag

   Runs against the query planner engine; legacy engine cancellation is
   not yet implemented (pgwire enables the planner, so it's not on the
   critical path)."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike.api :as d]
            [datahike.query :as q]))

(def ^:dynamic ^:private *conn* nil)

(def ^:private cfg
  {:store {:backend :memory :id #uuid "cafe0001-0000-0000-0000-cace10000001"}
   :schema-flexibility :read
   :keep-history? false})

(defn- setup-db
  "Load N datoms into a fresh memory db. N=50k keeps a full scan in
   the tens of ms so cancellation timing is observable but the fixture
   isn't painfully slow."
  [n]
  (d/create-database cfg)
  (let [conn (d/connect cfg)]
    (d/transact conn (into []
                           (mapcat (fn [i]
                                     [[:db/add (inc i) :x i]
                                      [:db/add (inc i) :y (str "v" i)]
                                      [:db/add (inc i) :group (mod i 10)]]))
                           (range n)))
    conn))

(defn- with-db-fixture [f]
  (try (d/delete-database cfg) (catch Exception _ nil))
  (binding [*conn* (setup-db 50000)
            ;; Disable the result cache — repeated identical queries
            ;; would otherwise hit the cache and bypass execute entirely,
            ;; which short-circuits the cancel check we're trying to exercise.
            q/*query-result-cache?* false]
    (try (f)
         (finally
           (d/release *conn*)
           (d/delete-database cfg)))))

(use-fixtures :each with-db-fixture)

(defn- cancel-exception? [e]
  (and (instance? clojure.lang.ExceptionInfo e)
       (true? (:datahike/canceled (ex-data e)))))

(defn- budget-exception? [e budget-name]
  (and (instance? clojure.lang.ExceptionInfo e)
       (true? (:datahike/budget-exceeded (ex-data e)))
       (= budget-name (:datahike.budget/name (ex-data e)))))

(deftest synchronous-resource-budgets
  (let [db (d/db *conn*)]
    (testing "semantic limit does not bypass the work budget"
      (let [error (try
                    (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                          :args [db]
                          :limit 1
                          :order-by '[?v :asc]
                          :max-work 10})
                    nil
                    (catch Exception error error))]
        (is (budget-exception? error :query-work))))
    (testing "output exhaustion is structured and never a partial result"
      (let [error (try
                    (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                          :args [db]
                          :max-work 100
                          :max-results 5})
                    nil
                    (catch Exception error error))]
        (is (budget-exception? error :query-results))))
    (testing "relation and legacy collectors charge before retaining output"
      (doseq [[disable-planner? query]
              [[false '[:find (count ?e) :where [?e :x]]]
               [true '[:find ?e ?v :where [?e :x ?v]]]]]
        (binding [q/*disable-planner* disable-planner?]
          (let [error (try
                        (d/q {:query query
                              :args [db]
                              :max-results 5})
                        nil
                        (catch Exception error error))]
            (is (budget-exception? error :query-results))))))
    (testing "aggregate work is bounded despite scalar output"
      (let [error (try
                    (d/q {:query '[:find (count ?e) . :where [?e :x]]
                          :args [db]
                          :max-work 10
                          :max-results 2})
                    nil
                    (catch Exception error error))]
        (is (budget-exception? error :query-work))))
    (testing "broad connected joins exhaust work before retaining their product"
      (doseq [disable-planner? [false true]]
        (binding [q/*disable-planner* disable-planner?]
          (let [error (try
                        (d/q {:query '[:find ?left ?right
                                      :where
                                      [?left :group ?group]
                                      [?right :group ?group]]
                              :args [db]
                              :max-work 100})
                        nil
                        (catch Exception error error))]
            (is (budget-exception? error :query-work)
                (str "connected join is bounded with planner disabled? "
                     disable-planner?))))))
    (testing "disconnected Cartesian components exhaust the same work budget"
      (doseq [disable-planner? [false true]]
        (binding [q/*disable-planner* disable-planner?]
          (let [error (try
                        (d/q {:query '[:find ?left ?right
                                      :where
                                      [?left :x]
                                      [?right :y]]
                              :args [db]
                              :max-work 100})
                        nil
                        (catch Exception error error))]
            (is (budget-exception? error :query-work)
                (str "Cartesian product is bounded with planner disabled? "
                     disable-planner?))))))
    (testing "find-pull inherits the active result-weight budget"
      (let [error (try
                    (d/q {:query '[:find (pull ?e [:x :y])
                                   :where [?e :x 0]]
                          :args [db]
                          :max-result-weight 4})
                    nil
                    (catch Exception error error))]
        (is (budget-exception? error :result-weight))))
    (testing "a later bounded query succeeds after exhaustion"
      (is (= 1 (count (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                            :args [db]
                            :limit 1
                            :max-work 100000
                            :max-results 5
                            :max-result-weight 100})))))))

(deftest cancel-nil-is-free
  (testing ":cancel nil (default) does not affect results"
    (binding [q/*disable-planner* false]
      (let [db (d/db *conn*)
            r1 (d/q '[:find ?e ?v :where [?e :x ?v]] db)
            r2 (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                     :args [db]
                     :cancel nil})
            r3 (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                     :args [db]
                     :cancel (volatile! false)})]
        (is (= 50000 (count r1)))
        (is (= r1 r2))
        (is (= r1 r3))))))

(deftest preset-cancel-raises-fast
  (testing "pre-set cancel flag raises :datahike/canceled at first check point"
    (binding [q/*disable-planner* false]
      (let [db (d/db *conn*)
            thrown (try
                     (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                           :args [db]
                           :cancel (volatile! true)})
                     nil
                     (catch Exception e e))]
        (is (some? thrown))
        (is (cancel-exception? thrown))))))

(deftest concurrent-cancel-direct-path
  (testing "watchdog flip interrupts a live scan on the direct path"
    ;; Without timing budgets: the bound loop (dotimes 200) would take
    ;; 1-2s in 50k-row queries if cancel did nothing; the cancel flip
    ;; makes it bail at the first check-cancel! site on the next
    ;; iteration after the flag is observed. We only assert correctness
    ;; (cancel-exception raised) — not a specific upper bound on
    ;; elapsed time, which is JIT/GC-sensitive.
    (binding [q/*disable-planner* false]
      (let [db (d/db *conn*)
            cancel (volatile! false)
            watchdog (future
                       (Thread/sleep 5)
                       (vreset! cancel true))
            thrown (try
                     (dotimes [_ 200]
                       (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                             :args [db]
                             :cancel cancel}))
                     nil
                     (catch Exception e e))]
        @watchdog
        (is (cancel-exception? thrown))))))

(deftest concurrent-cancel-relation-path
  (testing "cancel also fires on the relation path (predicate forces fallback)"
    (binding [q/*disable-planner* false]
      (let [db (d/db *conn*)
            cancel (volatile! false)
            watchdog (future
                       (Thread/sleep 5)
                       (vreset! cancel true))
            thrown (try
                     (dotimes [_ 200]
                       (d/q {:query '[:find ?v1 ?v2
                                      :where [?e1 :x ?v1]
                                      [?e2 :x ?v2]
                                      [(< ?v1 ?v2)]]
                             :args [db]
                             :cancel cancel}))
                     nil
                     (catch Exception e e))]
        @watchdog
        (is (cancel-exception? thrown))))))

(deftest cancel-reset-allows-reuse
  (testing "vreset! cancel false → query runs to completion again"
    (binding [q/*disable-planner* false]
      (let [db (d/db *conn*)
            cancel (volatile! true)]
        (is (thrown? Exception
                     (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                           :args [db]
                           :cancel cancel})))
        (vreset! cancel false)
        (is (= 50000 (count (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                                  :args [db]
                                  :cancel cancel}))))))))
