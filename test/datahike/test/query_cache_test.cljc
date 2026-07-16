(ns datahike.test.query-cache-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   #?(:clj [datahike.db :as db])
   #?(:clj [datahike.connections :as connections])
   #?(:clj [datahike.lru :as lru])
   #?(:clj [datahike.query :as dq])
   #?(:clj [datahike.store :as store])
   #?(:clj [datahike.writing :as writing])))

(defn- with-temp-db
  "Create a temp in-memory db with schema, run f with the connection, then clean up."
  [schema-txs f]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :attribute-refs? false}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      (d/transact conn schema-txs)
      (f conn)
      (finally
        (d/release conn)
        (d/delete-database cfg)))))

(def ^:private label-schema
  [{:db/ident :c/id
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident :c/labels
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many}
   {:db/ident :c/note
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

#?(:clj
   (defn- query-cache-keys []
     (set (keys (lru/weighted-entries (:lru @dq/query-result-cache))))))

#?(:clj
   (deftest committed-identity-ignores-forced-legacy-collision-across-stores
     (let [cfg-a {:store {:backend :memory :id (random-uuid)}
                  :schema-flexibility :write :attribute-refs? false}
           cfg-b {:store {:backend :memory :id (random-uuid)}
                  :schema-flexibility :write :attribute-refs? false}]
       (try
         (dq/clear-query-cache!)
         (d/create-database cfg-a)
         (d/create-database cfg-b)
         (let [conn-a (d/connect cfg-a)
               conn-b (d/connect cfg-b)]
           (try
             (d/transact conn-a (conj label-schema {:c/id "a" :c/note "store-a"}))
             (d/transact conn-b (conj label-schema {:c/id "b" :c/note "store-b"}))
             (let [db-a (assoc @conn-a :hash ::forced :max-tx 7 :max-eid 9)
                   db-b (assoc @conn-b :hash ::forced :max-tx 7 :max-eid 9)
                   identity-a (db/committed-cache-identity db-a)
                   identity-b (db/committed-cache-identity db-b)
                   query '[:find ?n . :where [_ :c/note ?n]]]
               (is (= (mapv #(get db-a %) [:hash :max-tx :max-eid])
                      (mapv #(get db-b %) [:hash :max-tx :max-eid])))
               (is (not= identity-a identity-b))
               (is (= ["store-a" "store-b" "store-a" "store-b"]
                      [(d/q query db-a) (d/q query db-b)
                       (d/q query db-a) (d/q query db-b)]))
               (is (= #{identity-a identity-b} (query-cache-keys)))
               (d/release conn-a)
               (is (= #{identity-b} (query-cache-keys)))
               (is (= "store-b" (d/q query db-b))))
             (finally
               (try (d/release conn-a) (catch Exception _))
               (d/release conn-b))))
         (finally
           (d/delete-database cfg-a)
           (d/delete-database cfg-b)
           (dq/clear-query-cache!))))))

#?(:clj
   (deftest sibling-branches-at-one-commit-have-independent-cache-scopes
     (let [cfg {:store {:backend :memory :id (random-uuid)}
                :schema-flexibility :write :attribute-refs? false}]
       (try
         (dq/clear-query-cache!)
         (d/create-database cfg)
         (let [main (d/connect cfg)]
           (d/transact main (conj label-schema {:c/id "branch" :c/note "base"}))
           (let [head (d/commit-id @main)]
             (d/branch! main head :left)
             (d/branch! main head :right)
             (d/release main)
             (let [left (d/connect (assoc cfg :branch :left))
                   right (d/connect (assoc cfg :branch :right))]
               (try
                 (let [left-id (db/committed-cache-identity @left)
                       right-id (db/committed-cache-identity @right)
                       query '[:find ?n . :where [_ :c/note ?n]]]
                   (is (= (last left-id) (last right-id)))
                   (is (not= (first left-id) (first right-id)))
                   (is (not= (second left-id) (second right-id)))
                   (is (= ["base" "base"] [(d/q query @left) (d/q query @right)]))
                   (is (= #{left-id right-id} (query-cache-keys)))
                   (d/transact left [{:c/id "branch" :c/note "left-new"}])
                   (is (= ["left-new" "base"] [(d/q query @left) (d/q query @right)]))
                   (d/release left)
                   (is (= #{right-id} (query-cache-keys)))
                   (is (= "base" (d/q query @right))))
                 (finally
                   (try (d/release left) (catch Exception _))
                   (d/release right))))))
         (finally
           (d/delete-database cfg)
           (dq/clear-query-cache!))))))

#?(:clj
   (deftest committed-cache-identity-excludes-speculative-and-temporal-values
     (with-temp-db label-schema
       (fn [conn]
         (d/transact conn [{:c/id "identity" :c/note "committed"}])
         (let [committed @conn
               speculative (:db-after (d/with committed [{:c/id "identity"
                                                          :c/note "speculative"}]))]
           (is (= 3 (count (db/committed-cache-identity committed))))
           (is (nil? (db/committed-cache-identity speculative)))
           (is (nil? (db/committed-cache-identity (d/as-of committed (:max-tx committed)))))
           (is (nil? (db/committed-cache-identity (d/history committed)))))))))

#?(:clj
   (deftest final-release-atomically-fences-late-cache-put
     (let [cfg {:store {:backend :memory :id (random-uuid)}
                :schema-flexibility :write
                :attribute-refs? false}
           _ (d/create-database cfg)]
       (try
         (dq/clear-query-cache!)
         (let [conn (d/connect cfg)]
           (d/transact conn label-schema)
           (d/transact conn [{:c/id "late" :c/note "value"}])
           (let [committed @conn
                 [connection-id generation-before _]
                 (db/committed-cache-identity committed)]
             (d/q '[:find ?n :where [_ :c/note ?n]] committed)
             (is (pos? (:snapshot-count (dq/query-cache-metrics))))
             (d/release conn)
             (is (zero? (:snapshot-count (dq/query-cache-metrics))))
             (#'dq/result-cache-put! committed ::late #{["stale"]} #{:c/note}
                                     (:epoch @dq/query-result-cache))
             (is (zero? (:snapshot-count (dq/query-cache-metrics)))
                 "a query finishing after release cannot resurrect its generation")
             (let [reconnected (d/connect cfg)
                   [_ generation-after _] (db/committed-cache-identity @reconnected)]
               (is (not= generation-before generation-after))
               (d/q '[:find ?n :where [_ :c/note ?n]] @reconnected)
               (let [before-stale-close (dq/query-cache-metrics)]
                 (dq/close-query-cache-generation!
                  connection-id generation-before)
                 (is (= before-stale-close (dq/query-cache-metrics))
                     "a delayed old-generation close cannot erase a reconnect"))
               (d/release reconnected))))
         (finally
           (d/delete-database cfg)
           (dq/clear-query-cache!))))))

#?(:clj
   (deftest explicit-clear-atomically-fences-an-already-admitted-cache-put
     (with-temp-db
       (conj label-schema {:c/id "clear" :c/note "value"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [database @conn
               admitted-epoch (:epoch @dq/query-result-cache)]
           (dq/clear-query-cache!)
           (is (false?
                (#'dq/result-cache-put! database ::late #{["stale"]}
                                        #{:c/note} admitted-epoch)))
           (is (zero? (:snapshot-count (dq/query-cache-metrics)))))))))

#?(:clj
   (deftest config-mismatch-does-not-open-or-alter-cache-generation
     (let [cfg {:store {:backend :memory :id (random-uuid)}
                :keep-history? true
                :schema-flexibility :write
                :attribute-refs? false}
           _ (d/create-database cfg)
           conn (d/connect cfg)]
       (try
         (d/transact conn (conj label-schema {:c/id "mismatch" :c/note "stable"}))
         (d/q '[:find ?n :where [_ :c/note ?n]] @conn)
         (let [conn-id (store/connection-id (:config @conn))
               entry-before (get @connections/*connections* conn-id)
               metrics-before (dq/query-cache-metrics)
               keys-before (query-cache-keys)]
           (is (= :config-does-not-match-existing-connections
                  (try
                    (d/connect (assoc cfg :keep-history? false))
                    (catch Exception e (:type (ex-data e))))))
           (is (= entry-before (get @connections/*connections* conn-id)))
           (is (= metrics-before (dq/query-cache-metrics)))
           (is (= keys-before (query-cache-keys)))
           (is (= #{["stable"]}
                  (d/q '[:find ?n :where [_ :c/note ?n]] @conn))))
         (finally
           (d/release conn)
           (d/delete-database cfg)
           (dq/clear-query-cache!))))))

#?(:clj
   (deftest identical-concurrent-datahike-queries-compute-once
     (with-temp-db
       (conj label-schema {:c/id "single-flight" :c/note "value"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [database @conn
               start (java.util.concurrent.CountDownLatch. 1)
               calls (atom 0)
               pred (fn [_]
                      (swap! calls inc)
                      (Thread/sleep 75)
                      true)
               run #(d/q '[:find ?n
                           :in $ ?pred
                           :where [_ :c/note ?n]
                           [(?pred ?n)]]
                         database pred)
               workers (doall
                        (for [_ (range 16)]
                          (future (.await start) (run))))]
           (.countDown start)
           (is (= (vec (repeat 16 #{["value"]})) (mapv deref workers)))
           (is (= 1 @calls))
           (is (zero? (get-in (dq/query-cache-metrics)
                              [:single-flight :active-flights]))))))))

#?(:clj
   (deftest evidence-returning-query-preserves-result-and-reports-local-work
     (with-temp-db
       (conj label-schema {:c/id "evidence" :c/note "value"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [database @conn
               query '[:find ?n :where [_ :c/note ?n]]
               owner (dq/q-with-evidence query database)
               hit (dq/q-with-evidence query database)]
           (is (= #{["value"]} (d/q query database)
                  (:datahike.query/result owner)
                  (:datahike.query/result hit)))
           (is (= :datahike.cache.outcome/miss-owner
                  (get-in owner [:datahike.query/cache-evidence
                                 :datahike.cache/outcome])))
           (is (= :datahike.cache.outcome/hit
                  (get-in hit [:datahike.query/cache-evidence
                               :datahike.cache/outcome])))
           (is (= #{:c/note}
                  (:datahike.query/attribute-dependencies owner)
                  (:datahike.query/attribute-dependencies hit)))
           (is (= #{:c/note}
                  (:datahike.query/attribute-dependencies
                   (with-redefs [dq/query-attribute-dependencies
                                 (fn [_]
                                   (throw (ex-info "unexpected recomputation"
                                                   {})))]
                     (dq/q-with-evidence query database)))))
           (is (= #{:c/note}
                  (:datahike.query/attribute-dependencies
                   (binding [dq/*query-result-cache?* false]
                     (dq/q-with-evidence query database)))))
           (is (= :all
                  (:datahike.query/attribute-dependencies
                   (dq/q-with-evidence
                    '[:find ?attribute :where [_ ?attribute _]] database))))
           (is (pos? (get-in owner [:datahike.query/resource-evidence
                                    :datahike.resource/work])))
           (is (= 0 (get-in hit [:datahike.query/resource-evidence
                                 :datahike.resource/work])))
           (is (every? (fn [value]
                         (or (number? value) (boolean? value)
                             (keyword? value) (map? value)))
                       (vals (:datahike.query/cache-evidence owner)))))))))

#?(:clj
   (deftest bounded-queries-share-completed-semantic-results
     (with-temp-db
       (conj label-schema {:c/id "bounded-cache" :c/note "value"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [database @conn
               calls (atom 0)
               predicate (fn [_] (swap! calls inc) true)
               query '[:find ?n
                       :in $ ?pred
                       :where [_ :c/note ?n]
                       [(?pred ?n)]]
               input {:query query
                      :args [database predicate]
                      :max-work 1000
                      :max-results 10
                      :max-result-weight 1000}
               bounded-owner (dq/q-with-evidence input)
               unbounded-hit (dq/q-with-evidence query database predicate)
               bounded-hit (dq/q-with-evidence input)]
           (is (= 1 @calls))
           (is (= #{["value"]}
                  (:datahike.query/result bounded-owner)
                  (:datahike.query/result unbounded-hit)
                  (:datahike.query/result bounded-hit)))
           (is (= :datahike.cache.outcome/miss-owner
                  (get-in bounded-owner [:datahike.query/cache-evidence
                                         :datahike.cache/outcome])))
           (is (= [:datahike.cache.outcome/hit
                   :datahike.cache.outcome/hit]
                  (mapv #(get-in % [:datahike.query/cache-evidence
                                    :datahike.cache/outcome])
                        [unbounded-hit bounded-hit])))
           (is (= 0 (get-in bounded-hit
                            [:datahike.query/resource-evidence
                             :datahike.resource/work])))
           (is (= {:datahike.resource/max-work 1000
                   :datahike.resource/max-results 10
                   :datahike.resource/max-result-weight 1000}
                  (get-in bounded-hit
                          [:datahike.query/resource-evidence
                           :datahike.resource/limits]))))))))

#?(:clj
   (deftest completed-result-is-certified-per-bounded-caller
     (with-temp-db
       (conj label-schema {:c/id "bounded-certify" :c/note "value"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [query '[:find ?n :where [_ :c/note ?n]]
               result (dq/q-with-evidence query @conn)]
           (is (= #{["value"]} (:datahike.query/result result)))
           (is (thrown-with-msg?
                clojure.lang.ExceptionInfo
                #"query-results budget exceeded"
                (dq/q-with-evidence {:query query
                                     :args [@conn]
                                     :max-results 0})))
           (is (= :datahike.cache.outcome/hit
                  (get-in (dq/q-with-evidence {:query query
                                               :args [@conn]
                                               :max-results 1})
                          [:datahike.query/cache-evidence
                           :datahike.cache/outcome]))))))))

#?(:clj
   (deftest different-cold-work-limits-do-not-share-failure
     (with-temp-db
       (conj label-schema {:c/id "bounded-flight" :c/note "value"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [start (java.util.concurrent.CountDownLatch. 1)
               predicate (fn [_] (Thread/sleep 50) true)
               query '[:find ?n
                       :in $ ?pred
                       :where [_ :c/note ?n]
                       [(?pred ?n)]]
               run (fn [max-work]
                     (.await start)
                     (try
                       (dq/q-with-evidence {:query query
                                            :args [@conn predicate]
                                            :request-id (str (random-uuid))
                                            :max-work max-work})
                       (catch clojure.lang.ExceptionInfo error error)))
               strict (future (run 1))
               generous (future (run 1000))]
           (.countDown start)
           (let [strict-result @strict
                 generous-result @generous]
             (is (:datahike/budget-exceeded (ex-data strict-result)))
             (is (= #{["value"]}
                    (:datahike.query/result generous-result)))
             (is (= :datahike.cache.outcome/hit
                    (get-in (dq/q-with-evidence {:query query
                                                 :args [@conn predicate]
                                                 :max-work 1})
                            [:datahike.query/cache-evidence
                             :datahike.cache/outcome])))))))))

#?(:clj
   (deftest concurrent-evidence-distinguishes-owner-and-joined-callers
     (with-temp-db
       (conj label-schema {:c/id "joined-evidence" :c/note "value"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [database @conn
               start (java.util.concurrent.CountDownLatch. 1)
               calls (atom 0)
               predicate (fn [_]
                           (swap! calls inc)
                           (Thread/sleep 75)
                           true)
               workers
               (doall
                (for [_ (range 8)]
                  (future
                    (.await start)
                    (dq/q-with-evidence
                     {:query '[:find ?n
                               :in $ ?predicate
                               :where [_ :c/note ?n]
                               [(?predicate ?n)]]
                      :args [database predicate]
                      :request-id (str (random-uuid))
                      :max-work 1000
                      :max-results 10
                      :max-result-weight 1000}))))]
           (.countDown start)
           (let [results (mapv deref workers)
                 outcomes (frequencies
                           (map #(get-in % [:datahike.query/cache-evidence
                                            :datahike.cache/outcome])
                                results))]
             (is (= 1 @calls))
             (is (= 1 (:datahike.cache.outcome/miss-owner outcomes)))
             (is (= 7 (:datahike.cache.outcome/miss-joined outcomes)))
             (is (= #{#{:c/note}}
                    (set (map :datahike.query/attribute-dependencies results))))
             (is (= #{["value"]}
                    (:datahike.query/result (first results))))
             (is (= #{:datahike.resource.scope/local-computation
                      :datahike.resource.scope/shared-computation}
                    (set (map #(get-in % [:datahike.query/resource-evidence
                                          :datahike.resource/scope])
                              results))))))))))

#?(:clj
   (deftest batched-writer-invalidates-the-union-of-request-attributes
     (let [cfg {:store {:backend :memory :id (random-uuid)}
                :writer {:backend :self :commit-wait-time 500}
                :schema-flexibility :write
                :attribute-refs? false}
           _ (d/create-database cfg)
           conn (d/connect cfg)]
       (try
         (d/transact conn (conj label-schema
                                {:c/id "batch" :c/note "before"
                                 :c/labels ["before"]}))
         (let [parent @conn]
           (is (= #{["before"]}
                  (d/q '[:find ?v :where [_ :c/note ?v]] parent)))
           (is (= #{["before"]}
                  (d/q '[:find ?v :where [_ :c/labels ?v]] parent)))
           ;; The previous commit's configured pause keeps the commit consumer
           ;; asleep while both independent requests reach its queue.
           (let [note-result (future
                               (d/transact conn [{:c/id "batch"
                                                  :c/note "after"}]))
                 label-result (future
                                (d/transact conn [{:c/id "batch"
                                                   :c/labels ["after"]}]))
                 note-report @note-result
                 label-report @label-result]
             (is (= (get-in note-report [:tx-meta :db/commitId])
                    (get-in label-report [:tx-meta :db/commitId]))
                 "the proof must exercise one physical writer batch")
             (let [committed @conn]
               (is (= #{["after"]}
                      (d/q '[:find ?v :where [_ :c/note ?v]] committed)))
               (is (= #{["before"] ["after"]}
                      (d/q '[:find ?v :where [_ :c/labels ?v]] committed))))))
         (finally
           (d/release conn)
           (d/delete-database cfg)
           (dq/clear-query-cache!))))))

#?(:clj
   (deftest unknown-batch-member-disables-cache-propagation
     (let [known {:db-after {:ref-ident-map nil}
                  :tx-data [{:a :c/note}]}
           unknown {:db-after {:ref-ident-map nil}
                    :tx-data []}]
       (is (= #{:c/note}
              (writing/batch-cache-propagation-attributes [known])))
       (is (nil?
            (writing/batch-cache-propagation-attributes [known unknown]))
           "one unknowable change must make the complete commit conservative"))))

(deftest test-pull-only-attr-retract-invalidates-cache
  (testing "Core bug: retract on attr only in pull pattern must invalidate cache"
    (with-temp-db label-schema
      (fn [conn]
        (d/transact conn [{:c/id "t1" :c/labels ["a" "b"]}])
        ;; Populate cache
        (d/q '[:find [(pull ?c [:c/id :c/labels]) ...]
               :where [?c :c/id]]
             @conn)
        ;; Retract one label
        (let [tx (d/transact conn [[:db/retract [:c/id "t1"] :c/labels "a"]])]
          (is (= ["b"]
                 (:c/labels (first (d/q '[:find [(pull ?c [:c/id :c/labels]) ...]
                                          :where [?c :c/id]]
                                        (:db-after tx)))))))))))

(deftest test-pull-only-attr-assert-invalidates-cache
  (testing "Adding a value to an attr only in pull pattern must show in results"
    (with-temp-db label-schema
      (fn [conn]
        (d/transact conn [{:c/id "t1" :c/labels ["a"]}])
        ;; Populate cache
        (d/q '[:find [(pull ?c [:c/id :c/labels]) ...]
               :where [?c :c/id]]
             @conn)
        ;; Add a label
        (let [tx (d/transact conn [{:c/id "t1" :c/labels ["b"]}])]
          (is (= #{"a" "b"}
                 (set (:c/labels (first (d/q '[:find [(pull ?c [:c/id :c/labels]) ...]
                                               :where [?c :c/id]]
                                             (:db-after tx))))))))))))

(deftest test-pull-only-attr-update-cardinality-one
  (testing "Updating a cardinality-one attr only in pull must show new value"
    (with-temp-db label-schema
      (fn [conn]
        (d/transact conn [{:c/id "t1" :c/note "old"}])
        ;; Populate cache
        (d/q '[:find [(pull ?c [:c/id :c/note]) ...]
               :where [?c :c/id]]
             @conn)
        ;; Update note
        (let [tx (d/transact conn [{:c/id "t1" :c/note "new"}])]
          (is (= "new"
                 (:c/note (first (d/q '[:find [(pull ?c [:c/id :c/note]) ...]
                                        :where [?c :c/id]]
                                      (:db-after tx)))))))))))

(deftest test-wildcard-pull-invalidates-on-any-change
  (testing "Wildcard pull [*] must invalidate when any attr changes"
    (with-temp-db label-schema
      (fn [conn]
        (d/transact conn [{:c/id "t1" :c/note "old"}])
        ;; Populate cache with wildcard pull
        (d/q '[:find [(pull ?c [*]) ...]
               :where [?c :c/id]]
             @conn)
        ;; Update note
        (let [tx (d/transact conn [{:c/id "t1" :c/note "new"}])]
          (is (= "new"
                 (:c/note (first (d/q '[:find [(pull ?c [*]) ...]
                                        :where [?c :c/id]]
                                      (:db-after tx)))))))))))

(deftest test-variable-pull-pattern-invalidates-on-any-change
  (testing "Variable pull pattern bound via :in must invalidate conservatively"
    (with-temp-db label-schema
      (fn [conn]
        (d/transact conn [{:c/id "t1" :c/note "old"}])
        ;; Populate cache with variable pattern
        (d/q '[:find [(pull ?c ?pattern) ...]
               :in $ ?pattern
               :where [?c :c/id]]
             @conn [:c/id :c/note])
        ;; Update note
        (let [tx (d/transact conn [{:c/id "t1" :c/note "new"}])]
          (is (= "new"
                 (:c/note (first (d/q '[:find [(pull ?c ?pattern) ...]
                                        :in $ ?pattern
                                        :where [?c :c/id]]
                                      (:db-after tx) [:c/id :c/note]))))))))))

(deftest test-where-attr-change-still-invalidates
  (testing "Changes to attrs in :where clauses still invalidate correctly"
    (with-temp-db label-schema
      (fn [conn]
        (d/transact conn [{:c/id "t1" :c/labels ["a" "b"]}
                          {:c/id "t2" :c/labels ["x"]}])
        ;; Query with :c/id in both :where and pull
        (is (= 2 (count (d/q '[:find [(pull ?c [:c/id :c/labels]) ...]
                               :where [?c :c/id]]
                             @conn))))
        ;; Retract an entity's :c/id — affects :where clause
        (let [tx (d/transact conn [[:db/retract [:c/id "t2"] :c/id "t2"]])]
          (is (= 1 (count (d/q '[:find [(pull ?c [:c/id :c/labels]) ...]
                                 :where [?c :c/id]]
                               (:db-after tx))))))))))

;; CLJ only: ClojureScript has no BigDecimal (`bigdec`), and the
;; scale-insensitivity collision the fix guards against cannot arise on JS
;; numbers — so there is nothing to test under CLJS.
#?(:clj
   (deftest test-bigdecimal-scale-not-collapsed-by-cache
     (testing "BigDecimals of equal value but different scale must not share a
            plan/result cache entry (Clojure = / hash are scale-insensitive:
            (= 1.50M 1.500M) => true with equal hash). Querying one scale must
            not make a later numerically-equal query return the first scale."
       (with-temp-db
         [{:db/ident :x/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}]
         (fn [conn]
           (let [db @conn
                 ;; const-only fn binding: value flows through verbatim, so the
                 ;; result's scale is exactly the input's scale.
                 run (fn [legacy? s]
                       (binding [dq/*disable-planner* legacy?]
                         (str (ffirst (d/q '{:find [?v] :in [$ ?p ?f] :where [[(?f ?p) ?v]]}
                                           db (bigdec s) identity)))))]
             (doseq [legacy? [true false]]
               (let [tag (str "force-legacy=" legacy?)]
                 ;; seed cache with one scale, then query equal-value other scales
                 (is (= "0.001" (run legacy? "0.001")) tag)
                 (is (= "0.001000" (run legacy? "0.001000"))
                     (str tag " — second query must keep its own scale, not the cached one"))
                 (is (= "1.5" (run legacy? "1.5")) tag)
                 (is (= "1.50" (run legacy? "1.50")) tag)
                 (is (= "1.500" (run legacy? "1.500")) tag)))))))))
