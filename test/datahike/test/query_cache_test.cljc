(ns datahike.test.query-cache-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   #?(:clj [datahike.db :as db])
   #?(:clj [datahike.db.utils :as dbu])
   #?(:clj [datahike.connections :as connections])
   #?(:clj [datahike.lru :as lru])
   #?(:clj [datahike.query :as dq])
   #?(:clj [datahike.store :as store])
   #?(:clj [datahike.writing :as writing])))

(defn- with-temp-db
  "Create a temp in-memory db with schema, run f with the connection, then clean up."
  ([schema-txs f]
   (with-temp-db {} schema-txs f))
  ([config schema-txs f]
   (let [cfg (merge {:store {:backend :memory :id (random-uuid)}
                     :schema-flexibility :write
                     :attribute-refs? false}
                    config)
         _ (d/create-database cfg)
         conn (d/connect cfg)]
     (try
       (d/transact conn schema-txs)
       (f conn)
       (finally
         (d/release conn)
         (d/delete-database cfg))))))

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
   (deftest direct-earlier-numeric-as-of-has-private-query-cache-identity
     (with-temp-db
       {:keep-history? true}
       (conj label-schema {:c/id "temporal-identity" :c/note "before"})
       (fn [conn]
         (let [earlier-t (:max-tx @conn)
               _ (d/transact conn [{:c/id "temporal-identity" :c/note "after"}])
               head @conn
               raw-key (db/committed-cache-identity head)
               temporal (d/as-of head earlier-t)
               speculative (:db-after
                            (d/with head [{:c/id "temporal-identity"
                                           :c/note "local"}]))
               detached (db/clear-cache-context head)]
           (is (= (conj raw-key earlier-t) (#'dq/db-cache-key temporal)))
           (is (nil? (db/committed-cache-identity temporal))
               "the public committed identity remains raw-only")
           (is (nil? (#'dq/db-cache-key
                      (d/as-of head (java.util.Date.)))))
           (is (nil? (#'dq/db-cache-key (d/as-of head (:max-tx head)))))
           (is (nil? (#'dq/db-cache-key (d/as-of head (inc (:max-tx head))))))
           (is (nil? (#'dq/db-cache-key (d/history temporal))))
           (is (nil? (#'dq/db-cache-key (d/history head))))
           (is (nil? (#'dq/db-cache-key (d/since head earlier-t))))
           (is (nil? (#'dq/db-cache-key (d/filter temporal (constantly true)))))
           (is (nil? (#'dq/db-cache-key (d/as-of speculative earlier-t))))
           (is (nil? (#'dq/db-cache-key (d/as-of detached earlier-t)))))))))

#?(:clj
   (deftest temporal-host-query-calls-share-thirty-two-callers-and-completed-hit
     (with-temp-db
       {:keep-history? true}
       (conj label-schema {:c/id "temporal-host" :c/note "before"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [earlier-t (:max-tx @conn)
               _ (d/transact conn [{:c/id "temporal-host" :c/note "after"}])
               temporal (d/as-of @conn earlier-t)
               predicate-calls (atom 0)
               predicate (fn [_] (swap! predicate-calls inc) true)
               query '[:find ?value
                       :in $ ?predicate
                       :where [_ :c/note ?value]
                       [(?predicate ?value)]]
               calls
               (mapv
                (fn [index]
                  (d/acquire-q!
                   {:query query
                    :args [temporal predicate]
                    :request-id (str "temporal-host-" index)}))
                (range 32))
               states (mapv d/q-call-state calls)
               completions (vec (repeatedly 32 promise))]
           (is (= 1 (count (filter #{:run} states))))
           (is (= 31 (count (filter #{:waiting} states))))
           (doseq [[call completion] (map vector calls completions)]
             (is (true? (d/on-q-complete! call #(deliver completion %)))))
           (let [owner (first (filter #(= :run (d/q-call-state %)) calls))]
             (is (= #{["before"]}
                    (:datahike.query/result (d/run-q! owner)))))
           (let [responses (mapv deref completions)
                 outcomes (frequencies
                           (map #(get-in % [:value
                                            :datahike.query/cache-evidence
                                            :datahike.cache/outcome])
                                responses))]
             (is (every? #(= :ok (:status %)) responses))
             (is (= 1 (:datahike.cache.outcome/miss-owner outcomes)))
             (is (= 31 (:datahike.cache.outcome/miss-joined outcomes))))
           (is (= 1 @predicate-calls))
           (let [hit (dq/q-with-evidence query temporal predicate)]
             (is (= #{["before"]} (:datahike.query/result hit)))
             (is (= :datahike.cache.outcome/hit
                    (get-in hit [:datahike.query/cache-evidence
                                 :datahike.cache/outcome])))
             (is (zero? (get-in hit [:datahike.query/resource-evidence
                                     :datahike.resource/work]))))
           (is (= 1 @predicate-calls))
           (is (zero? (:datahike.single-flight/active-flights
                       (d/query-cache-evidence))))
           (is (zero? (:datahike.single-flight/active-callers
                       (d/query-cache-evidence)))))))))

#?(:clj
   (deftest temporal-host-query-calls-share-two-callers
     (with-temp-db
       {:keep-history? true}
       (conj label-schema {:c/id "temporal-pair" :c/note "before"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [earlier-t (:max-tx @conn)
               _ (d/transact conn [{:c/id "temporal-pair" :c/note "after"}])
               temporal (d/as-of @conn earlier-t)
               predicate-calls (atom 0)
               predicate (fn [_] (swap! predicate-calls inc) true)
               query '[:find ?value
                       :in $ ?predicate
                       :where [_ :c/note ?value]
                       [(?predicate ?value)]]
               owner (d/acquire-q! {:query query :args [temporal predicate]
                                    :request-id "temporal-pair-owner"})
               waiter (d/acquire-q! {:query query :args [temporal predicate]
                                     :request-id "temporal-pair-waiter"})
               waiter-completion (promise)]
           (is (= :run (d/q-call-state owner)))
           (is (= :waiting (d/q-call-state waiter)))
           (d/on-q-complete! waiter #(deliver waiter-completion %))
           (is (= #{["before"]}
                  (:datahike.query/result (d/run-q! owner))))
           (is (= :datahike.cache.outcome/miss-joined
                  (get-in @waiter-completion
                          [:value :datahike.query/cache-evidence
                           :datahike.cache/outcome])))
           (is (= 1 @predicate-calls))
           (is (zero? (:datahike.single-flight/active-flights
                       (d/query-cache-evidence)))))))))

#?(:clj
   (deftest temporal-query-flight-identity-isolates-every-semantic-input
     (with-temp-db
       {:keep-history? true}
       (conj label-schema {:c/id "temporal-isolation" :c/note "zero"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [t0 (:max-tx @conn)
               _ (d/transact conn [{:c/id "temporal-isolation" :c/note "one"}])
               commit-a (d/commit-id @conn)
               t1 (:max-tx @conn)
               _ (d/transact conn [{:c/id "temporal-isolation" :c/note "two"}])
               head @conn
               commit-db (d/commit-as-db conn commit-a)
               t0-at-a (d/as-of commit-db t0)
               t0-at-b (d/as-of head t0)
               t1-at-b (d/as-of head t1)
               by-value '[:find ?value
                          :in $ ?expected
                          :where [_ :c/note ?value]
                          [(= ?value ?expected)]]
               specifications
               [{:query '[:find ?value :where [_ :c/note ?value]]
                 :args [t0-at-a] :request-id "temporal-commit-a"}
                {:query '[:find ?value :where [_ :c/note ?value]]
                 :args [t0-at-b] :request-id "temporal-commit-b"}
                {:query '[:find ?value :where [_ :c/note ?value]]
                 :args [t1-at-b] :request-id "temporal-t-one"}
                {:query '[:find ?id :where [?entity :c/id ?id]]
                 :args [t0-at-b] :request-id "temporal-query"}
                {:query by-value :args [t0-at-b "zero"]
                 :request-id "temporal-arg-zero"}
                {:query by-value :args [t0-at-b "missing"]
                 :request-id "temporal-arg-missing"}
                {:query '[:find ?value :where [_ :c/note ?value]]
                 :args [t0-at-b] :request-id "temporal-work-strict"
                 :max-work 10}
                {:query '[:find ?value :where [_ :c/note ?value]]
                 :args [t0-at-b] :request-id "temporal-work-generous"
                 :max-work 100}]
               calls (mapv d/acquire-q! specifications)]
           (try
             (is (every? #{:run} (map d/q-call-state calls))
                 "different t, commit, query, args, and limits own distinct flights")
             (is (not= (#'dq/db-cache-key t0-at-a)
                       (#'dq/db-cache-key t0-at-b)))
             (is (not= (#'dq/db-cache-key t0-at-b)
                       (#'dq/db-cache-key t1-at-b)))
             (finally
               (doseq [{:keys [request-id]} specifications]
                 (d/cancel-query! request-id))
               (d/release-materialized-db commit-db)))
           (is (zero? (:datahike.single-flight/active-flights
                       (d/query-cache-evidence))))
           (is (zero? (:datahike.single-flight/active-callers
                       (d/query-cache-evidence)))))))))

#?(:clj
   (deftest temporal-query-cancellation-and-clear-leave-no-flight
     (with-temp-db
       {:keep-history? true}
       (conj label-schema {:c/id "temporal-cancel" :c/note "before"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [earlier-t (:max-tx @conn)
               _ (d/transact conn [{:c/id "temporal-cancel" :c/note "after"}])
               temporal (d/as-of @conn earlier-t)
               query '[:find ?value :where [_ :c/note ?value]]
               owner-id "temporal-cancel-owner"
               waiter-id "temporal-cancel-waiter"
               owner (d/acquire-q! {:query query :args [temporal]
                                    :request-id owner-id})
               waiter (d/acquire-q! {:query query :args [temporal]
                                     :request-id waiter-id})]
           (is (= :run (d/q-call-state owner)))
           (is (= :waiting (d/q-call-state waiter)))
           (is (false? (:datahike.query.cancel/unstarted-owner?
                        (d/cancel-query! owner-id))))
           (let [cancelled (d/cancel-query! waiter-id)]
             (is (:datahike.query.cancel/unstarted-owner? cancelled))
             (is (= owner-id
                    (:datahike.query.cancel/owner-request-id cancelled))))
           (is (thrown-with-msg? clojure.lang.ExceptionInfo #"canceled"
                                 (d/run-q! owner)))
           (let [clear-owner (d/acquire-q!
                              {:query query :args [temporal]
                               :request-id "temporal-clear-owner"})
                 clear-waiter (d/acquire-q!
                               {:query query :args [temporal]
                                :request-id "temporal-clear-waiter"})
                 owner-completion (promise)
                 waiter-completion (promise)]
             (d/on-q-complete! clear-owner #(deliver owner-completion %))
             (d/on-q-complete! clear-waiter #(deliver waiter-completion %))
             (dq/clear-query-cache!)
             (is (= :datahike/query-coordinator-cleared
                    (:type (ex-data (:throwable @owner-completion)))))
             (is (= :datahike/query-coordinator-cleared
                    (:type (ex-data (:throwable @waiter-completion)))))
             (is (thrown-with-msg? clojure.lang.ExceptionInfo #"cleared"
                                   (d/run-q! clear-owner))))
           (is (zero? (:snapshot-count (dq/query-cache-metrics))))
           (is (zero? (:datahike.single-flight/active-flights
                       (d/query-cache-evidence))))
           (is (zero? (:datahike.single-flight/active-callers
                       (d/query-cache-evidence)))))))))

#?(:clj
   (deftest temporal-query-shares-failure-retries-and-bypasses-reentrancy
     (with-temp-db
       {:keep-history? true}
       (conj label-schema {:c/id "temporal-failure" :c/note "before"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [earlier-t (:max-tx @conn)
               _ (d/transact conn [{:c/id "temporal-failure" :c/note "after"}])
               temporal (d/as-of @conn earlier-t)
               fail? (atom true)
               predicate (fn [_]
                           (if (compare-and-set! fail? true false)
                             (throw (ex-info "shared temporal failure" {}))
                             true))
               query '[:find ?value
                       :in $ ?predicate
                       :where [_ :c/note ?value]
                       [(?predicate ?value)]]
               owner (d/acquire-q! {:query query :args [temporal predicate]
                                    :request-id "temporal-failure-owner"})
               waiter (d/acquire-q! {:query query :args [temporal predicate]
                                     :request-id "temporal-failure-waiter"})
               owner-completion (promise)
               waiter-completion (promise)]
           (d/on-q-complete! owner #(deliver owner-completion %))
           (d/on-q-complete! waiter #(deliver waiter-completion %))
           (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                 #"shared temporal failure"
                                 (d/run-q! owner)))
           (is (= "shared temporal failure"
                  (.getMessage (:throwable @owner-completion))))
           (is (= "shared temporal failure"
                  (.getMessage (:throwable @waiter-completion))))
           (let [retry (d/acquire-q!
                        {:query query :args [temporal predicate]
                         :request-id "temporal-failure-retry"})]
             (is (= :run (d/q-call-state retry)))
             (is (= #{["before"]}
                    (:datahike.query/result (d/run-q! retry)))))
           (dq/clear-query-cache!)
           (let [entered? (atom false)
                 reentrant-predicate (atom nil)
                 reentrant-query '[:find ?value
                                   :in $ ?predicate
                                   :where [_ :c/note ?value]
                                   [(?predicate ?value)]]]
             (reset! reentrant-predicate
                     (fn [_]
                       (when (compare-and-set! entered? false true)
                         (d/q reentrant-query temporal @reentrant-predicate))
                       true))
             (is (= #{["before"]}
                    (d/q reentrant-query temporal @reentrant-predicate)))
             (is (true? @entered?)))
           (is (zero? (:datahike.single-flight/active-flights
                       (d/query-cache-evidence))))
           (is (zero? (:datahike.single-flight/active-callers
                       (d/query-cache-evidence)))))))))

#?(:clj
   (deftest temporal-completed-result-is-certified-per-caller
     (with-temp-db
       {:keep-history? true}
       (conj label-schema {:c/id "temporal-budget" :c/note "before"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [earlier-t (:max-tx @conn)
               _ (d/transact conn [{:c/id "temporal-budget" :c/note "after"}])
               temporal (d/as-of @conn earlier-t)
               query '[:find ?value :where [_ :c/note ?value]]
               owner (dq/q-with-evidence query temporal)]
           (is (= #{["before"]} (:datahike.query/result owner)))
           (is (thrown-with-msg?
                clojure.lang.ExceptionInfo
                #"query-results budget exceeded"
                (dq/q-with-evidence {:query query
                                     :args [temporal]
                                     :max-results 0})))
           (let [hit (dq/q-with-evidence {:query query
                                          :args [temporal]
                                          :max-work 1
                                          :max-results 1
                                          :max-result-weight 10})]
             (is (= :datahike.cache.outcome/hit
                    (get-in hit [:datahike.query/cache-evidence
                                 :datahike.cache/outcome])))
             (is (zero? (get-in hit [:datahike.query/resource-evidence
                                     :datahike.resource/work])))))))))

#?(:clj
   (deftest temporal-generation-release-and-reconnect-fence-results-and-flights
     (let [cfg {:store {:backend :memory :id (random-uuid)}
                :keep-history? true
                :schema-flexibility :write
                :attribute-refs? false}
           _ (d/create-database cfg)]
       (try
         (dq/clear-query-cache!)
         (let [conn (d/connect cfg)]
           (d/transact conn (conj label-schema
                                  {:c/id "temporal-generation"
                                   :c/note "before"}))
           (let [earlier-t (:max-tx @conn)
                 _ (d/transact conn [{:c/id "temporal-generation"
                                      :c/note "after"}])
                 database @conn
                 [connection-id generation-before _]
                 (db/committed-cache-identity database)
                 temporal (d/as-of database earlier-t)
                 query '[:find ?value :where [_ :c/note ?value]]
                 owner (d/acquire-q! {:query query :args [temporal]
                                      :request-id "temporal-release-owner"})
                 waiter (d/acquire-q! {:query query :args [temporal]
                                       :request-id "temporal-release-waiter"})
                 owner-completion (promise)
                 waiter-completion (promise)]
             (d/on-q-complete! owner #(deliver owner-completion %))
             (d/on-q-complete! waiter #(deliver waiter-completion %))
             (d/release conn)
             (is (= :datahike/query-cache-scope-closed
                    (:type (ex-data (:throwable @owner-completion)))))
             (is (= :datahike/query-cache-scope-closed
                    (:type (ex-data (:throwable @waiter-completion)))))
             (is (thrown-with-msg? clojure.lang.ExceptionInfo #"scope closed"
                                   (d/run-q! owner)))
             (is (zero? (:snapshot-count (dq/query-cache-metrics))))
             (is (zero? (:datahike.single-flight/active-flights
                         (d/query-cache-evidence))))
             (let [reconnected (d/connect cfg)
                   [_ generation-after _]
                   (db/committed-cache-identity @reconnected)
                   reopened (d/as-of @reconnected earlier-t)
                   response (dq/q-with-evidence query reopened)]
               (is (= connection-id
                      (first (db/committed-cache-identity @reconnected))))
               (is (not= generation-before generation-after))
               (is (= #{["before"]} (:datahike.query/result response)))
               (is (= :datahike.cache.outcome/miss-owner
                      (get-in response [:datahike.query/cache-evidence
                                        :datahike.cache/outcome])))
               (is (pos? (:snapshot-count (dq/query-cache-metrics))))
               (d/release reconnected)
               (is (zero? (:snapshot-count (dq/query-cache-metrics)))))))
         (finally
           (d/delete-database cfg)
           (dq/clear-query-cache!))))))

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
             (#'dq/result-cache-put!
              committed ::late #{["stale"]} #{:c/note}
              {(db/committed-cache-identity committed)
               (:cache-context committed)}
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
                (#'dq/result-cache-put!
                 database ::late #{["stale"]} #{:c/note}
                 {(db/committed-cache-identity database)
                  (:cache-context database)}
                 admitted-epoch)))
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
           (is (= {:datahike.query.dependency/sources
                   [{:datahike.query.source/symbol '$
                     :datahike.query.source/argument-position 0
                     :datahike.query.source/attributes #{:c/note}}]}
                  (:datahike.read/dependency-plan owner)
                  (:datahike.read/dependency-plan hit)))
           (is (= #{:c/note}
                  (:datahike.query/attribute-dependencies
                   (with-redefs [dq/query-dependency-plan
                                 (fn [& _]
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
   (deftest input-bound-attribute-plan-drives-cache-inheritance
     (with-temp-db
       (conj label-schema {:c/id "input-plan" :c/note "value"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [query '[:find ?value
                       :in $ ?attribute
                       :where [_ ?attribute ?value]]
               initial (dq/q-with-evidence query @conn :c/note)
               unrelated (d/transact conn
                                     [{:c/id "input-plan"
                                       :c/labels ["other"]}])
               inherited (dq/q-with-evidence
                          query (:db-after unrelated) :c/note)]
           (is (= #{["value"]} (:datahike.query/result initial)
                  (:datahike.query/result inherited)))
           (is (= #{:c/note}
                  (:datahike.query.source/attributes
                   (first (get-in initial
                                  [:datahike.read/dependency-plan
                                   :datahike.query.dependency/sources])))))
           (is (= :datahike.cache.outcome/hit
                  (get-in inherited [:datahike.query/cache-evidence
                                     :datahike.cache/outcome]))))))))

#?(:clj
   (deftest ordinary-symbol-inputs-remain-ground-through-planning-and-caching
     (with-temp-db
       [{:db/ident :ordinary.namespace/name
         :db/valueType :db.type/symbol
         :db/unique :db.unique/identity
         :db/cardinality :db.cardinality/one}
        {:db/ident :ordinary.agent/namespace
         :db/valueType :db.type/ref
         :db/cardinality :db.cardinality/one}
        {:db/ident :ordinary.agent/id
         :db/valueType :db.type/string
         :db/unique :db.unique/identity
         :db/cardinality :db.cardinality/one}
        {:db/ident :ordinary.agent/terminated-at
         :db/valueType :db.type/instant
         :db/cardinality :db.cardinality/one}
        {:db/ident :ordinary.agent/state
         :db/valueType :db.type/symbol
         :db/cardinality :db.cardinality/one}
        {:ordinary.namespace/name 'ordinary.namespace.known}
        {:ordinary.agent/id "ordinary-agent"
         :ordinary.agent/state 'ordinary.state/active
         :ordinary.agent/namespace
         [:ordinary.namespace/name 'ordinary.namespace.known]}]
       (fn [conn]
         (dq/clear-query-cache!)
         (let [query '[:find ?id .
                       :in $ ?name
                       :where [?namespace :ordinary.namespace/name ?name]
                              [?agent :ordinary.agent/namespace ?namespace]
                              [?agent :ordinary.agent/id ?id]
                              (not [?agent :ordinary.agent/terminated-at _])]
               state-query
               '[:find ?id .
                 :in $ ?state
                 :where [?agent :ordinary.agent/state ?state]
                        [?agent :ordinary.agent/id ?id]]
               database @conn]
           (is (= "ordinary-agent"
                  (d/q query database 'ordinary.namespace.known)))
           (is (nil? (d/q query database 'ordinary.namespace.missing))
               "a completed query and cached plan must not retain a prior :in value")
           (is (= "ordinary-agent"
                  (d/q query database 'ordinary.namespace.known))
               "the original ordinary input remains independently cacheable")
           (is (= "ordinary-agent"
                  (d/q state-query database 'ordinary.state/active)))
           (is (nil? (d/q state-query database 'ordinary.state/missing))
               "a non-indexed symbol value is a ground scan constraint")
           (doseq [[candidate-query candidate]
                   [[query 'ordinary.namespace.known]
                    [query 'ordinary.namespace.missing]
                    [state-query 'ordinary.state/active]
                    [state-query 'ordinary.state/missing]]]
             (is (= (binding [dq/*disable-planner* true]
                      (d/q candidate-query database candidate))
                    (binding [dq/*disable-planner* false]
                      (d/q candidate-query database candidate)))
                 (str "planned execution must preserve legacy symbol-value semantics for "
                      candidate " in " candidate-query))))))))

#?(:clj
   (deftest write-only-transactions-do-not-create-query-cache-snapshots
     (with-temp-db
       (conj label-schema {:c/id "lazy-cache" :c/note "stable"})
       (fn [conn]
         (dq/clear-query-cache!)
         (is (= #{["stable"]}
                (d/q '[:find ?value :where [_ :c/note ?value]] @conn)))
         (let [before (dq/query-cache-metrics)]
           (dotimes [index 100]
             (d/transact conn
                         [{:c/id "lazy-cache"
                           :c/labels [(str "write-" index)]}]))
           (is (= (:snapshot-count before)
                  (:snapshot-count (dq/query-cache-metrics)))
               "commits without cache demand must not manufacture cache rows")
           (is (= (:total-weight before)
                  (:total-weight (dq/query-cache-metrics)))
               "commits without cache demand must not retain result weight")
           (is (= #{:c/labels}
                  (set (keys (get-in @conn
                                     [:cache-context
                                      :datahike.cache/attribute-revisions]))))
               "revision state is bounded by changed attributes, not commits")
           (let [demanded (dq/q-with-evidence
                           '[:find ?value :where [_ :c/note ?value]] @conn)
                 after-demand (dq/query-cache-metrics)]
             (is (= #{["stable"]} (:datahike.query/result demanded)))
             (is (= :datahike.cache.outcome/hit
                    (get-in demanded [:datahike.query/cache-evidence
                                      :datahike.cache/outcome])))
             (is (zero? (get-in demanded [:datahike.query/resource-evidence
                                          :datahike.resource/work]))
                 "final demand must inherit without executing the query")
             (is (= (inc (:snapshot-count before))
                    (:snapshot-count after-demand))
                 "only the demanded immutable database value gains a row")))))))

#?(:clj
   (deftest affected-transaction-recomputes-only-when-the-child-is-demanded
     (with-temp-db
       (conj label-schema {:c/id "lazy-affected" :c/note "before"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [query '[:find ?value :where [_ :c/note ?value]]
               parent @conn
               initial (dq/q-with-evidence query parent)
               before-commit (dq/query-cache-metrics)
               report (d/transact conn [{:c/id "lazy-affected"
                                         :c/note "after"}])]
           (is (= before-commit (dq/query-cache-metrics))
               "even an affected commit must not mutate cached result rows")
           (let [child (dq/q-with-evidence query (:db-after report))]
             (is (= #{["after"]} (:datahike.query/result child)))
             (is (= :datahike.cache.outcome/miss-owner
                    (get-in child [:datahike.query/cache-evidence
                                   :datahike.cache/outcome]))))
           (is (= #{["before"]} (d/q query parent))
               "the held immutable parent retains its exact cached value"))))))

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
   (deftest host-query-calls-complete-thirty-two-callers
     (with-temp-db
       [{:db/ident :host/value
         :db/valueType :db.type/long
         :db/cardinality :db.cardinality/one}
        {:host/value 42}]
       (fn [conn]
         (dq/clear-query-cache!)
         (let [predicate-calls (atom 0)
               predicate (fn [_] (swap! predicate-calls inc) true)
               query '[:find ?value
                       :in $ ?predicate
                       :where [_ :host/value ?value]
                       [(?predicate ?value)]]
               calls
               (mapv
                (fn [index]
                  (d/acquire-q!
                   {:query query
                    :args [@conn predicate]
                    :request-id (str "host-call-" index)}))
                (range 32))
               states (mapv d/q-call-state calls)
               completions (vec (repeatedly 32 promise))]
           (is (= 1 (count (filter #{:run} states))))
           (is (= 31 (count (filter #{:waiting} states))))
           (doseq [[call completion] (map vector calls completions)]
             (is (true? (d/on-q-complete! call #(deliver completion %)))))
           (let [owner (first (filter #(= :run (d/q-call-state %)) calls))]
             (is (= #{[42]} (:datahike.query/result (d/run-q! owner)))))
           (let [responses (mapv deref completions)
                 outcomes
                 (mapv #(get-in % [:value :datahike.query/cache-evidence
                                   :datahike.cache/outcome])
                       responses)]
             (is (every? #(= :ok (:status %)) responses))
             (is (= 1 (count (filter #{:datahike.cache.outcome/miss-owner}
                                     outcomes))))
             (is (= 31 (count (filter #{:datahike.cache.outcome/miss-joined}
                                      outcomes)))))
           (is (= 1 @predicate-calls))
           (is (zero? (:datahike.single-flight/active-flights
                       (d/query-cache-evidence))))
           (is (zero? (:datahike.single-flight/active-callers
                       (d/query-cache-evidence)))))))))

#?(:clj
   (deftest host-query-call-release-reopen-fences-stale-owner
     (with-temp-db
       [{:db/ident :host.release/value
         :db/valueType :db.type/long
         :db/cardinality :db.cardinality/one}
        {:host.release/value 7}]
       (fn [conn]
         (dq/clear-query-cache!)
         (let [database @conn
               {:datahike.value/keys [connection-id generation]}
               (d/committed-value-identity database)
               query '[:find ?value :where [_ :host.release/value ?value]]
               old (d/acquire-q!
                    {:query query :args [database]
                     :request-id "host-release-old"})
               old-completion (promise)]
           (is (= :run (d/q-call-state old)))
           (d/on-q-complete! old #(deliver old-completion %))
           (d/close-query-cache-generation! connection-id generation)
           (is (= :datahike/query-cache-scope-closed
                  (:type (ex-data (:throwable @old-completion)))))
           (dq/open-query-cache-generation! connection-id generation)
           (let [successor
                 (d/acquire-q!
                  {:query query :args [database]
                   :request-id "host-release-successor"})]
             (is (= :run (d/q-call-state successor)))
             (is (thrown-with-msg? clojure.lang.ExceptionInfo #"scope closed"
                                   (d/run-q! old)))
             (is (= #{[7]}
                    (:datahike.query/result (d/run-q! successor))))
             (is (zero? (:datahike.single-flight/active-flights
                         (d/query-cache-evidence))))))))))

#?(:clj
   (deftest final-host-cancel-names-the-unstarted-owner-job
     (with-temp-db
       [{:db/ident :host.cancel/value
         :db/valueType :db.type/long
         :db/cardinality :db.cardinality/one}
        {:host.cancel/value 8}]
       (fn [conn]
         (dq/clear-query-cache!)
         (let [query '[:find ?value :where [_ :host.cancel/value ?value]]
               owner-id "host-cancel-owner"
               waiter-id "host-cancel-waiter"
               owner (d/acquire-q! {:query query :args [@conn]
                                    :request-id owner-id})
               waiter (d/acquire-q! {:query query :args [@conn]
                                     :request-id waiter-id})]
           (is (= :run (d/q-call-state owner)))
           (is (= :waiting (d/q-call-state waiter)))
           (is (false? (:datahike.query.cancel/unstarted-owner?
                        (d/cancel-query! owner-id))))
           (let [cancelled (d/cancel-query! waiter-id)]
             (is (:datahike.query.cancel/unstarted-owner? cancelled))
             (is (= owner-id
                    (:datahike.query.cancel/owner-request-id cancelled))))
           (is (thrown-with-msg? clojure.lang.ExceptionInfo #"canceled"
                                 (d/run-q! owner)))
           (is (zero? (:datahike.single-flight/active-flights
                       (d/query-cache-evidence)))))))))

#?(:clj
   (deftest host-query-call-shares-failure-and-retries
     (with-temp-db
       [{:db/ident :host.failure/value
         :db/valueType :db.type/long
         :db/cardinality :db.cardinality/one}
        {:host.failure/value 9}]
       (fn [conn]
         (dq/clear-query-cache!)
         (let [fail? (atom true)
               predicate
               (fn [_]
                 (if (compare-and-set! fail? true false)
                   (throw (ex-info "shared host failure" {}))
                   true))
               query '[:find ?value
                       :in $ ?predicate
                       :where [_ :host.failure/value ?value]
                       [(?predicate ?value)]]
               owner
               (d/acquire-q!
                {:query query :args [@conn predicate]
                 :request-id "host-failure-owner"
                 :max-work 100})
               waiter
               (d/acquire-q!
                {:query query :args [@conn predicate]
                 :request-id "host-failure-waiter"
                 :max-work 100})
               owner-result (promise)
               waiter-result (promise)]
           (d/on-q-complete! owner #(deliver owner-result %))
           (d/on-q-complete! waiter #(deliver waiter-result %))
           (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                 #"shared host failure"
                                 (d/run-q! owner)))
           (is (= "shared host failure"
                  (.getMessage (:throwable @owner-result))))
           (is (= "shared host failure"
                  (.getMessage (:throwable @waiter-result))))
           (let [retry
                 (d/acquire-q!
                  {:query query :args [@conn predicate]
                   :request-id "host-failure-retry"
                   :max-work 100})
                 response (d/run-q! retry)]
             (is (= #{[9]} (:datahike.query/result response)))
             (is (pos? (get-in response [:datahike.query/resource-evidence
                                         :datahike.resource/work])))
             (is (= {:datahike.resource/max-work 100}
                    (get-in response [:datahike.query/resource-evidence
                                      :datahike.resource/limits]))))
           (is (zero? (:datahike.single-flight/active-flights
                       (d/query-cache-evidence)))))))))

#?(:clj
   (deftest host-direct-query-preserves-resource-evidence
     (with-temp-db
       [{:db/ident :host.direct/value
         :db/valueType :db.type/long
         :db/cardinality :db.cardinality/one}
        {:host.direct/value 11}]
       (fn [conn]
         (let [call (binding [dq/*query-result-cache?* false]
                      (d/acquire-q!
                       {:query '[:find ?value
                                 :where [_ :host.direct/value ?value]]
                        :args [@conn]
                        :request-id "host-direct"}))
               response (d/run-q! call)]
           (is (= :run (d/q-call-state call)))
           (is (= #{[11]} (:datahike.query/result response)))
           (is (pos? (get-in response [:datahike.query/resource-evidence
                                       :datahike.resource/work]))))))))

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
   (deftest unknown-batch-member-advances-the-conservative-cache-revision
     (let [known {:db-after {:ref-ident-map nil}
                  :tx-data [{:a :c/note}]}
           unknown {:db-after {:ref-ident-map nil}
                    :tx-data []}]
       (is (= #{:c/note}
              (writing/batch-cache-revision-attributes [known])))
       (is (nil?
            (writing/batch-cache-revision-attributes [known unknown]))
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

#?(:clj
   (deftest schema-changing-transactions-do-not-inherit-query-results
     (with-temp-db
       (conj label-schema {:c/id "schema-cache" :c/note "value"})
       (fn [conn]
         (dq/clear-query-cache!)
         (let [query '[:find ?value :where [_ :c/note ?value]]
               initial (dq/q-with-evidence query @conn)
               schema-change
               (d/transact conn
                           [{:db/ident :c/new-value
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one}])
               after (dq/q-with-evidence query (:db-after schema-change))]
           (is (= #{["value"]} (:datahike.query/result initial)
                  (:datahike.query/result after)))
           (is (= :datahike.cache.outcome/miss-owner
                  (get-in after [:datahike.query/cache-evidence
                                 :datahike.cache/outcome]))))))))

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

#?(:clj
   (deftest parsed-query-source-bindings-name-only-top-level-source-arguments
     (is (= 1 (d/query-input-count
               '[:find ?value :in ?value :where [(identity ?value)]])))
     (is (= [] (d/query-source-bindings
                '[:find ?value :in ?value :where [(identity ?value)]])))
     (is (= 1 (d/query-input-count
               '[:find ?value :where [_ :c/note ?value]])))
     (is (= [{:datahike.query.source/symbol '$
              :datahike.query.source/argument-position 0}]
            (d/query-source-bindings
             '[:find ?value :where [_ :c/note ?value]])))
     (is (= [{:datahike.query.source/symbol '$a
              :datahike.query.source/argument-position 0}
             {:datahike.query.source/symbol '$b
              :datahike.query.source/argument-position 2}
             {:datahike.query.source/symbol '$c
              :datahike.query.source/argument-position 3}]
            (d/query-source-bindings
             '[:find ?value
               :in $a ?ordinary $b $c
               :where [$a _ :c/note ?value]])))
     (is (= 4 (d/query-input-count
               '[:find ?value
                 :in $a ?ordinary $b $c
                 :where [$a _ :c/note ?value]])))
     (is (= [{:datahike.query.source/symbol '$a
              :datahike.query.source/argument-position 0}]
            (d/query-source-bindings
             '[:find ?ordinary
               :in $a [?ordinary ...]
               :where [(identity ?ordinary)]])))))

#?(:clj
   (deftest three-source-query-cache-has-one-composite-ordinary-key-and-all-member-lifetime
     (with-temp-db
       (conj label-schema {:c/id "a" :c/note "A"})
       (fn [a]
         (with-temp-db
           (conj label-schema {:c/id "b" :c/note "B"})
           (fn [b]
             (with-temp-db
               (conj label-schema {:c/id "c" :c/note "C"})
               (fn [c]
                 (dq/clear-query-cache!)
                 (let [query '[:find ?a ?b ?c
                               :in $a $b $c
                               :where
                               [$a _ :c/note ?a]
                               [$b _ :c/note ?b]
                               [$c _ :c/note ?c]]
                       first-result (dq/q-with-evidence query @a @b @c)
                       second-result (dq/q-with-evidence query @a @b @c)
                       cache-keys (query-cache-keys)
                       cache-entries
                       (lru/weighted-entries (:lru @dq/query-result-cache))
                       [_ connection-b generation-b]
                       (let [[connection generation _]
                             (db/committed-cache-identity @b)]
                         [nil connection generation])]
                   (is (= #{["A" "B" "C"]}
                          (:datahike.query/result first-result)))
                   (is (= :datahike.cache.outcome/miss-owner
                          (get-in first-result
                                  [:datahike.query/cache-evidence
                                   :datahike.cache/outcome])))
                   (is (= :datahike.cache.outcome/hit
                          (get-in second-result
                                  [:datahike.query/cache-evidence
                                   :datahike.cache/outcome])))
                   (is (= 1 (count cache-keys)))
                   (is (not-any? dbu/db?
                                 (tree-seq coll? seq cache-entries)))
                   (d/close-query-cache-generation! connection-b generation-b)
                   (is (zero? (:snapshot-count (dq/query-cache-metrics)))))))))))))

#?(:clj
   (deftest composite-source-cache-inherits-one-advanced-source-conservatively
     (with-temp-db
       (conj label-schema {:c/id "left" :c/note "L"})
       (fn [left]
         (with-temp-db
           (conj label-schema {:c/id "right" :c/note "R"})
           (fn [right]
             (dq/clear-query-cache!)
             (let [query '[:find ?left ?right
                           :in $left $right
                           :where
                           [$left _ :c/note ?left]
                           [$right _ :c/note ?right]]
                   initial (dq/q-with-evidence query @left @right)
                   unrelated (d/transact right
                                         [{:c/id "right"
                                           :c/labels ["unrelated"]}])
                   inherited (dq/q-with-evidence
                              query @left (:db-after unrelated))
                   changed (d/transact right
                                       [{:c/id "right" :c/note "R2"}])
                   recomputed (dq/q-with-evidence
                               query @left (:db-after changed))]
               (is (= #{["L" "R"]} (:datahike.query/result initial)))
               (is (= :datahike.cache.outcome/hit
                      (get-in inherited [:datahike.query/cache-evidence
                                         :datahike.cache/outcome])))
               (is (= #{["L" "R2"]} (:datahike.query/result recomputed)))
               (is (= :datahike.cache.outcome/miss-owner
                      (get-in recomputed [:datahike.query/cache-evidence
                                          :datahike.cache/outcome]))))))))))

#?(:clj
   (deftest composite-cache-inheritance-checks-only-the-advanced-source-plan
     (with-temp-db
       (conj label-schema {:c/id "left" :c/note "L"})
       (fn [left]
         (with-temp-db
           (conj label-schema
                 {:c/id "right" :c/note "R" :c/labels ["x"]})
           (fn [right]
             (dq/clear-query-cache!)
             (let [query '[:find ?left ?label
                           :in $left $right
                           :where
                           [$left _ :c/note ?left]
                           [$right _ :c/labels ?label]]
                   initial (dq/q-with-evidence query @left @right)
                   other-source-attribute
                   (d/transact right [{:c/id "right" :c/note "R2"}])
                   inherited
                   (dq/q-with-evidence query @left
                                       (:db-after other-source-attribute))
                   selected-source-attribute
                   (d/transact right [{:c/id "right" :c/labels ["y"]}])
                   recomputed
                   (dq/q-with-evidence query @left
                                       (:db-after selected-source-attribute))]
               (is (= #{["L" "x"]} (:datahike.query/result initial)))
               (is (= :datahike.cache.outcome/hit
                      (get-in inherited [:datahike.query/cache-evidence
                                         :datahike.cache/outcome])))
               (is (= #{["L" "x"] ["L" "y"]}
                      (:datahike.query/result recomputed)))
               (is (= :datahike.cache.outcome/miss-owner
                      (get-in recomputed [:datahike.query/cache-evidence
                                          :datahike.cache/outcome]))))))))))

#?(:clj
   (deftest duplicate-source-members-share-one-metric-and-nonprimary-close-detaches-flight
     (with-temp-db
       (conj label-schema {:c/id "left" :c/note "L"})
       (fn [left]
         (with-temp-db
           (conj label-schema {:c/id "right" :c/note "R"})
           (fn [right]
             (dq/clear-query-cache!)
             (let [duplicate-query '[:find ?a ?b
                                     :in $a $b
                                     :where
                                     [$a _ :c/note ?a]
                                     [$b _ :c/note ?b]]
                   duplicate (d/acquire-q!
                              {:query duplicate-query
                               :args [@left @left]
                               :request-id "duplicate-source"})
                   left-key (db/committed-cache-identity @left)
                   metrics (dq/query-cache-metrics)]
               (is (= :run (d/q-call-state duplicate)))
               (is (= 1 (count (get-in metrics
                                       [:single-flight :active-by-database]))))
               (is (= 1 (get-in metrics
                                [:single-flight :active-by-database
                                 left-key :flights])))
               (is (:datahike.query.cancel/detached?
                    (d/cancel-query! "duplicate-source")))
               (is (zero? (get-in (dq/query-cache-metrics)
                                  [:single-flight :active-flights]))))
             (let [query '[:find ?left ?right
                           :in $left $right
                           :where
                           [$left _ :c/note ?left]
                           [$right _ :c/note ?right]]
                   owner (d/acquire-q! {:query query :args [@left @right]
                                        :request-id "composite-owner"})
                   waiter (d/acquire-q! {:query query :args [@left @right]
                                         :request-id "composite-waiter"})
                   owner-result (promise)
                   waiter-result (promise)
                   [connection-id generation _]
                   (db/committed-cache-identity @right)]
               (d/on-q-complete! owner #(deliver owner-result %))
               (d/on-q-complete! waiter #(deliver waiter-result %))
               (d/close-query-cache-generation! connection-id generation)
               (is (= :datahike/query-cache-scope-closed
                      (:type (ex-data (:throwable @owner-result)))))
               (is (= :datahike/query-cache-scope-closed
                      (:type (ex-data (:throwable @waiter-result)))))
               (is (zero? (get-in (dq/query-cache-metrics)
                                  [:single-flight :active-flights]))))))))))
