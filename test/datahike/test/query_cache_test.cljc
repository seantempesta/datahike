(ns datahike.test.query-cache-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   #?(:clj [datahike.db :as db])
   #?(:clj [datahike.query :as dq])))

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
                 [_ generation-before _] (db/committed-cache-identity committed)]
             (d/q '[:find ?n :where [_ :c/note ?n]] committed)
             (is (pos? (:snapshot-count (dq/query-cache-metrics))))
             (d/release conn)
             (is (zero? (:snapshot-count (dq/query-cache-metrics))))
             (#'dq/result-cache-put! committed ::late #{["stale"]} #{:c/note})
             (is (zero? (:snapshot-count (dq/query-cache-metrics)))
                 "a query finishing after release cannot resurrect its generation")
             (let [reconnected (d/connect cfg)
                   [_ generation-after _] (db/committed-cache-identity @reconnected)]
               (is (not= generation-before generation-after))
               (d/release reconnected))))
         (finally
           (d/delete-database cfg)
           (dq/clear-query-cache!))))))

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
