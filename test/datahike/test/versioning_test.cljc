(ns datahike.test.versioning-test
  (:require #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
               :clj  [clojure.test :as t :refer [is deftest testing]])
            [datahike.versioning :refer
             [branches branch-history branch! delete-branch! merge! force-branch! branch-as-db parent-commit-ids
              commit-id commit-as-db]]
            [datahike.constants :as const]
            [datahike.db.utils :refer [db?]]
            [datahike.writing :as writing]
            [datahike.gc-guard :as guard]
            [datahike.api :as d]
            #?(:clj [clojure.core.async :as async])
            [konserve.core :as k]
            [superv.async :refer [<?? S]]))

#?(:clj
   (deftest release-materialized-db-is-owned-and-idempotent
     (let [closed (atom 0)
           closeable (reify java.io.Closeable
                       (close [_] (swap! closed inc)))
           materialized {:datahike.db/materialized-secondary-indices? true
                         :datahike.db/released? (atom false)
                         :secondary-indices {:test/index closeable}}
           ordinary {:secondary-indices {:test/index closeable}}]
       (is (= [] (writing/release-db ordinary)))
       (is (= 0 @closed))
       (is (= [] (writing/release-db materialized)))
       (is (= 1 @closed))
       (is (= [] (writing/release-db materialized)))
       (is (= 1 @closed)))))

#?(:clj
   (deftest concurrent-branches-from-separate-connections-stay-discoverable
     (let [cfg {:store {:backend :file
                        :path (str "target/concurrent-branches-" (random-uuid))
                        :id (random-uuid)}
                :keep-history? true
                :schema-flexibility :read}
           _ (d/create-database cfg)
           base (d/connect cfg)]
       (try
         (branch! base :db :left)
         (branch! base :db :right)
         (let [left (d/connect (assoc cfg :branch :left))
               right (d/connect (assoc cfg :branch :right))]
           (try
             (is (not (identical? (:store @left) (:store @right))))
             (let [branch-count 32
                   start (java.util.concurrent.CountDownLatch. 1)
                   futures
                   (mapv (fn [i]
                           (future
                             (.await start)
                             (let [branch (keyword (str "concurrent-" i))]
                               (try
                                 (branch! (if (even? i) left right) :db branch)
                                 {:branch branch :ok? true}
                                 (catch Throwable error
                                   {:branch branch :ok? false :error error})))))
                         (range branch-count))]
               (.countDown start)
               (let [outcomes (mapv deref futures)
                     successful (into #{} (comp (filter :ok?) (map :branch)) outcomes)
                     roster (set (branches left))
                     readable
                     (into #{}
                           (keep (fn [branch]
                                   (try
                                     (let [connection (d/connect (assoc cfg :branch branch))]
                                       (try
                                         @connection
                                         branch
                                         (finally
                                           (d/release connection))))
                                     (catch Throwable _
                                       nil))))
                           successful)]
                 (is (every? :ok? outcomes) (pr-str outcomes))
                 (is (every? roster successful)
                     (str "Successful branches missing from roster: "
                          (pr-str (remove roster successful))))
                 (is (= successful readable)
                     (str "Successful branches not readable: "
                          (pr-str (remove readable successful))))))
             (finally
               (d/release left)
               (d/release right))))
         (finally
           (d/release base)
           (d/delete-database cfg))))))

(deftest datahike-versioning-test
  (testing "Testing versioning functionality."
    (let [cfg {:store              {:backend :file
                                    :path    "/tmp/dh-versioning-test"
                                    :id #uuid "1e510000-0000-0000-0000-00000000001e"}
               :keep-history?      true
               :schema-flexibility :write
               :index              :datahike.index/persistent-set}
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))
          schema [{:db/ident       :age
                   :db/cardinality :db.cardinality/one
                   :db/valueType   :db.type/long}]
          _ (d/transact conn schema)
          store (:store @conn)]
      (testing "Test branching."
        (branch! conn :db :foo)
        (is (= (k/get store :branches nil {:sync? true})
               #{:db :foo})))
      (testing "Test merging."
        (let [foo-conn (d/connect (assoc cfg :branch :foo))]
          (d/transact foo-conn [{:age 42}])
          ;; extracted data from foo and decide to merge it into :db
          (merge! conn #{:foo} [{:age 42}])
          (d/release foo-conn))
        (is (= 4 (count (<?? S (branch-history conn)))))
        (is (= 2 (count (parent-commit-ids @conn)))))
      (testing "Force branch."
        (force-branch! @conn :foo2 #{:foo})
        (let [forced (branch-as-db store :foo2)]
          (is (db? forced))
          (is (every? uuid? (parent-commit-ids forced))
              "branch parents are frozen to immutable commit ids")))
      (testing "Load different references on current db."
        (is (= (commit-as-db store (commit-id @conn))
               (branch-as-db store :db)
               @conn)))
      (testing "Check branch history."
        (let [conn-foo2 (d/connect (assoc cfg :branch :foo2))]
          (is (= 4 (count (<?? S (branch-history conn-foo2)))))
          (d/release conn-foo2)))
      (testing "Delete branch."
        (delete-branch! conn :foo)
        (is (= (k/get store :branches nil {:sync? true})
               #{:db :foo2})))
      (d/release conn))))

#?(:clj
   (deftest commit-as-db-preserves-only-attached-generation-identity
     (let [cfg {:store {:backend :memory :id (random-uuid)}
                :keep-history? true
                :schema-flexibility :read}
           _ (d/create-database cfg)
           conn (d/connect cfg)]
       (try
         (d/transact conn [{:db/id 1 :value :first}])
         (let [first-commit (commit-id @conn)
               store (:store @conn)]
           (d/transact conn [{:db/id 1 :value :second}])
           (let [head-identity (d/committed-value-identity @conn)
                 attached (commit-as-db conn first-commit)
                 attached-via-db (commit-as-db @conn first-commit)
                 detached (commit-as-db store first-commit)
                 query '[:find ?value . :where [1 :value ?value]]
                 owner (d/q-with-evidence query attached)
                 hit (d/q-with-evidence query attached-via-db)]
             (doseq [db-value [attached attached-via-db]]
               (is (= (select-keys head-identity
                                   [:datahike.value/connection-id
                                    :datahike.value/generation])
                      (select-keys (d/committed-value-identity db-value)
                                   [:datahike.value/connection-id
                                    :datahike.value/generation])))
               (is (= first-commit
                      (:datahike.value/commit-id
                       (d/committed-value-identity db-value))))
               (is (= :first (d/q query db-value))))
             (is (= :first (:datahike.query/result owner)
                    (:datahike.query/result hit)))
             (is (= :datahike.cache.outcome/miss-owner
                    (get-in owner [:datahike.query/cache-evidence
                                   :datahike.cache/outcome])))
             (is (= :datahike.cache.outcome/hit
                    (get-in hit [:datahike.query/cache-evidence
                                 :datahike.cache/outcome])))
             (is (nil? (d/committed-value-identity detached))
                 "a raw store does not acquire connection ownership")))
         (finally
           (d/release conn)
           (d/delete-database cfg))))))

#?(:clj
   (deftest branch-preflights-commit-records
     (let [cfg {:store {:backend :memory :id (random-uuid)}
                :keep-history? true
                :schema-flexibility :read}
           _ (d/create-database cfg)
           conn (d/connect cfg)]
       (try
         (let [missing (random-uuid)]
           (is (= :commit-not-found
                  (try
                    (branch! conn missing :missing)
                    (catch Exception e (:type (ex-data e))))))
           (is (= #{:db} (set (datahike.versioning/branches conn)))))
         (finally
           (d/release conn)
           (d/delete-database cfg))))))

#?(:clj
   (deftest commit-branching-rejects-commit-graph-opt-out
     (let [cfg {:store {:backend :memory :id (random-uuid)}
                :keep-history? true
                :commit-graph? false
                :schema-flexibility :write}
           _ (d/create-database cfg)
           conn (d/connect cfg)]
       (try
         (let [cid (commit-id @conn)]
           (is (= :commit-graph-disabled
                  (try
                    (branch! conn cid :unavailable)
                    (catch Exception e (:type (ex-data e))))))
           (is (= #{:db} (set (datahike.versioning/branches conn))))
           (branch! conn :db :from-head)
           (is (= #{:db :from-head}
                  (set (datahike.versioning/branches conn)))))
         (finally
           (d/release conn)
           (d/delete-database cfg))))))

#?(:clj
   (deftest guarded-force-rejects-a-stale-head
     (let [cfg {:store {:backend :memory :id (random-uuid)}
                :keep-history? true
                :schema-flexibility :read}
           _ (d/create-database cfg)
           conn (d/connect cfg)]
       (try
         (d/transact conn [{:db/id 1 :value :at-t}])
         (let [cid-t (commit-id @conn)
               db-t (commit-as-db conn cid-t)]
           (d/transact conn [{:db/id 1 :value :at-head}])
           (let [cid-head (commit-id @conn)]
             (is (= :invalid-force-branch-options
                    (try
                      (force-branch! db-t :db #{cid-t}
                                     {:expected-current-commit cid-head
                                      :expected-commit cid-head})
                      (catch Exception e (:type (ex-data e)))))
                 "unknown guard keys are rejected instead of silently disabling safety")
             (is (= :stale-branch-head
                    (try
                      (force-branch! db-t :db #{cid-t}
                                     {:expected-current-commit cid-t})
                      (catch Exception e (:type (ex-data e))))))
             (is (= cid-head (commit-id (branch-as-db conn :db))))
             (is (= #{:at-head}
                    (set (d/q '[:find [?v ...] :where [_ :value ?v]]
                              (branch-as-db conn :db)))))

             (let [events (atom [])
                   installed (atom nil)
                   multi-assoc k/multi-assoc
                   update-store k/update]
               (with-redefs [k/multi-assoc
                             (fn [store entries & args]
                               (swap! events conj [:batch (mapv first entries)])
                               (apply multi-assoc store entries args))
                             k/update
                             (fn [store key update-fn & args]
                               (swap! events conj [:update key])
                               (apply update-store store key update-fn args))]
                 (reset! installed
                         (force-branch! db-t :db #{cid-t}
                                        {:expected-current-commit cid-head})))
               (is (uuid? @installed))
               (is (= @installed (commit-id (branch-as-db conn :db)))
                   "force-branch! returns the head it installed")
               (is (= [:db :branches]
                      (->> @events
                           (keep #(when (= :update (first %)) (second %)))
                           (partition-by identity)
                           (map first)
                           (take-last 2)))
                   "the forced head precedes the GC discovery roster"))
             (let [forced (branch-as-db conn :db)]
               (is (= #{:at-t}
                      (set (d/q '[:find [?v ...] :where [_ :value ?v]] forced))))
               (is (not= cid-head (commit-id forced))))))
         (finally
           (d/release conn)
           (d/delete-database cfg))))))

#?(:clj
   (deftest reachability-gate-refuses-start-while-sweep-is-queued
     (let [store-id (random-uuid)
           receipt {:datahike.test.maintenance/id "sweep-1"}
           roster (guard/acquire-reachability-permit! store-id :roster)
           sweep-ready (guard/acquire-sweep-permit!
                        store-id
                        {:sync? false
                         :datahike.gc-guard/maintenance-receipt receipt})]
       (try
         (let [refusal (guard/try-reachability-permit! store-id :roster)]
           (is (= :sweep-in-progress (:seon.error/kind refusal)))
           (is (true? (:seon.error/retryable? refusal)))
           (is (= store-id (:seon.store/id refusal)))
           (is (= receipt (:datahike.gc-guard/maintenance-receipt refusal)))
           (is (not (guard/reachability-permit-held? refusal))))
         (finally
           (guard/release-reachability-permit! roster)))
       (let [sweep (async/<!! sweep-ready)]
         (try
           (is (guard/reachability-permit-held? sweep))
           (finally
             (guard/release-reachability-permit! sweep)))))))

#?(:clj
   (deftest public-branch-wrappers-forward-reachability-permits
     (let [store-id (random-uuid)
           cfg {:store {:backend :memory :id store-id}
                :schema-flexibility :read}
           _ (d/create-database cfg)
           conn (d/connect cfg)
           permit (guard/acquire-reachability-permit! store-id :roster)]
       (try
         (d/branch! conn :db :permitted
                    {:datahike.gc-guard/reachability-permit permit})
         (is (contains? (d/branches conn) :permitted))
         (d/delete-branch! conn :permitted
                           {:datahike.gc-guard/reachability-permit permit})
         (is (not (contains? (d/branches conn) :permitted)))
         (finally
           (guard/release-reachability-permit! permit)
           (d/release conn)
           (d/delete-database cfg))))))

#?(:clj
   (deftest datahike-fork-database-test
     (testing "Testing fork-database: independent writable forks at head, tx-id and inst."
       (let [src-cfg {:store              {:backend :file
                                           :path    "/tmp/dh-fork-test-src"
                                           :id      #uuid "1e510000-0000-0000-0000-0000000000f0"}
                      :keep-history?      true
                      :schema-flexibility :write
                      :index              :datahike.index/persistent-set}
             tgt-store (fn [n & [id]]
                         {:store (cond-> {:backend :file
                                          :path    (str "/tmp/dh-fork-test-tgt-" n)}
                                   id (assoc :id id))})
             tgt-t-id   #uuid "1e510000-0000-0000-0000-0000000000f1"
             tgt-date-id #uuid "1e510000-0000-0000-0000-0000000000f2"
             tgt-err-id #uuid "1e510000-0000-0000-0000-0000000000f3"
             nuke-dir! (fn [p]
                         (let [f (java.io.File. ^String p)]
                           (when (.exists f)
                             (run! (fn [^java.io.File x] (.delete x))
                                   (reverse (file-seq f))))))
             _ (doseq [cfg [src-cfg
                            (tgt-store "t" tgt-t-id)
                            (tgt-store "date" tgt-date-id)
                            (tgt-store "err" tgt-err-id)]]
                 (d/delete-database cfg))
             _ (nuke-dir! "/tmp/dh-fork-test-tgt-head")
             _ (d/create-database src-cfg)
             conn (d/connect src-cfg)
             _ (d/transact conn [{:db/ident       :age
                                  :db/cardinality :db.cardinality/one
                                  :db/valueType   :db.type/long}])
             _ (d/transact conn [{:age 41}])
             t-mid (:max-tx @conn)
             _ (Thread/sleep 30)
             date-mid (java.util.Date.)
             _ (Thread/sleep 30)
             _ (d/transact conn [{:age 42}])
             t-head (:max-tx @conn)]
         (testing "Fork at head: independent, writable, identical."
           (let [fork-cfg (d/fork-database src-cfg (tgt-store "head"))
                 fconn (d/connect fork-cfg)]
             (testing "Fresh store identity, distinct conn in the same JVM."
               (is (uuid? (get-in fork-cfg [:store :id])))
               (is (not= (get-in src-cfg [:store :id]) (get-in fork-cfg [:store :id])))
               (is (not (identical? conn fconn))))
             (testing "Identical state at the fork point."
               (is (= t-head (:max-tx @fconn)))
               (is (= (set (d/q '[:find ?e ?a ?t :where [?e :age ?a ?t]] @conn))
                      (set (d/q '[:find ?e ?a ?t :where [?e :age ?a ?t]] @fconn)))))
             (testing "Writes do not leak in either direction."
               (d/transact fconn [{:age 99}])
               (is (= 1 (count (d/q '[:find ?e :where [?e :age 99]] @fconn))))
               (is (= 0 (count (d/q '[:find ?e :where [?e :age 99]] @conn))))
               (d/transact conn [{:age 77}])
               (is (= 0 (count (d/q '[:find ?e :where [?e :age 77]] @fconn)))))
             (testing "delete-database on the fork leaves the source intact."
               (d/release fconn)
               (d/delete-database fork-cfg)
               (is (d/database-exists? src-cfg))
               (is (= 3 (count (d/q '[:find ?e :where [?e :age]] @conn)))))))
         (testing "Fork at a tx-id mid-history."
           (let [fork-cfg (d/fork-database src-cfg (tgt-store "t" tgt-t-id) {:at t-mid})
                 fconn (d/connect fork-cfg)]
             (is (= t-mid (:max-tx @fconn)))
             (testing "Query results equal the source's as-of t; eids and tx-eids identical."
               (is (= (set (d/q '[:find ?e ?a ?t :where [?e :age ?a ?t]] (d/as-of @conn t-mid)))
                      (set (d/q '[:find ?e ?a ?t :where [?e :age ?a ?t]] @fconn)))))
             (testing "History inside the fork equals the source's history up to t."
               (is (= (set (map (juxt :e :a :v :tx :added) (d/datoms (d/history @fconn) :eavt)))
                      (set (map (juxt :e :a :v :tx :added)
                                (clojure.core/filter #(<= (Math/abs (long (:tx %))) t-mid)
                                                     (d/datoms (d/history @conn) :eavt)))))))
             (testing "New transactions continue at t+1."
               (d/transact fconn [{:age 55}])
               (is (= (inc t-mid) (:max-tx @fconn))))
             (testing "as-of works inside the fork."
               (is (= 1 (count (d/q '[:find ?e :where [?e :age]] (d/as-of @fconn t-mid))))))
             (d/release fconn)
             (d/delete-database fork-cfg)))
         (testing "Fork at an inst between two txs resolves to the earlier commit."
           (let [fork-cfg (d/fork-database src-cfg (tgt-store "date" tgt-date-id) {:at date-mid})
                 fconn (d/connect fork-cfg)]
             (is (= t-mid (:max-tx @fconn)))
             (d/release fconn)
             (d/delete-database fork-cfg)))
         (testing "Error cases leave no target store behind."
           (is (= :fork-target-same-store-identity
                  (try
                    (d/fork-database
                     src-cfg
                     {:store {:backend :file
                              :path "/tmp/dh-fork-test-same-id"
                              :id (get-in src-cfg [:store :id])}})
                    (catch Exception e (:type (ex-data e)))))
               "same-store fork refuses before acquiring the non-reentrant gate twice")
           (is (= :fork-point-after-head
                  (try (d/fork-database src-cfg (tgt-store "err" tgt-err-id) {:at (+ t-head 10)})
                       (catch Exception e (:type (ex-data e))))))
           ;; below tx0 — predates the initial commit, so the parent walk
           ;; exhausts without an exact :max-tx match
           (is (= :fork-point-not-found
                  (try (d/fork-database src-cfg (tgt-store "err" tgt-err-id) {:at (- const/tx0 5)})
                       (catch Exception e (:type (ex-data e))))))
           (is (= :invalid-fork-point
                  (try (d/fork-database src-cfg (tgt-store "err" tgt-err-id) {:at "yesterday"})
                       (catch Exception e (:type (ex-data e))))))
           (is (not (d/database-exists? (tgt-store "err" tgt-err-id)))))
         (testing "Semantic config conflicts are rejected."
           (is (= :fork-config-mismatch
                  (try (d/fork-database src-cfg (assoc (tgt-store "err" tgt-err-id) :keep-history? false))
                       (catch Exception e (:type (ex-data e)))))))
         (d/release conn)
         (d/delete-database src-cfg)))))

#?(:clj
   (deftest merge-shares-the-transaction-basis-fence-and-keeps-lineage-exact
     (let [cfg {:store {:backend :memory :id (random-uuid)} :keep-history? true :schema-flexibility :read}
           _ (d/create-database cfg)
           conn (d/connect cfg)
           _ (d/transact conn [{:db/id 1 :name "base"}])
           _ (branch! conn :db :foo)
           foo (d/connect (assoc cfg :branch :foo))
           _ (d/transact foo [{:db/id 2 :name "candidate"}])
           foo-cid (commit-id @foo)
           h (:max-tx @conn)
           h-cid (commit-id @conn)
           refusal (fn [arg-map]
                     (try (d/merge-db conn (merge {:parents #{foo-cid} :tx-data [{:db/id 2 :name "candidate"}]} arg-map))
                          (catch Exception e (:error (ex-data (last (take-while some? (iterate ex-cause e))))))))
           carrying (fn [] (count (filter #(contains? (set (parent-commit-ids %)) foo-cid)
                                          (<?? S (branch-history conn)))))]
       (try
         (d/transact conn [{:db/id 3 :name "moved"}])
         (is (= :transaction/stale-basis (refusal {:datahike/expected-basis-t h})))
         (is (nil? (:name (d/entity @conn 2))) "a stale merge lands no datom")
         (is (= :transaction/validation-rejected
                (refusal {:tx-meta {:datahike/validate-report (constantly :refused)}})))
         (is (= 0 (carrying)) "refused merges record no lineage")
         (let [moved (commit-id @conn)
               report (d/merge-db conn {:parents #{foo-cid} :tx-data [{:db/id 2 :name "candidate"}]
                                        :datahike/expected-basis-t (:max-tx @conn)})]
           (is (= #{moved foo-cid} (set (parent-commit-ids @conn))) "immutable parents")
           (is (= (commit-id (:db-after report)) (commit-id @conn)) "held connection advanced")
           (is (not= h-cid moved)))
         (d/transact conn [{:db/id 4 :name "after"}])
         (is (= 1 (carrying)) "a later transaction does not inherit merge parents")
         (let [merged @(d/merge-db! conn {:parents #{foo-cid} :tx-data [{:db/id 5 :name "again"}]})
               pending (d/transact! conn [{:db/id 6 :name "queued"}])]
           @pending
           (is (map? merged))
           (is (= 2 (carrying)) "a merge batched with a transaction keeps its parents"))
         (finally (d/release foo) (d/release conn) (d/delete-database cfg))))))
