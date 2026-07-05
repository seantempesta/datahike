(ns datahike.test.versioning-test
  (:require #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
               :clj  [clojure.test :as t :refer [is deftest testing]])
            [datahike.versioning :refer
             [branch-history branch! delete-branch! merge! force-branch! branch-as-db parent-commit-ids
              commit-id commit-as-db]]
            [datahike.constants :as const]
            [datahike.db.utils :refer [db?]]
            [datahike.api :as d]
            [konserve.core :as k]
            [superv.async :refer [<?? S]]))

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
        (is (db? (branch-as-db store :foo2))))
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
