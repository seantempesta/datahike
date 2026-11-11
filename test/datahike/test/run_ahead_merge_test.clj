(ns datahike.test.run-ahead-merge-test
  "A merge applied run-ahead on a :sync writer (`apply!` with `{:op 'merge!}`) is
   committed by `commit-pending!` as a merge commit: its parents recorded, the
   merge-parents key never persisted. `abandon-pending!` drops it whole."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.versioning :as v]
            [datahike.writer :as writer]))

(defn- setup []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true
             :initial-tx [{:db/ident :n :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}]
             :writer {:backend :sync}}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (v/branch! conn :db :foo)
      (let [foo (d/connect (assoc cfg :branch :foo))]
        (d/transact foo [{:db/id 1 :n 1}])
        (d/release foo))
      [cfg conn])))

(defn- instants-after
  "The :db/txInstant of each transaction of db after t, in transaction order."
  [db t]
  (->> (d/datoms db :aevt :db/txInstant)
       (filter #(> (:e %) t))
       (sort-by :e)
       (mapv #(.getTime ^java.util.Date (:v %)))))

(deftest a-run-ahead-merge-commits-its-parents
  (let [[cfg conn] (setup)
        foo-head (d/commit-id (v/branch-as-db conn :foo))
        t0 (:max-tx @conn)]
    (writer/apply! conn {:tx-data [{:db/id 2 :n 2}]})
    (writer/apply! conn {:tx-data [{:db/id 3 :n 3}]})
    (writer/apply! conn {:parents #{:foo} :tx-data [{:db/id 1 :n 1}]} {:op 'merge!})
    (let [{:keys [commit-id tx-count]} (writer/commit-pending! conn)]
      (is (= 3 tx-count))
      (testing "the commit is a merge commit of the branch and the merged one"
        (is (= 2 (count (v/parent-commit-ids @conn))))
        (is (contains? (set (v/parent-commit-ids @conn)) foo-head))
        (is (= foo-head (v/merge-base conn :db :foo)) "the merged branch's head is the merge base"))
      (testing "the merge-parents key is never persisted"
        (is (not (contains? (:meta @conn) :datahike/merge-parents)))
        (is (not (contains? (:meta (v/commit-as-db conn commit-id)) :datahike/merge-parents)))
        (let [c2 (d/connect cfg)]
          (is (not (contains? (:meta @c2) :datahike/merge-parents)))
          (is (= commit-id (d/commit-id @c2)))
          (d/release c2)))
      (testing "the three transactions have three rising instants"
        (let [ms (instants-after @conn t0)]
          (is (= 3 (count ms)))
          (is (apply < ms)))))
    (d/release conn)))

(deftest an-abandoned-run-ahead-merge-leaves-nothing
  (let [[_ conn] (setup)
        head (d/commit-id @conn)
        parents (v/parent-commit-ids @conn)]
    (writer/apply! conn {:tx-data [{:db/id 2 :n 2}]})
    (writer/apply! conn {:parents #{:foo} :tx-data [{:db/id 1 :n 1}]} {:op 'merge! :show? true})
    (is (not (contains? (:meta @conn) :datahike/merge-parents)) "a shown value carries no merge parents")
    (is (= 2 (:tx-count (writer/abandon-pending! conn))))
    (is (= head (d/commit-id @conn)))
    (is (= parents (v/parent-commit-ids @conn)))
    (is (not (contains? (:meta @conn) :datahike/merge-parents)))
    (testing "the next commit is an ordinary one"
      (d/transact conn [{:db/id 4 :n 4}])
      (is (= 1 (count (v/parent-commit-ids @conn)))))
    (d/release conn)))
