(ns datahike.test.run-ahead-test
  "Run-ahead writes at a :sync writer's serial caller (`datahike.writer/apply!`,
   `commit-pending!`, `abandon-pending!`): what is applied is committed once,
   later. With `{:show? true}` the applied value is also the connection's value
   at once, with no commit id of its own: it names the commit it was applied on
   as `:datahike/uncommitted-parent` instead."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.db :as db]
            [datahike.versioning :as dv]
            [datahike.writer :as writer]
            [superv.async :refer [<?? S]]))

(def ^:private run-dir (str (System/getProperty "java.io.tmpdir") "/dh-run-ahead-" (random-uuid)))

(defn- fresh-conn [label]
  (let [cfg {:store {:backend :file
                     :path (str run-dir "-" label)
                     :id (random-uuid)}
             :keep-history? false
             :schema-flexibility :read
             :writer {:backend :sync}}]
    (d/delete-database cfg)
    (d/create-database cfg)
    (d/connect cfg)))

(defn- names [db]
  (set (map first (d/q '[:find ?n :where [_ :name ?n]] db))))

;; the store's own head, read past the connection's value
(defn- stored [conn]
  (dv/branch-as-db conn :db))

(defn- mark [db]
  (select-keys (:meta db) [:datahike/commit-id :datahike/uncommitted-parent]))

(deftest a-shown-apply-is-the-connections-value-at-once
  (let [conn (fresh-conn "shown")
        cid (d/commit-id @conn)]
    (try
      (let [r (writer/apply! conn {:tx-data [{:name "a"}]} {:show? true})]
        (is (map? r))
        (is (= #{"a"} (names @conn)) "the connection shows the applied datom")
        (is (nil? (d/commit-id @conn)) "a shown value answers no commit id")
        (is (= cid (get-in @conn [:meta :datahike/uncommitted-parent])))
        (is (nil? (db/committed-value-identity @conn)) "nor a committed cache identity")
        (is (= cid (d/commit-id (:db-after r))) "the report's value is transact!'s own, unmarked")
        (is (= #{} (names (stored conn))) "the store holds nothing of it yet"))
      (finally (d/release conn)))))

(deftest shown-applies-commit-once
  (let [conn (fresh-conn "commit")
        cid (d/commit-id @conn)
        history #(count (<?? S (dv/branch-history conn)))
        commits (history)]
    (try
      (doseq [n ["a" "b" "c"]]
        (is (map? (writer/apply! conn {:tx-data [{:name n}]} {:show? true}))))
      (is (= #{"a" "b" "c"} (names @conn)))
      (is (= cid (get-in @conn [:meta :datahike/uncommitted-parent])) "each names the one commit below them")
      (is (= #{} (names (stored conn))))
      (is (= cid (d/commit-id (stored conn))))
      (let [{:keys [commit-id tx-count]} (writer/commit-pending! conn)]
        (is (= 3 tx-count))
        (is (some? commit-id))
        (is (not= cid commit-id))
        (is (= {:datahike/commit-id commit-id} (mark @conn)) "the committed value, with no mark")
        (is (= commit-id (:datahike.value/commit-id (db/committed-value-identity @conn))))
        (is (= #{cid} (:datahike/parents (:meta @conn))))
        (is (= #{"a" "b" "c"} (names (stored conn))))
        (is (= commit-id (d/commit-id (stored conn))))
        (is (= (inc commits) (history)) "exactly one new commit")
        (is (not (writer/pending? conn))))
      (finally (d/release conn)))))

(deftest abandon-restores-the-committed-value
  (let [conn (fresh-conn "abandon")
        _ (d/transact conn [{:name "kept"}])
        committed @conn]
    (try
      (writer/apply! conn {:tx-data [{:name "a"}]} {:show? true})
      (writer/apply! conn {:tx-data [{:name "b"}]} {:show? true})
      (is (= 2 (:tx-count (writer/abandon-pending! conn))))
      (is (identical? committed @conn) "the connection holds the committed value again")
      (is (= (d/commit-id committed) (d/commit-id @conn)))
      (is (= #{"kept"} (names @conn)))
      (is (not (writer/pending? conn)))
      (testing "a later apply and commit start from it"
        (writer/apply! conn {:tx-data [{:name "c"}]} {:show? true})
        (let [{:keys [commit-id]} (writer/commit-pending! conn)]
          (is (= #{"kept" "c"} (names @conn) (names (stored conn))))
          (is (= #{(d/commit-id committed)} (:datahike/parents (:meta @conn))))
          (is (= {:datahike/commit-id commit-id} (mark @conn)))))
      (finally (d/release conn)))))

(deftest without-show-the-connection-waits-for-the-commit
  (let [conn (fresh-conn "unshown")
        committed @conn]
    (try
      (let [r (writer/apply! conn {:tx-data [{:name "a"}]})]
        (is (= #{"a"} (names (:db-after r))) "the caller reads its value from the report")
        (is (identical? committed @conn) "the connection does not move")
        (writer/apply! conn {:tx-data [{:name "b"}]})
        (is (identical? committed @conn))
        (let [{:keys [commit-id]} (writer/commit-pending! conn)]
          (is (= #{"a" "b"} (names @conn)))
          (is (= {:datahike/commit-id commit-id} (mark @conn)))))
      (finally (d/release conn)))))
