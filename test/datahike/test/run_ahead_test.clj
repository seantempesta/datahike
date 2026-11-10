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

;; the connection's own value: on a writer that does not stream (:sync), a deref
;; of the connection reads the branch head from the store unless a value is shown
;; (`deref-conn`)
(defn- value [conn]
  @(:wrapped-atom conn))

;; the store's own head, read past the connection's value
(defn- stored [conn]
  (dv/branch-as-db conn :db))

(defn- mark [db]
  (select-keys (:meta db) [:datahike/commit-id :datahike/uncommitted-parent]))

(deftest a-shown-apply-is-the-connections-value-at-once
  (let [conn (fresh-conn "shown")
        cid (d/commit-id (value conn))]
    (try
      (let [r (writer/apply! conn {:tx-data [{:name "a"}]} {:show? true})]
        (is (map? r))
        (is (= #{"a"} (names (value conn))) "the connection shows the applied datom")
        (is (= #{"a"} (names @conn)) "and a deref of it answers it")
        (is (nil? (d/commit-id (value conn))) "a shown value answers no commit id")
        (is (nil? (d/commit-id @conn)))
        (is (= cid (get-in (value conn) [:meta :datahike/uncommitted-parent])))
        (is (nil? (db/committed-value-identity (value conn))) "nor a committed cache identity")
        (is (= cid (d/commit-id (:db-after r))) "the report's value is transact!'s own, unmarked")
        (is (= #{} (names (stored conn))) "the store holds nothing of it yet"))
      (finally (d/release conn)))))

(deftest shown-applies-commit-once
  (let [conn (fresh-conn "commit")
        cid (d/commit-id (value conn))
        history #(count (<?? S (dv/branch-history conn)))
        commits (history)]
    (try
      (doseq [n ["a" "b" "c"]]
        (is (map? (writer/apply! conn {:tx-data [{:name n}]} {:show? true}))))
      (is (= #{"a" "b" "c"} (names (value conn))))
      (is (= cid (get-in (value conn) [:meta :datahike/uncommitted-parent])) "each names the one commit below them")
      (is (= #{} (names (stored conn))))
      (is (= cid (d/commit-id (stored conn))))
      (let [{:keys [commit-id tx-count]} (writer/commit-pending! conn)]
        (is (= 3 tx-count))
        (is (some? commit-id))
        (is (not= cid commit-id))
        (is (= {:datahike/commit-id commit-id} (mark (value conn))) "the committed value, with no mark")
        (is (= commit-id (d/commit-id @conn)))
        (is (= #{"a" "b" "c"} (names @conn)))
        (is (= commit-id (:datahike.value/commit-id (db/committed-value-identity (value conn)))))
        (is (= #{cid} (:datahike/parents (:meta (value conn)))))
        (is (= #{"a" "b" "c"} (names (stored conn))))
        (is (= commit-id (d/commit-id (stored conn))))
        (is (= (inc commits) (history)) "exactly one new commit")
        (is (not (writer/pending? conn))))
      (finally (d/release conn)))))

(deftest abandon-restores-the-committed-value
  (let [conn (fresh-conn "abandon")
        _ (d/transact conn [{:name "kept"}])
        committed (value conn)]
    (try
      (writer/apply! conn {:tx-data [{:name "a"}]} {:show? true})
      (writer/apply! conn {:tx-data [{:name "b"}]} {:show? true})
      (is (= 2 (:tx-count (writer/abandon-pending! conn))))
      (is (identical? committed (value conn)) "the connection holds the committed value again")
      (is (= (d/commit-id committed) (d/commit-id (value conn))))
      (is (= #{"kept"} (names (value conn))))
      (is (= (d/commit-id committed) (d/commit-id @conn)) "a deref answers the committed value")
      (is (= #{"kept"} (names @conn)))
      (is (not (writer/pending? conn)))
      (testing "a later apply and commit start from it"
        (writer/apply! conn {:tx-data [{:name "c"}]} {:show? true})
        (let [{:keys [commit-id]} (writer/commit-pending! conn)]
          (is (= #{"kept" "c"} (names (value conn)) (names (stored conn))))
          (is (= #{(d/commit-id committed)} (:datahike/parents (:meta (value conn)))))
          (is (= {:datahike/commit-id commit-id} (mark (value conn))))))
      (finally (d/release conn)))))

(deftest without-show-the-connection-waits-for-the-commit
  (let [conn (fresh-conn "unshown")
        committed (value conn)]
    (try
      (let [r (writer/apply! conn {:tx-data [{:name "a"}]})]
        (is (= #{"a"} (names (:db-after r))) "the caller reads its value from the report")
        (is (identical? committed (value conn)) "the connection does not move")
        (writer/apply! conn {:tx-data [{:name "b"}]})
        (is (identical? committed (value conn)))
        (let [{:keys [commit-id]} (writer/commit-pending! conn)]
          (is (= #{"a" "b"} (names (value conn))))
          (is (= {:datahike/commit-id commit-id} (mark (value conn))))))
      (finally (d/release conn)))))

(deftest a-write-that-does-not-run-ahead-is-refused-while-one-is-pending
  (let [conn (fresh-conn "refused")]
    (try
      (writer/apply! conn {:tx-data [{:name "a"}]} {:show? true})
      (writer/apply! conn {:tx-data [{:name "b"}]} {:show? true})
      (let [e (try (d/transact conn [{:name "c"}]) nil
                   (catch Throwable e e))]
        (is (= {:type :run-ahead-pending :tx-count 2} (select-keys (ex-data e) [:type :tx-count]))))
      (let [e (try (writer/transact-batch! conn [{:tx-data [{:name "c"}]}]) nil
                   (catch Throwable e e))]
        (is (= :run-ahead-pending (:type (ex-data e)))))
      (is (= #{"a" "b"} (names @conn)) "the refused writes left the shown value as it was")
      (writer/commit-pending! conn)
      (testing "once committed, a write goes through"
        (d/transact conn [{:name "c"}])
        (is (= #{"a" "b" "c"} (names @conn) (names (stored conn)))))
      (finally (d/release conn)))))
