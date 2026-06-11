(ns datahike.test.nodejs-test
  "End-to-end CLJS roundtrip + branching + GC tests.

   Migrated 2026-05-17 from `(async done (go ...))` channel ceremony
   to native `^:async deftest` + `(await ...)`. Demonstrates the
   selective-Promise API: I/O-bound fns (`transact!`, `connect`,
   `delete-database`, `branches`, etc.) return Promises and are
   `await`ed; pure reads (`q`, `entity`, `pull`, `datoms`,
   `commit-id`) return values and are used directly.

   See `datahike.api.async` and `datahike.api`'s `emit-api` for the
   wrap rules — `:referentially-transparent?` in `api-specification`
   drives which functions wrap."
  (:require [cljs.test :refer [deftest is] :as t]
            [datahike.api :as d]
            [datahike.api.async :refer [chan->promise]]
            [datahike.online-gc :as online-gc]
            [konserve.core :as k]
            [konserve.node-filestore] ;; Register :file backend for Node.js
            [cljs.nodejs :as nodejs]
            ;; Sibling test namespaces — included so `bb node-cljs-test`
            ;; covers them too.
            [datahike.test.cljs-pattern-scan-test]
            [datahike.test.optimistic-test]
            [datahike.test.valid-time-test]
            [datahike.test.query-getelse-test]))

;; Hook cljs.test's end-of-run callback so the Node process exits with
;; status 0 only when all tests pass.
(defmethod t/report [::t/default :end-run-tests] [m]
  (.exit js/process (if (t/successful? m) 0 1)))

(def fs (nodejs/require "fs"))
(def path (nodejs/require "path"))
(def os (nodejs/require "os"))

(defn tmp-dir []
  (let [dir (path.join (os.tmpdir) (str "datahike-node-test-" (rand-int 100000)))]
    dir))

(deftest ^:async roundtrip-test
  (let [dir (tmp-dir)
        store-id (random-uuid)
        cfg {:store {:backend :file :path dir :id store-id}
             :keep-history? true
             :schema-flexibility :write}]
    ;; --- async: create + connect + transact ---
    (is (await (d/create-database cfg)) "Database created")
    (let [conn (await (d/connect cfg))]
      (is conn "Connection established")

      (let [schema-tx [{:db/ident :name
                        :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one}
                       {:db/ident :age
                        :db/valueType :db.type/long
                        :db/cardinality :db.cardinality/one}]
            report (await (d/transact! conn schema-tx))]
        (is (:db-after report) "Schema added"))

      (let [tx-report (await (d/transact! conn [{:name "Alice" :age 30}
                                                {:name "Bob" :age 25}]))]
        (is (:db-after tx-report) "Data transacted")
        (is (pos? (count (:tx-data tx-report))) "Datoms were added"))

      ;; --- sync reads on the db value ---
      (let [all-datoms (vec (d/datoms @conn :eavt))
            name-datoms (filter #(= :name (:a %)) all-datoms)
            age-datoms (filter #(= :age (:a %)) all-datoms)]
        (is (= 2 (count name-datoms)) "Found 2 name datoms")
        (is (= 2 (count age-datoms)) "Found 2 age datoms"))

      (let [entities (d/q '[:find ?e :where [?e :name _]] @conn)
            e1 (ffirst entities)]
        (when e1
          (let [pulled (d/pull @conn [:name :age] e1)]
            (is (:name pulled) "Pull retrieved name")
            (is (:age pulled) "Pull retrieved age"))))

      (let [q1 (d/q '[:find ?e :where [?e :name _]] @conn)]
        (is (= 2 (count q1)) "Single pattern: found 2 entities")
        (is (every? #(number? (first %)) q1) "Single pattern: entity IDs are numbers"))

      (let [q2 (d/q '[:find ?v :where [_ :name ?v]] @conn)
            names (set (map first q2))]
        (is (= 2 (count q2)) "Value query: found 2 names")
        (is (contains? names "Alice") "Value query: found Alice")
        (is (contains? names "Bob") "Value query: found Bob"))

      (let [q3 (d/q '[:find ?e ?name ?age
                      :where
                      [?e :name ?name]
                      [?e :age ?age]] @conn)
            results (into {} (map (fn [[e name age]] [name {:e e :age age}]) q3))]
        (is (= 2 (count q3)) "Join query: found 2 entity/name/age tuples")
        (is (number? (get-in results ["Alice" :e])) "Join query: Alice has valid entity ID")
        (is (= 30 (get-in results ["Alice" :age])) "Join query: Alice is 30")
        (is (number? (get-in results ["Bob" :e])) "Join query: Bob has valid entity ID")
        (is (= 25 (get-in results ["Bob" :age])) "Join query: Bob is 25"))

      (let [entities (d/q '[:find ?e :where [?e :name _]] @conn)
            e1 (ffirst entities)]
        (when e1
          (let [entity (d/entity @conn e1)]
            (is (:name entity) "Entity has name")
            (is (:age entity) "Entity has age"))))

      ;; --- async write for history ---
      (let [entities (d/q '[:find ?e :where [?e :name _]] @conn)
            e1 (ffirst entities)]
        (when e1
          (await (d/transact! conn [[:db/add e1 :age 31]]))))

      ;; --- mix of sync reads + async writes for as-of test ---
      (let [entities (d/q '[:find ?e :where [?e :name _]] @conn)
            e1 (ffirst entities)
            before-update-tx (:max-tx @conn)]
        (when e1
          (await (d/transact! conn [[:db/add e1 :age 99]])))
        (let [current-age-after (when e1 (:age (d/entity @conn e1)))]
          (is (= 99 current-age-after) "Current DB shows updated age"))
        (let [as-of-db (d/as-of @conn before-update-tx)
              as-of-age (when e1 (:age (d/entity as-of-db e1)))]
          (is (= 31 as-of-age) "as-of DB shows old age value")))

      (let [hist-db (d/history @conn)
            hist-datoms (vec (filter #(= :age (:a %)) (d/datoms hist-db :eavt)))]
        (is (>= (count hist-datoms) 4) "History contains multiple age values"))

      (await (d/release conn)))

    ;; --- reconnect to verify persistence ---
    (let [conn2 (await (d/connect cfg))]
      (is conn2 "Reconnected successfully")

      (let [all-datoms (vec (d/datoms @conn2 :eavt))
            name-datoms (filter #(= :name (:a %)) all-datoms)]
        (is (= 2 (count name-datoms)) "Data persisted after reconnect"))

      (let [aevt-datoms (take 5 (d/datoms @conn2 :aevt))
            avet-datoms (take 5 (d/datoms @conn2 :avet))]
        (is (seq aevt-datoms) "Got datoms from AEVT index")
        (is (seq avet-datoms) "Got datoms from AVET index"))

      (await (d/release conn2)))

    (is (nil? (await (d/delete-database cfg))) "Database deleted")
    (is (not (fs.existsSync dir)) "Directory removed")))

(deftest ^:async branching-and-merge-test
  (let [dir (tmp-dir)
        cfg {:store {:backend :file :path dir :id (random-uuid)}
             :keep-history? false
             :schema-flexibility :write}]
    (await (d/create-database cfg))
    (let [conn (await (d/connect cfg))]

      (await (d/transact! conn [{:db/ident :name
                                 :db/valueType :db.type/string
                                 :db/cardinality :db.cardinality/one}]))
      (await (d/transact! conn [{:name "Alice"} {:name "Bob"}]))

      ;; branches reads the store — async.
      (let [bs (await (d/branches conn))]
        (is (contains? bs :db) "Default branch :db exists")
        (is (= 1 (count bs)) "Only one branch initially"))

      ;; commit-id / parent-commit-ids are pure reads — sync.
      (let [cid (d/commit-id @conn)
            pids (d/parent-commit-ids @conn)]
        (is (uuid? cid) "commit-id is a UUID")
        (is (set? pids) "parent-commit-ids is a set"))

      (await (d/branch! conn :db :feature))
      (let [bs (await (d/branches conn))]
        (is (= #{:db :feature} bs) "Feature branch created"))

      (let [feat-conn (await (d/connect (assoc cfg :branch :feature)))]
        (await (d/transact! feat-conn [{:name "Charlie"}]))

        (let [feat-names (d/q '[:find ?n :where [_ :name ?n]] @feat-conn)
              feat-name-set (set (map first feat-names))]
          (is (contains? feat-name-set "Charlie") "Feature has Charlie")
          (is (contains? feat-name-set "Alice") "Feature inherited Alice"))

        (let [main-names (d/q '[:find ?n :where [_ :name ?n]] @conn)
              main-name-set (set (map first main-names))]
          (is (not (contains? main-name-set "Charlie")) "Main does not have Charlie yet"))

        (let [merge-report (await (d/merge-db! conn #{:feature} [{:name "Charlie"}]))]
          (is (:db-after merge-report) "Merge produced db-after"))

        (let [main-names (d/q '[:find ?n :where [_ :name ?n]] @conn)
              main-name-set (set (map first main-names))]
          (is (contains? main-name-set "Charlie") "Main has Charlie after merge")
          (is (contains? main-name-set "Alice") "Main still has Alice"))

        ;; branch-as-db / commit-as-db touch storage — async.
        (let [feat-db (await (d/branch-as-db conn :feature))]
          (is (some? feat-db) "branch-as-db returns a db"))

        (let [cid (d/commit-id @conn)
              cdb (await (d/commit-as-db conn cid))]
          (is (some? cdb) "commit-as-db returns a db"))

        (await (d/delete-branch! conn :feature))
        (let [bs (await (d/branches conn))]
          (is (= #{:db} bs) "Feature branch deleted"))

        (await (d/release feat-conn)))
      (await (d/release conn)))

    (await (d/delete-database cfg))))

(deftest ^:async online-gc-basic-test
  (let [dir (tmp-dir)
        cfg-no-gc {:store {:backend :file :path dir :id (random-uuid)}
                   :online-gc {:enabled? false}
                   :crypto-hash? false
                   :keep-history? false
                   :schema-flexibility :write}]

    (await (d/create-database cfg-no-gc))
    (let [conn (await (d/connect cfg-no-gc))]

      (await (d/transact! conn [{:db/ident :name
                                 :db/valueType :db.type/string
                                 :db/cardinality :db.cardinality/one}]))
      (await (d/transact! conn [{:name "Alice"}]))
      (await (d/transact! conn [{:name "Bob"}]))

      (let [freed-atom (-> @conn :store :storage :freed-addresses)
            initial-freed (count @freed-atom)]
        (is (> initial-freed 0) "Should have freed addresses with GC disabled"))

      ;; online-gc/online-gc! is NOT in datahike.api, so it still
      ;; returns a channel. Bridge to Promise so await works.
      (let [gc-result (await (chan->promise
                               (online-gc/online-gc! (:store @conn)
                                                     {:enabled? true
                                                      :grace-period-ms 0
                                                      :sync? false})))]
        (is (number? gc-result) "GC returned a count"))

      (let [freed-atom (-> @conn :store :storage :freed-addresses)
            final-freed (count @freed-atom)]
        (is (= 0 final-freed) "Freed addresses should be cleared after GC"))

      (await (d/release conn)))

    (await (d/delete-database cfg-no-gc))))

(deftest ^:async online-gc-multi-branch-safety-test
  (let [dir (tmp-dir)
        cfg-no-gc {:store {:backend :file :path dir :id (random-uuid)}
                   :online-gc {:enabled? false}
                   :crypto-hash? false
                   :keep-history? false
                   :schema-flexibility :write}]

    (await (d/create-database cfg-no-gc))
    (let [conn (await (d/connect cfg-no-gc))]

      (await (d/transact! conn [{:db/ident :name
                                 :db/valueType :db.type/string
                                 :db/cardinality :db.cardinality/one}]))
      (await (d/transact! conn [{:name "Alice"}]))

      ;; konserve.core returns channels — bridge to Promise.
      (await (chan->promise (k/assoc (:store @conn) :branches #{:db :branch-a})))

      (let [branches (await (chan->promise (k/get (:store @conn) :branches)))]
        (is (= 2 (count branches)) "Should have two branches"))

      (await (d/transact! conn [{:name "Bob"}]))

      (let [freed-before (count @(-> @conn :store :storage :freed-addresses))]
        (is (> freed-before 0) "Should have freed addresses with GC disabled"))

      (let [gc-result (await (chan->promise
                               (online-gc/online-gc! (:store @conn)
                                                     {:enabled? true
                                                      :grace-period-ms 0
                                                      :sync? false})))]
        (is (= 0 gc-result) "Multi-branch GC should be skipped (return 0)"))

      (let [freed-after (count @(-> @conn :store :storage :freed-addresses))]
        (is (> freed-after 0) "Multi-branch GC should leave freed addresses for offline GC"))

      (let [result (d/q '[:find ?e ?n :where [?e :name ?n]] @conn)]
        (is (= 2 (count result)) "Both Alice and Bob should still exist"))

      (await (d/release conn)))

    (await (d/delete-database cfg-no-gc))))

(defn -main []
  (t/run-tests 'datahike.test.nodejs-test
               'datahike.test.cljs-pattern-scan-test
               'datahike.test.optimistic-test
               'datahike.test.valid-time-test
               'datahike.test.query-getelse-test))
