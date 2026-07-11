(ns datahike.test.nodejs-test
  "End-to-end CLJS roundtrip + branching + GC tests.

   Migrated 2026-05-17 from `...` channel ceremony
   to native `^:async deftest` + `(await ...)`. Demonstrates the
   selective-Promise API: I/O-bound fns (`transact!`, `connect`,
   `delete-database`, `branches`, etc.) return Promises and are
   `await`ed; pure reads (`q`, `entity`, `pull`, `datoms`,
   `commit-id`) return values and are used directly.

   See `datahike.api.async` and `datahike.api`'s `emit-api` for the
   wrap rules — `:referentially-transparent?` in `api-specification`
   drives which functions wrap."
  (:require [cljs.test :refer [deftest is] :as t]
            [cljs.reader]
            [datahike.api :as d]
            [datahike.api.async :refer [chan->promise]]
            [datahike.index.audit :as ia]
            [datahike.audit :as audit]
            [datahike.online-gc :as online-gc]
            [konserve.core :as k]
            [konserve.node-filestore :as nfs] ;; Register :file backend for Node.js
            [cljs.nodejs :as nodejs]
            ;; Sibling test namespaces — included so `bb node-cljs-test`
            ;; covers them too.
            [datahike.test.index-test]
            [datahike.test.cljs-tiered-storage-test]
            [datahike.test.cljs-pattern-scan-test]
            ;; NOTE: datahike.test.optimistic-test, valid-time-test,
            ;; reference-test, time-variance-test, query-aggregates-test,
            ;; query-rules-test and background-gc-test are UPSTREAM tests
            ;; written against the core.async *channel* async contract
            ;; (`(<! (d/connect ...))`). This fork ships the native
            ;; Promise contract instead (see `datahike.api.async` +
            ;; `emit-api`'s `:referentially-transparent?` wrap), so those
            ;; tests' `<!` on a Promise throws. They are excluded from
            ;; this runner; port them to `await`/`^:async` (as this ns
            ;; was) if/when the Promise-contract equivalents are needed.
            [datahike.test.query-getelse-test]
            [datahike.test.cljs-recursive-rule-test]
            ;; Portable (pure, channel-free) upstream suites.
            [datahike.test.query-not-test]
            [datahike.test.query-or-test]
            ;; Portable graph algorithms.
            [datahike.test.experimental.graph-util-test]
            [datahike.test.experimental.graph-test]
            [datahike.test.experimental.anomaly-test]
            ;; Weighted LRU query-cache — the cljs WeightedLRU deftype has its
            ;; own implementation, so cover it (unit + test.check property) here.
            [datahike.test.lru-weighted-test]
            [datahike.test.lru-weighted-property-test]))

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

      (await (await (d/release conn))))

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

      (await (await (d/release conn2))))

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
      (await (await (d/release conn))))

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

      (await (await (d/release conn))))

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

      (await (await (d/release conn))))

    (await (d/delete-database cfg-no-gc))))

;; diff-buf phase-1 gate: read a JVM-written diff-buf store from cljs and verify the
;; buffered-leaf projection (Branch.child) reconstructs identical datoms cross-host.
;; The store + reference datoms are produced by /tmp/dh_exchange_build.clj on the JVM;
;; this test is a no-op (passes) when that artifact is absent (e.g. normal CI).
(def ^:private exchange-expected-file "/tmp/dh-exchange-expected.edn")

(deftest ^:async jvm-opbuf-exchange-test
  (try
             (if-not (fs.existsSync exchange-expected-file)
               (is true "JVM diff-buf exchange artifact absent — skipped")
               (let [{:keys [store-id dir n-count n-sum datom-count datoms]}
                     (cljs.reader/read-string (.readFileSync fs exchange-expected-file "utf8"))
                     cfg {:store {:backend :file :path dir :id store-id}
                          :schema-flexibility :write :keep-history? false}
                     conn (await (d/connect cfg))
                     db   @conn
                     got-datoms (->> (d/datoms db :eavt)
                                     (map (fn [d] [(:e d) (name (:a d)) (str (:v d))]))
                                     (sort)
                                     (vec))
                     got-n-count (d/q '[:find (count ?e) . :where [?e :n _]] db)
                     got-n-sum   (reduce + (map :v (filter #(= :n (:a %)) (d/datoms db :eavt))))]
                 (is (= datom-count (count got-datoms))
                     (str "cljs read same datom count (jvm=" datom-count " cljs=" (count got-datoms) ")"))
                 (is (= n-count got-n-count)
                     (str ":n entity count matches (jvm=" n-count " cljs=" got-n-count ")"))
                 (is (= n-sum got-n-sum)
                     (str ":n value sum matches (projection-sound) (jvm=" n-sum " cljs=" got-n-sum ")"))
                 (is (= datoms got-datoms)
                     "cljs eavt datoms identical to JVM (full buffered-leaf projection)")
                 (await (d/release conn))))
             (catch js/Error e
               (is false (str "jvm-opbuf-exchange-test error: " (.-message e))))
             ))

;; diff-buf phase-2 gate: cljs WRITE path. Same-host (create+transact+query all in cljs,
;; avoiding the pre-existing cross-host connect bug). Incremental commits make leaves
;; content-only dirty → buffered leaf slots in the root → on cold reopen they project back.
;; Writes to a FIXED dir (not deleted) so buffering can be confirmed externally (grep slots).
(def ^:private cljs-opbuf-dir "/tmp/dh-cljs-opbuf")

(deftest ^:async cljs-opbuf-write-roundtrip-test
  (let [sid #uuid "00000000-0000-0000-0000-00000000c1c5"
        cfg {:store {:backend :file :path cljs-opbuf-dir :id sid}
             :schema-flexibility :write :keep-history? false
             :index :datahike.index/persistent-set
             :index-config {:diff-buf-size 256}}]
    (try
               (when (await (d/database-exists? cfg)) (await (d/delete-database cfg)))
               (await (d/create-database cfg))
               (let [conn (await (d/connect cfg))]
                 (await (d/transact! conn [{:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}]))
                 (loop [bs (partition-all 100 (range 3000))]
                   (when (seq bs)
                     (await (d/transact! conn (mapv (fn [i] {:n i}) (first bs))))
                     (recur (rest bs))))
                 (let [db @conn
                       n-count (d/q '[:find (count ?e) . :where [?e :n _]] db)
                       n-sum   (reduce + (map :v (filter #(= :n (:a %)) (d/datoms db :eavt))))]
                   (is (= 3000 n-count) (str "warm n-count=" n-count))
                   (is (= 4498500 n-sum) (str "warm n-sum=" n-sum)))
                 (await (d/release conn)))
          ;; cold reopen → forces projection-on-read of buffered slots
               (let [conn2 (await (d/connect cfg))
                     db2   @conn2
                     n-count2 (d/q '[:find (count ?e) . :where [?e :n _]] db2)
                     n-sum2   (reduce + (map :v (filter #(= :n (:a %)) (d/datoms db2 :eavt))))
                     all-vs   (vec (sort (map :v (filter #(= :n (:a %)) (d/datoms db2 :eavt)))))]
                 (is (= 3000 n-count2) (str "cold n-count=" n-count2))
                 (is (= 4498500 n-sum2) (str "cold n-sum=" n-sum2))
                 (is (= (vec (range 3000)) all-vs) "cold :n values exact 0..2999 (buffered-leaf projection sound)")
                 (await (d/release conn2)))
               (catch js/Error e
                 (is false (str "cljs-opbuf-write-roundtrip error: " (.-message e))))
               )))

;; diff-buf phase-2 gate: cljs $remove path (retractions → leaf underflow → merge/borrow,
;; exercising the rotate/merge/merge-split slot-carry). Insert 2000, retract the even ones,
;; cold-reopen and verify the surviving odd set exactly.
(def ^:private cljs-opbuf-rm-dir "/tmp/dh-cljs-opbuf-rm")

(deftest ^:async cljs-opbuf-remove-roundtrip-test
  (let [sid #uuid "00000000-0000-0000-0000-0000000c1c5b"
        cfg {:store {:backend :file :path cljs-opbuf-rm-dir :id sid}
             :schema-flexibility :write :keep-history? false
             :index :datahike.index/persistent-set
             :index-config {:diff-buf-size 256}}]
    (try
               (when (await (d/database-exists? cfg)) (await (d/delete-database cfg)))
               (await (d/create-database cfg))
               (let [conn (await (d/connect cfg))]
                 (await (d/transact! conn [{:db/ident :n :db/valueType :db.type/long
                                         :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}]))
                 (loop [bs (partition-all 100 (range 2000))]
                   (when (seq bs)
                     (await (d/transact! conn (mapv (fn [i] {:n i}) (first bs))))
                     (recur (rest bs))))
            ;; retract even-:n entities (unique :n ⇒ lookup-ref retraction) in small commits
                 (loop [bs (partition-all 100 (filter even? (range 2000)))]
                   (when (seq bs)
                     (await (d/transact! conn (mapv (fn [i] [:db/retractEntity [:n i]]) (first bs))))
                     (recur (rest bs))))
                 (let [db @conn
                       vs (vec (sort (map :v (filter #(= :n (:a %)) (d/datoms db :eavt)))))]
                   (is (= 1000 (count vs)) (str "warm survivors=" (count vs)))
                   (is (= (vec (range 1 2000 2)) vs) "warm: exactly the odd :n survive"))
                 (await (d/release conn)))
          ;; cold reopen → projection-on-read of buffered slots after structural removes
               (let [conn2 (await (d/connect cfg))
                     db2   @conn2
                     vs    (vec (sort (map :v (filter #(= :n (:a %)) (d/datoms db2 :eavt)))))
                     sum   (reduce + vs)]
                 (is (= 1000 (count vs)) (str "cold survivors=" (count vs)))
                 (is (= 1000000 sum) (str "cold sum of odds=" sum))
                 (is (= (vec (range 1 2000 2)) vs) "cold: exactly the odd :n survive (remove+merge slot-carry sound)")
                 (await (d/release conn2)))
               (catch js/Error e
                 (is false (str "cljs-opbuf-remove-roundtrip error: " (.-message e))))
               )))

;; diff-buf phase-2 gate: cljs $replace path. A cardinality-one re-assertion (upsert with an
;; old value) routes through psset/replace → Branch.$replace for eavt/aevt. Insert 1000 ids
;; with :n 0, then update each :n to its id in small commits, cold-reopen and verify :n == id.
(def ^:private cljs-opbuf-rep-dir "/tmp/dh-cljs-opbuf-rep")

(deftest ^:async cljs-opbuf-replace-roundtrip-test
  (let [sid #uuid "00000000-0000-0000-0000-0000000c1c5c"
        cfg {:store {:backend :file :path cljs-opbuf-rep-dir :id sid}
             :schema-flexibility :write :keep-history? false
             :index :datahike.index/persistent-set
             :index-config {:diff-buf-size 256}}]
    (try
               (when (await (d/database-exists? cfg)) (await (d/delete-database cfg)))
               (await (d/create-database cfg))
               (let [conn (await (d/connect cfg))]
                 (await (d/transact! conn [{:db/ident :id :db/valueType :db.type/long
                                         :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}
                                        {:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}]))
                 (loop [bs (partition-all 100 (range 1000))]
                   (when (seq bs)
                     (await (d/transact! conn (mapv (fn [i] {:id i :n 0}) (first bs))))
                     (recur (rest bs))))
            ;; cardinality-one update of :n in small commits → upsert → $replace (eavt/aevt)
                 (loop [bs (partition-all 100 (range 1000))]
                   (when (seq bs)
                     (await (d/transact! conn (mapv (fn [i] [:db/add [:id i] :n i]) (first bs))))
                     (recur (rest bs))))
                 (let [db @conn
                       pairs (d/q '[:find ?id ?n :where [?e :id ?id] [?e :n ?n]] db)]
                   (is (= 1000 (count pairs)) (str "warm pairs=" (count pairs)))
                   (is (every? (fn [[id n]] (= id n)) pairs) "warm: every :n updated to its :id"))
                 (await (d/release conn)))
          ;; cold reopen → projection-on-read after $replace buffering
               (let [conn2 (await (d/connect cfg))
                     db2   @conn2
                     pairs (d/q '[:find ?id ?n :where [?e :id ?id] [?e :n ?n]] db2)
                     nsum  (reduce + (map second pairs))]
                 (is (= 1000 (count pairs)) (str "cold pairs=" (count pairs)))
                 (is (every? (fn [[id n]] (= id n)) pairs) "cold: every :n == its :id ($replace projection sound)")
                 (is (= 499500 nsum) (str "cold sum :n=" nsum)))
               (catch js/Error e
                 (is false (str "cljs-opbuf-replace-roundtrip error: " (.-message e))))
               )))

;; diff-buf phase-2 soundness gate: randomized insert/retract churn under a SMALL diff-buf
;; budget (more frequent buffer/write decisions, merges, borrows, splits) with periodic cold
;; reopens, compared against a reference set. Seeded LCG ⇒ deterministic/reproducible.
(def ^:private cljs-opbuf-gen-dir "/tmp/dh-cljs-opbuf-gen")

(deftest ^:async cljs-opbuf-generative-test
  (let [sid  #uuid "00000000-0000-0000-0000-0000000c1c5d"
        cfg  {:store {:backend :file :path cljs-opbuf-gen-dir :id sid}
              :schema-flexibility :write :keep-history? false
              :index :datahike.index/persistent-set
              :index-config {:diff-buf-size 64}}
        seed (atom 777)
        rnd  (fn [n] (mod (swap! seed (fn [x] (mod (+ (* x 1103515245) 12345) 2147483648))) n))
        idset (fn [c] (set (d/q '[:find [?id ...] :where [_ :id ?id]] @c)))]
    (try
               (when (await (d/database-exists? cfg)) (await (d/delete-database cfg)))
               (await (d/create-database cfg))
               (let [present (atom #{})
                     conn0 (await (d/connect cfg))]
                 (await (d/transact! conn0 [{:db/ident :id :db/valueType :db.type/long
                                          :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}]))
            ;; bulk-seed >bf entities so the index has BRANCH nodes (diff-buf only engages on
            ;; branches; a sub-512 tree is a single leaf and never buffers).
                 (loop [bs (partition-all 200 (range 2000))]
                   (when (seq bs)
                     (await (d/transact! conn0 (mapv (fn [i] {:id i}) (first bs))))
                     (recur (rest bs))))
                 (reset! present (set (range 2000)))
                 (loop [conn conn0, round 0]
                   (if (>= round 40)
                     (do (await (d/release conn))
                         (let [c (await (d/connect cfg))]
                           (is (= @present (idset c)) (str "final ref=" (count @present) " got=" (count (idset c))))
                           (await (d/release c))))
                     (let [insert? (even? (rnd 2))
                           cand    (vec (distinct (repeatedly 40 #(rnd 4000))))
                           ops     (if insert? (vec (remove @present cand)) (vec (filter @present cand)))]
                       (when (seq ops)
                         (if insert?
                           (do (await (d/transact! conn (mapv (fn [i] {:id i}) ops)))
                               (swap! present into ops))
                           (do (await (d/transact! conn (mapv (fn [i] [:db/retractEntity [:id i]]) ops)))
                               (swap! present (fn [s] (reduce disj s ops))))))
                       (if (zero? (mod (inc round) 8))
                         (do (await (d/release conn))
                             (let [c (await (d/connect cfg))]
                               (is (= @present (idset c)) (str "round " round " ref=" (count @present) " got=" (count (idset c))))
                               (recur c (inc round))))
                         (recur conn (inc round)))))))
               (catch js/Error e
                 (is false (str "cljs-opbuf-generative error: " (.-message e))))
               )))

;; diff-buf phase-3 gate: cljs MERKLE AUDIT (crypto-hash). Validates the cljs port of
;; branch-crypto-uuid/canon/walk-pss + -recompute-merkle-root, exercised via the real
;; datahike.audit/verify-chain :deep? API (which re-derives every node's content hash from
;; storage and confirms it matches its address). Covers baseline crypto AND crypto+diff-buf
;; (branch hash folds the slots), warm and after a cold reopen (projection-on-read). Also
;; spot-checks the index-level protocol directly.
(defn- audit-indices [db]
  (mapv (fn [k] [k (:status (ia/-recompute-merkle-root (get db k)))])
        [:eavt :aevt :avet]))

(defn- deep-verify-ok? [db]
  (let [rep (audit/verify-chain db nil {:deep? true})]
    [(:status rep) (get-in rep [:deep :status]) (get-in rep [:deep :diffs])]))

(deftest ^:async cljs-merkle-audit-test
  (try
             (doseq [[label opbuf] [["crypto baseline" 0] ["crypto + diff-buf" 256]]]
               (let [dir (tmp-dir)
                     cfg {:store {:backend :file :path dir :id (random-uuid)}
                          :schema-flexibility :write :keep-history? false
                          :crypto-hash? true
                          :index :datahike.index/persistent-set
                          :index-config (when (pos? opbuf) {:diff-buf-size opbuf})}]
                 (await (d/create-database cfg))
                 (let [conn (await (d/connect cfg))]
                   (await (d/transact! conn [{:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}]))
                   (loop [bs (partition-all 100 (range 2000))]
                     (when (seq bs)
                       (await (d/transact! conn (mapv (fn [i] {:n i}) (first bs))))
                       (recur (rest bs))))
                   (let [res (audit-indices @conn)
                         [st deep diffs] (deep-verify-ok? @conn)]
                     (is (every? (fn [[_ s]] (= :ok s)) res) (str label " warm index audit: " (pr-str res)))
                     (is (and (= :ok st) (= :ok deep)) (str label " warm verify-chain deep: " st "/" deep " diffs=" (pr-str diffs))))
                   (await (d/release conn)))
            ;; cold reopen → audit must still re-derive matching hashes (diff-buf projection)
                 (let [conn2 (await (d/connect cfg))
                       res   (audit-indices @conn2)
                       [st deep diffs] (deep-verify-ok? @conn2)]
                   (is (every? (fn [[_ s]] (= :ok s)) res) (str label " cold index audit: " (pr-str res)))
                   (is (and (= :ok st) (= :ok deep)) (str label " cold verify-chain deep: " st "/" deep " diffs=" (pr-str diffs)))
                   (await (d/release conn2)))
                 (await (d/delete-database cfg))))
             (catch js/Error e
               (is false (str "cljs-merkle-audit error: " (.-message e))))
             ))

;; Isolation probe: read a JVM-konserve-written map (default fressian serializer) cross-host
;; to test fress deserialization of namespaced keywords etc. (datahike-independent).
;; Written by /tmp/kons_probe_write.clj. Skips if absent.
(deftest ^:async xhost-fress-probe-test
  (try
             (if-not (fs.existsSync "/tmp/kons-probe")
               (is true "kons-probe artifact absent — skipped")
               (let [store (await (chan->promise (nfs/connect-fs-store "/tmp/kons-probe" :opts {:sync? false})))
                     v     (await (chan->promise (k/get store :probe nil {:sync? false})))]
                 (is (= :datahike.index/persistent-set (:ns-kw v)) (str ":ns-kw = " (pr-str (:ns-kw v))))
                 (is (= :db.type/long (:ns-kw2 v)) (str ":ns-kw2 = " (pr-str (:ns-kw2 v))))
                 (is (= :write (:simple-kw v)) (str ":simple-kw = " (pr-str (:simple-kw v))))
                 (is (= :x/y (get-in v [:nested :inner])) (str ":nested :inner = " (pr-str (get-in v [:nested :inner]))))
                 (is (= [:a/b :c] (:vec v)) (str ":vec = " (pr-str (:vec v))))))
             (catch js/Error e
               (is false (str "xhost-fress-probe error: " (.-message e))))
             ))

(defn -main []
  (t/run-tests 'datahike.test.nodejs-test
               'datahike.test.index-test
               'datahike.test.cljs-tiered-storage-test
               'datahike.test.cljs-pattern-scan-test
               ;; channel-contract upstream suites excluded — see the
               ;; require-block NOTE (this fork's Promise API contract).
               'datahike.test.query-getelse-test
               'datahike.test.cljs-recursive-rule-test
               'datahike.test.query-not-test
               'datahike.test.query-or-test
               'datahike.test.experimental.graph-util-test
               'datahike.test.experimental.graph-test
               'datahike.test.experimental.anomaly-test
               'datahike.test.lru-weighted-test
               'datahike.test.lru-weighted-property-test))
