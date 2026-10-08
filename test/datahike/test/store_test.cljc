(ns datahike.test.store-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as d]
   #?@(:clj [[clojure.core.async :as async]
             [datahike.versioning :as v]
             [datahike.writing :as dw]
             [konserve.core :as k]
             [superv.async :refer [<?? S]]]))
  (:import [java.lang System]))

(defn test-store [cfg]
  (let [_ (d/delete-database cfg)]
    (is (not (d/database-exists? cfg)))
    (let [cfg (merge cfg {:schema-flexibility :read})
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (d/transact conn [{:db/id 1, :name  "Ivan", :age   15}
                        {:db/id 2, :name  "Petr", :age   37}
                        {:db/id 3, :name  "Ivan", :age   37}
                        {:db/id 4, :age 15}])
      (is (= (d/q '[:find ?e :where [?e :name]] @conn)
             #{[3] [2] [1]}))

      (is (nil? (:cache (:store @conn)))
          "the raw Konserve store has no unused wrapper LRU")
      (is (some? (:cache (:storage (:store @conn))))
          "CachedStorage owns the one persistent-set node cache")

      (d/release conn)
      (is (d/database-exists? cfg)))))

(deftest test-db-file-store
  (test-store {:store {:backend :file
                       :path (case (System/getProperty "os.name")
                               "Windows 10" (str (System/getProperty "java.io.tmpdir") "api-fs")
                               "/tmp/api-fs")
                       :id #uuid "f11e0000-0000-0000-0000-00000000000f"}}))

(deftest test-db-mem-store
  (test-store {:store {:backend :memory :id #uuid "e0000000-0000-0000-0000-00000000000e"}}))

(deftest test-index
  (let [config {:store {:backend :memory
                        :id #uuid "f0000000-0000-0000-0000-00000000000f"}
                :schema-flexibility :read
                :keep-history? false}]
    (d/delete-database config)
    (d/create-database config)
    (let [conn (d/connect config)]
      (testing "root node type"
        (d/transact conn [{:db/id 1, :name "Alice"}])
        (is (= (if (= :datahike.index/persistent-set (-> @conn :config :index))
                 org.replikativ.persistent_sorted_set.PersistentSortedSet
                 hitchhiker.tree.DataNode)
               (-> @conn :eavt type))))
      (testing "upsert"
        (d/transact conn [{:db/id 1, :name "Paula"}])
        (is (= "Paula" (:name (d/entity @conn 1)))))
      (d/release conn))))

(deftest test-binary-support
  (let [config {:store {:backend :memory
                        :id #uuid "00100000-0000-0000-0000-000000000010"}
                :schema-flexibility :read
                :keep-history? false}]
    (d/delete-database config)
    (d/create-database config)
    (let [conn (d/connect config)]
      (d/transact conn [{:db/id 1, :name "Jiayi", :payload (byte-array [0 2 3])}
                        {:db/id 2, :name "Peter", :payload (byte-array [1 2 3])}])
      (is (= "Jiayi"
             (d/q '[:find ?n .
                    :in $ ?arr
                    :where
                    [?e :payload ?arr]
                    [?e :name ?n]]
                  @conn
                  (byte-array [0 2 3]))))
      (d/release conn))))

(deftest test-bytes-schema-upsert
  ;; Regression test: upserting a :db.type/bytes attribute used to throw
  ;; "byte[] cannot be cast to java.lang.Comparable" because compare-value
  ;; passed byte arrays to clojure.core/compare, which requires Comparable.
  (let [config {:store {:backend :memory
                        :id #uuid "00100000-0000-0000-0000-000000000011"}
                :schema-flexibility :write
                :initial-tx
                [{:db/ident :doc/id
                  :db/unique :db.unique/identity
                  :db/valueType :db.type/string
                  :db/cardinality :db.cardinality/one}
                 {:db/ident :doc/save
                  :db/valueType :db.type/bytes
                  :db/cardinality :db.cardinality/one}]}]
    (d/delete-database config)
    (d/create-database config)
    (let [conn (d/connect config)]
      (testing "first transact with bytes value"
        (d/transact conn [{:doc/id "k" :doc/save (byte-array [1 2 3])}])
        (is (some? (d/entity @conn [:doc/id "k"]))))
      (testing "upsert with identical bytes value"
        (d/transact conn [{:doc/id "k" :doc/save (byte-array [1 2 3])}])
        (is (some? (d/entity @conn [:doc/id "k"]))))
      (testing "upsert with different bytes value"
        (d/transact conn [{:doc/id "k" :doc/save (byte-array [4 5 6])}])
        (is (some? (d/entity @conn [:doc/id "k"]))))
      (d/release conn))))

(deftest test-database-exists-with-invalid-store
  (testing "Store with missing :id"
    (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
                          #"\s+:id\s+"
                          (d/database-exists?
                           {:store {:backend :memory}})))))

#?(:clj
   (deftest test-diff-buf-upsert-reopen
     ;; Regression: diff-buf buffers a value-changing upsert as Absent(old)+Present(new) in a leaf-diff.
     ;; The leaf-diff must serialize to the comparator-agnostic {:absent :present} form; if it were a
     ;; map keyed by the Datom, fressian round-trip would re-key by Datom equality (e,a,v — ignores tx),
     ;; collapsing the two entries and dropping the removal → a stale old datom survives reopen.
     (testing "many value-changing upserts survive store→reopen with no stale/duplicate datoms (diff-buf)"
       (let [cfg {:store {:backend :file
                          :path (str (System/getProperty "java.io.tmpdir") "/dh-diffbuf-upsert")
                          :id #uuid "d1ffb000-0000-0000-0000-00000000d1ff"}
                  :schema-flexibility :write :keep-history? false
                  :index-config {:diff-buf-size 256 :branching-factor 16}}
             n   400]
         (d/delete-database cfg)
         (d/create-database cfg)
         (let [conn (d/connect cfg)]
           (d/transact conn [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                             {:db/ident :age  :db/valueType :db.type/long   :db/cardinality :db.cardinality/one}])
           (doseq [i (range n)]       (d/transact conn [{:name (str "p" i) :age (mod (* i 7) 100)}]))
           (doseq [i (range 0 n 3)]   (d/transact conn [{:name (str "p" i) :age (+ 100 i)}]))   ; value-changing upserts
           (d/release conn))
         (let [conn (d/connect cfg)
               db   @conn
               got  (set (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]] db))
               exp  (into #{} (for [i (range n)] [(str "p" i) (if (zero? (mod i 3)) (+ 100 i) (mod (* i 7) 100))]))]
           (is (= exp got) "name/age query exact after reopen")
           (is (= (* 2 n) (count (filter #(#{:name :age} (:a %)) (d/datoms db :eavt))))
               "exactly one :name + one :age datom per entity (no stale buffered-replace duplicate)")
           (d/release conn))
         (d/delete-database cfg)))))

#?(:clj
   (deftest test-detached-root-count-after-reopen
     ;; Regression (pre-existing on main, found by a diff-buf equivalence probe):
     ;; a restore+mutate can leave the PSS cached count unknown (-1); the
     ;; canonical root WRITE handler serialized :count via (count pset), and
     ;; recomputing it on the storage-DETACHED stored copy (db->stored detaches,
     ;; #854) NPE'd in Branch.child while materializing lazy children. Fixed
     ;; upstream in persistent-sorted-set 0.4.132 (the handler serializes the
     ;; cached count as-is; readers resolve -1 lazily with their own storage).
     ;; Recipe: deep tree (small bf), history on, mixed upserts/retractions,
     ;; reconnect mid-stream.
     (testing "commits keep succeeding across reopen+mutate on a deep tree"
       (let [cfg {:store {:backend :file
                          :path (str (System/getProperty "java.io.tmpdir") "/dh-detached-count")
                          :id #uuid "d1ffb000-0000-0000-0000-00000000dead"}
                  :schema-flexibility :write :keep-history? true
                  :index-config {:branching-factor 16}}
             rng (java.util.Random. 42)]
         (d/delete-database cfg)
         (d/create-database cfg)
         (let [conn (atom (d/connect cfg))]
           (d/transact @conn [{:db/ident :id :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                              {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
           (dotimes [i 150]
             (let [r (.nextInt rng 4) id (long (.nextInt rng 300))]
               (try
                 (case r
                   0 (d/transact @conn [{:id id :score (long (.nextInt rng 100))}])
                   1 (d/transact @conn [[:db/retractEntity [:id id]]])
                   (d/transact @conn (vec (for [j (range 5)] {:id (long (+ 300 (* i 5) j)) :score (long j)}))))
                 (catch Exception e
                   (when-not (re-find #"(?i)nothing to retract" (str (.getMessage e)))
                     (throw e)))))
             (when (zero? (mod (inc i) 50))
               (d/release @conn)
               (reset! conn (d/connect cfg))))
           ;; count must be consistent with the actual datom set after all that
           (let [db @@conn]
             (is (= (count (filter #(= :id (:a %)) (d/datoms db :eavt)))
                    (d/q '[:find (count ?e) . :where [?e :id _]] db))))
           (d/release @conn))
         (d/delete-database cfg)))))

#?(:clj
   (do
     (def ^:private compact-cfg
       {:store {:backend :file
                :path (str (System/getProperty "java.io.tmpdir") "/dh-compact-head")
                :id #uuid "c0c0a000-0000-0000-0000-0000000c0c0a"
                :config {:defer-immutable-sync? true :compressor {:type :lz4}}}
        :schema-flexibility :write :keep-history? true :fuse-index-roots? true
        :index-config {:branching-factor 16 :diff-buf-size 4}})

     (defn- datom-set [db index]
       (into #{} (map (juxt :e :a :v :tx :added)) (d/datoms db index)))

     (defn- observe-branch
       "What every branch-head reader gives for `branch`: connect, branch-as-db,
       branch-history, as-of and history."
       [cfg branch]
       (let [conn (d/connect (assoc cfg :branch branch))]
         (try
           (let [db @conn
                 as-db (v/branch-as-db conn branch)
                 history (async/<!! (v/branch-history conn))
                 first-tx (apply min (map :tx (d/datoms db :eavt :item/name)))]
             {:datoms (datom-set db :eavt)
              :commit-id (v/commit-id db)
              :as-db-datoms (datom-set as-db :eavt)
              :as-db-commit-id (v/commit-id as-db)
              :as-db-branch (get-in as-db [:config :branch])
              :history-commits (mapv v/commit-id history)
              :as-of (datom-set (d/as-of db first-tx) :eavt)
              :history-count (count (d/datoms (d/history db) :eavt))})
           (finally (d/release conn)))))

     (defn- with-raw-store
       "Calls `f` with the store of a connection to `branch`, then releases it."
       [cfg branch f]
       (let [conn (d/connect (assoc cfg :branch branch))]
         (try (f (:store @conn)) (finally (d/release conn)))))

     (defn- compact-heads!
       "Rewrites each branch key as the compact head naming its commit."
       [cfg branches]
       (with-raw-store cfg :db
         (fn [store]
           (doseq [b branches]
             (let [cid (get-in (k/get store b nil {:sync? true}) [:meta :datahike/commit-id])]
               (k/assoc store b (dw/compact-head cid) {:sync? true}))))))

     (defn- full-heads!
       "Rewrites each branch key as the full record of its commit: the head
       format written before compact heads."
       [cfg branches]
       (with-raw-store cfg :db
         (fn [store]
           (doseq [b branches]
             (let [cid (get-in (k/get store b nil {:sync? true}) [:meta :datahike/commit-id])]
               (k/assoc store b (assoc-in (k/get store cid nil {:sync? true}) [:config :branch] b)
                        {:sync? true}))))))

     (defn- seeded-store! [cfg]
       (d/delete-database cfg)
       (d/create-database cfg)
       (let [conn (d/connect cfg)]
         (d/transact conn [{:db/ident :item/name :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                           {:db/ident :item/n :db/valueType :db.type/long
                            :db/cardinality :db.cardinality/one}])
         (doseq [i (range 40)] (d/transact conn [{:item/name (str "i" i) :item/n i}]))
         (doseq [i (range 0 40 3)] (d/transact conn [{:item/name (str "i" i) :item/n (- i)}]))
         (v/branch! conn :db :feat)
         (d/release conn))
       (let [conn (d/connect (assoc cfg :branch :feat))]
         (d/transact conn [{:item/name "feat-only" :item/n 1000}])
         (d/release conn)))

     (deftest a-compact-head-reads-as-its-commit
       (let [cfg compact-cfg]
         (seeded-store! cfg)
         (full-heads! cfg [:db :feat])
         (with-raw-store cfg :db
           (fn [store] (is (dw/stored-db? (k/get store :feat nil {:sync? true})))))
         (let [full {:db (observe-branch cfg :db) :feat (observe-branch cfg :feat)}]
           (compact-heads! cfg [:db :feat])
           (with-raw-store cfg :db
             (fn [store]
               (is (dw/compact-head? (k/get store :feat nil {:sync? true}))
                   "the branch key holds only the commit it names")))
           (testing "connect, branch-as-db, branch-history, as-of and history read the named commit"
             (is (= full {:db (observe-branch cfg :db) :feat (observe-branch cfg :feat)})))
           (testing "branch! from a compact head"
             (let [conn (d/connect (assoc cfg :branch :feat))]
               (v/branch! conn :feat :feat2)
               (d/release conn))
             (let [feat2 (observe-branch cfg :feat2)]
               (is (= (:datoms (:feat full)) (:datoms feat2)))
               (is (= (:commit-id (:feat full)) (:commit-id feat2)))
               (is (= :feat2 (:as-db-branch feat2)))))
           (testing "merge! of a compact-head branch names its commit as parent"
             (let [conn (d/connect cfg)
                   report (v/merge! conn #{:feat} [{:item/name "feat-only" :item/n 1000}])]
               (is (contains? (v/parent-commit-ids (:db-after report))
                              (:commit-id (:feat full))))
               (d/release conn)))
           (testing "gc marks through compact heads: nothing reachable is swept"
             (compact-heads! cfg [:db :feat :feat2])
             (let [before {:db (observe-branch cfg :db) :feat (observe-branch cfg :feat)}
                   conn (d/connect cfg)]
               (<?? S (d/gc-storage conn))
               (d/release conn)
               (is (= before {:db (observe-branch cfg :db) :feat (observe-branch cfg :feat)}))))
           (testing "a compact head naming a missing record fails loudly"
             (with-raw-store cfg :db
               (fn [store] (k/assoc store :feat2 (dw/compact-head (java.util.UUID/randomUUID)) {:sync? true})))
             (let [thrown (try (observe-branch cfg :feat2) nil
                               (catch Throwable e e))]
               (is (= :head-commit-missing
                      (some (comp :type ex-data) (take-while some? (iterate ex-cause thrown))))))))
         (d/delete-database cfg)))))
