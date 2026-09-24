(ns datahike.test.fusion-root-pin-test
  "A flushed database value with fused index roots holds each root strongly.
   Under :fuse-index-roots? the root is inlined in the commit record and never
   written as its own object, so the live value is its only holder: clearing a
   Soft/Weak reference (what the collector does under heap pressure) and
   missing the node cache must not make the value unreadable."
  (:require [datahike.api :as d]
            [datahike.index :as di]
            [datahike.index.persistent-set :as ps]
            [datahike.versioning :as dv]
            [konserve.core :as k]
            [clojure.test :refer [deftest is testing]])
  (:import [java.lang.ref Reference]
           [org.replikativ.persistent_sorted_set PersistentSortedSet]))

(def ^:private index-keys
  [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet])

(defn- collect-and-evict
  "Each index of `db` as the collector and a cold node cache leave it: a root
   held through a java.lang.ref.Reference is cleared (a strong root survives),
   and the copy reads through a fresh storage whose node cache is empty."
  [db]
  (let [storage (ps/create-storage (:store db) (:config db))]
    (into {}
          (map (fn [k]
                 (let [^PersistentSortedSet idx (get db k)
                       root (.-_root idx)]
                   (when (instance? Reference root) (.clear ^Reference root))
                   [k (di/with-storage :datahike.index/persistent-set idx storage)])))
          index-keys)))

(defn- read-each [indexes]
  (into {}
        (map (fn [[k idx]]
               [k (try (count (seq idx))
                       (catch Throwable t (select-keys (ex-data t) [:type :address])))]))
        indexes))

(defn- unwritten-root? [db k]
  (not (k/exists? (:store db) (.-_address ^PersistentSortedSet (get db k)) {:sync? true})))

(deftest fused-roots-stay-readable-after-reference-clearing
  (let [cfg {:store {:backend :file
                     :path (str (System/getProperty "java.io.tmpdir") "/dh-fusion-root-pin-test")
                     :id (java.util.UUID/randomUUID)}
             :index :datahike.index/persistent-set
             :schema-flexibility :read :keep-history? true
             :fuse-index-roots? true}]
    (when (d/database-exists? cfg) (d/delete-database cfg))
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn (vec (for [i (range 50)] {:db/id (inc i) :n i})))
        (d/transact conn [{:db/id 1 :n 1000}])
        (testing "commit!: the connection's value reads every index"
          (let [db @conn
                expected (into {} (map (fn [k] [k (count (seq (get db k)))])) index-keys)]
            (is (every? #(unwritten-root? db %) index-keys)
                "the fused roots are not separate objects in the store")
            (is (= expected (read-each (collect-and-evict db))))
            (is (= 51 (d/q '[:find (count ?e) . :where [?e :n _]] db)))))
        (testing "force-branch!: the value it flushed reads every index"
          (let [db (d/db-with @conn [{:db/id 2 :n 2000}])
                expected (into {} (map (fn [k] [k (count (seq (get db k)))])) index-keys)]
            (dv/force-branch! db :pinned #{:db})
            (is (= expected (read-each (collect-and-evict db))))))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))
