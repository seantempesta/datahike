(ns datahike.test.overlay-test
  "A `:datahike/overlay` store keeps every write in its own memory frontend and
   never mutates the shared store it reads through."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.connections :as connections]
            [konserve.memory :as km])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- disk [dir]
  (let [fs (filter #(.isFile ^java.io.File %) (file-seq (io/file dir)))]
    {:files (count fs) :bytes (reduce + (map #(.length ^java.io.File %) fs))}))

(defn- overlay-config [base-cfg]
  (assoc base-cfg :store {:backend :datahike/overlay
                          :id (get-in base-cfg [:store :id])
                          :overlay (random-uuid)
                          :backend-config (:store base-cfg)}))

(deftest overlay-writes-never-reach-the-shared-store
  (let [dir (str (Files/createTempDirectory "overlay" (make-array FileAttribute 0)) "/store")
        base-cfg {:store {:backend :file :path dir :id (random-uuid)}
                  :keep-history? true :schema-flexibility :write}
        _ (d/create-database base-cfg)
        base (d/connect base-cfg)
        _ (d/transact base [{:db/ident :m/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
        _ (d/transact base (vec (for [i (range 200)] {:m/n i})))
        before (disk dir)
        count-n #(d/q '[:find (count ?e) . :where [?e :m/n]] %)]
    (try
      (let [cfg-a (overlay-config base-cfg)
            cfg-b (overlay-config base-cfg)
            a (d/connect cfg-a)
            _ (d/branch! a :db :member)
            member (d/connect (assoc cfg-a :branch :member))
            b (d/connect cfg-b)]
        (try
          (dotimes [i 20] (d/transact member [{:m/n (+ 1000 i)}]))
          (d/transact a [{:m/n 5000}])
          (testing "the overlay reads the shared store and its own writes"
            (is (= 220 (count-n @member)))
            (is (= 201 (count-n @a))))
          (testing "a second overlay of the same store is another world"
            (is (= 200 (count-n @b)))
            (is (not (contains? (set (d/branches b)) :member))))
          (testing "the shared store and its open connection see nothing"
            (is (= before (disk dir)))
            (is (= 200 (count-n @base)))
            (is (= #{:db} (set (d/branches base)))))
          (finally (d/release member) (d/release a) (d/release b)))
        (testing "deleting an overlay drops its writes and leaves the shared store"
          (d/delete-database cfg-a)
          (d/delete-database cfg-b)
          (is (= before (disk dir)))
          (let [again (d/connect cfg-a)]
            (try (is (= 200 (count-n @again))) (finally (d/release again)))
            (d/delete-database cfg-a))))
      (testing "after release the shared store is unchanged and still opens"
        (is (= before (disk dir)))
        (let [fresh (d/connect base-cfg)]
          (try (is (= 200 (count-n @fresh))) (finally (d/release fresh)))))
      (finally
        (d/release base)
        (d/delete-database base-cfg)))))

(defn- live-heap-bytes []
  (dotimes [_ 3] (System/gc) (Thread/sleep 100))
  (.getUsed (.getHeapMemoryUsage (java.lang.management.ManagementFactory/getMemoryMXBean))))

(defn- frontend-keys [cfg]
  (count @(:state (km/connect-mem-store (get-in cfg [:store :overlay]) {:sync? true}))))

(deftest overlays-read-through-their-backend-without-copies
  (let [dir (str (Files/createTempDirectory "overlay" (make-array FileAttribute 0)) "/store")
        base-cfg {:store {:backend :file :path dir :id (random-uuid)}
                  :keep-history? false :schema-flexibility :write
                  :store-cache-bytes 256000000}
        _ (d/create-database base-cfg)
        base (d/connect base-cfg)
        _ (d/transact base [{:db/ident :m/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                            {:db/ident :m/s :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
        _ (doseq [chunk (partition-all 10000 (range 40000))]
            (d/transact base (vec (for [i chunk] {:m/n i :m/s (str "value " i " of the shared store")}))))
        _ (d/release base) ; the last release drops the warm node cache
        base (d/connect base-cfg)
        scan #(count (d/datoms % :eavt))
        idle (live-heap-bytes)
        datoms (scan @base)
        base-scan (- (live-heap-bytes) idle)
        cfgs (vec (repeatedly 4 #(overlay-config base-cfg)))
        conns (mapv d/connect cfgs)]
    (try
      (is (every? #(= datoms (scan @%)) conns))
      (Thread/sleep 200) ; a read's frontend copy would land asynchronously
      (testing "every overlay shares its backend's one node cache"
        (is (= 1 (count (distinct (keep (fn [[[store-id] entry]]
                                          (when (= store-id (get-in base-cfg [:store :id])) (:node-cache entry)))
                                        @connections/*connections*))))))
      (testing "an overlay's frontend holds only its own writes, never what it read"
        (is (every? zero? (map frontend-keys cfgs))))
      (testing "four scanning overlays retain less than half of what one scan of the base retains"
        (is (< (- (live-heap-bytes) idle base-scan) (quot base-scan 2))))
      (testing "reads and writes stay correct and isolated"
        (d/transact (first conns) [{:m/n -1}])
        (is (< datoms (scan @(first conns))))
        (is (= datoms (scan @(second conns)) (scan @base)))
        (is (pos? (frontend-keys (first cfgs)))))
      (finally
        (doseq [c conns] (d/release c))
        (doseq [c cfgs] (d/delete-database c))
        (d/release base)
        (d/delete-database base-cfg)))))
