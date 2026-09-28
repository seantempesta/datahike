(ns datahike.test.overlay-test
  "A `:datahike/overlay` store keeps every write in its own memory frontend and
   never mutates the shared store it reads through."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d])
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
