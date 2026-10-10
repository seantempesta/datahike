(ns datahike.test.writer-commit-cost-test
  "Every report a commit makes durable carries that commit's cost, measured on
   the one platform thread that ran it, with the batch size it was shared by.
   A `:write-fn-map` is the connection's runtime: it never enters storage."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.writing :as dw]))

(defn- temp-dir
  "A store path that does not exist yet, inside a fresh temporary directory."
  []
  (str (java.nio.file.Files/createTempDirectory
        "datahike-commit-cost" (make-array java.nio.file.attribute.FileAttribute 0))
       "/store"))

(defn- delete-tree! [path]
  (doseq [f (reverse (file-seq (.getParentFile (io/file path))))] (io/delete-file f true)))

(deftest a-write-fn-map-never-enters-storage
  (let [path (temp-dir)
        seen (atom 0)
        cfg {:store {:backend :file :path path :id (random-uuid)}
             :schema-flexibility :read
             :writer {:backend :self
                      :write-fn-map {'transact! (fn [db argument]
                                                  (swap! seen inc)
                                                  (dw/transact! db argument))}}}]
    (try
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        @(d/transact! conn [{:db/id -1 :name "x"}])
        (d/release conn))
      (let [conn (d/connect cfg)]
        (is (= #{["x"]} (d/q '[:find ?n :where [_ :name ?n]] @conn)) "the commit is readable after a reconnect")
        (d/release conn))
      (is (= 1 @seen) "the supplied operation ran")
      (finally (delete-tree! path)))))

(deftest every-report-carries-the-cost-of-its-commit
  (doseq [backend [:memory :file]]
    (testing (name backend)
      (let [path (temp-dir)
            cfg {:store (cond-> {:backend backend :id (random-uuid)}
                          (= :file backend) (assoc :path path))
                 :schema-flexibility :read
                 ;; a long pause after each commit queues the next
                 ;; transactions into one batch
                 :writer {:backend :self :commit-wait-time 300}}]
        (try
          (d/create-database cfg)
          (let [conn (d/connect cfg)
                first-report @(d/transact! conn [{:db/id -1 :n 0}])
                pending (mapv #(d/transact! conn [{:db/id -1 :n (inc %)}]) (range 5))
                reports (mapv deref pending)
                costs (map :datahike/commit (cons first-report reports))]
            (is (every? (comp pos? :alloc-bytes) costs) "the commit thread allocated")
            (is (every? (comp nat-int? :cpu-ns) costs))
            (is (every? #(= (get-in % [:tx-meta :db/commitId]) (get-in % [:datahike/commit :commit-id]))
                        (cons first-report reports))
                "the cost names the commit the report landed in")
            (is (every? (fn [[_ members]] (every? #(= (count members) (:tx-count (:datahike/commit %))) members))
                        (group-by #(get-in % [:datahike/commit :commit-id]) (cons first-report reports)))
                "a batch's size is how many reports share its commit")
            (is (< 1 (apply max (map :tx-count costs))) "the queued transactions committed together")
            (d/release conn))
          (finally (delete-tree! path)))))))
