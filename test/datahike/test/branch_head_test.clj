(ns datahike.test.branch-head-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.versioning :as v]
            [datahike.writing :as writing]
            [konserve.core :as k]))

(deftest branches-share-the-selected-commit
  (doseq [commit-graph? [true false]]
    (let [config {:store {:backend :memory :id (random-uuid)}
                  :schema-flexibility :read :commit-graph? commit-graph?}]
      (d/create-database config)
      (let [connection (d/connect config)]
        (try
          (d/transact connection [{:item/name "source"}])
          (let [commit (v/commit-id @connection)]
            (doseq [[from branch] (cond-> [[:db :from-branch]]
                                   commit-graph? (conj [commit :from-commit]))]
              (d/branch! connection from branch)
              (let [head (k/get (:store @connection) branch nil {:sync? true})
                    reopened (d/connect (assoc config :branch branch))]
                (try
                  (is (= commit-graph? (boolean (writing/compact-head? head))))
                  (is (= commit (v/commit-id @reopened)))
                  (is (= branch (get-in @reopened [:config :branch])))
                  (is (= #{["source"]} (d/q '[:find ?name :where [_ :item/name ?name]] @reopened)))
                  (d/transact reopened [{:item/name "child"}])
                  (is (= #{["source"]} (d/q '[:find ?name :where [_ :item/name ?name]] @connection)))
                  (finally (d/release reopened))))))
          (finally
            (d/release connection)
            (d/delete-database config)))))))
