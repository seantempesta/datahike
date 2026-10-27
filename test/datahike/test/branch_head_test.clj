(ns datahike.test.branch-head-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.versioning :as v]
            [datahike.ids]
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

(deftest a-compact-head-names-its-commits-ids
  ;; set-ids! moves a store's id source past every branch head's ids, a walk of the roster: a
  ;; compact head carries its commit's :max-eid and :max-tx, so the walk reads each head alone and
  ;; never its commit record (a seon.db open read every head's whole record, 0.3 MB a branch)
  (let [config {:store {:backend :memory :id (random-uuid)} :schema-flexibility :read}]
    (d/create-database config)
    (let [connection (d/connect config)]
      (try
        (d/transact connection [{:item/name "source"}])
        (d/branch! connection :db :side)
        (let [side (d/connect (assoc config :branch :side))]
          (try
            (d/transact side [{:item/name "a"} {:item/name "b"}])
            (let [store (:store @connection)
                  head (k/get store :side nil {:sync? true})
                  {:keys [max-eid max-tx]} @side]
              (is (writing/compact-head? head))
              (is (= [max-eid max-tx] [(:max-eid head) (:max-tx head)]) "the head carries its commit's ids")
              ;; the commit record gone, the walk still answers: it never reads one
              (k/dissoc store (get-in head [:meta :datahike/commit-id]) {:sync? true})
              (let [reached (atom {})
                    ids (reify datahike.ids/IIds
                          (-take! [_ _] 0)
                          (-reach! [_ kind id] (swap! reached update kind (fnil max 0) id) nil))]
                (writing/set-ids! store (random-uuid) ids)
                (is (= {:eid max-eid :tx max-tx} @reached) "moved past the highest head's ids")))
            (finally (d/release side))))
        (finally
          (d/release connection)
          (d/delete-database config))))))
