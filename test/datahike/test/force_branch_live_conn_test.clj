(ns datahike.test.force-branch-live-conn-test
  "force-branch! moves a branch's head while a connection to that branch is live. A
   connection to the branch, made then or before, reads the moved head, and a write on it
   keeps what the move brought."
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.versioning :as v]))

(defn- setup []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true
             :initial-tx [{:db/ident :n :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}]
             :writer {:backend :sync}}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (v/branch! conn :db :foo)
      [cfg conn])))

(deftest a-write-after-force-branch-keeps-the-moved-head
  (let [[cfg conn] (setup)
        held (d/connect (assoc cfg :branch :foo))
        _ (d/transact conn [{:db/id 1 :n 1}])
        moved (d/db-with @conn [{:db/id 2 :n 2}])
        _ (d/force-branch! moved :foo #{(d/commit-id @conn)})
        again (d/connect (assoc cfg :branch :foo))]
    (is (= #{1 2} (set (map :v (d/datoms @again :aevt :n)))) "a connection made after the move reads it")
    (d/transact again [{:db/id 3 :n 3}])
    (is (= #{1 2 3} (set (map :v (d/datoms (v/branch-as-db conn :foo) :aevt :n))))
        "a write on it keeps what the move brought, though a connection from before the move is held")
    (d/release again) (d/release held) (d/release conn)))
