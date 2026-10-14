(ns datahike.test.node-not-found-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]))

(deftest node-not-found-carries-the-store-id-not-the-store
  (let [id  (random-uuid)
        cfg {:store {:backend :memory :id id}}
        _   (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      (let [storage (-> @conn :store :storage)
            address (random-uuid)
            data    (try (.restore ^org.replikativ.persistent_sorted_set.IStorage storage address)
                         (catch Exception e (ex-data e)))]
        (is (= :node-not-found (:type data)))
        (is (= address (:address data)))
        (is (= id (:store data))))
      (finally (d/release conn) (d/delete-database cfg)))))
