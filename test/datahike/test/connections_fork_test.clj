(ns datahike.test.connections-fork-test
  "datahike.connections/after-fork!: a forked process forgets its parent's connections and
  keeps each node cache, which its own first connect to that store adopts."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.connections :as connections]))

(deftest a-fork-keeps-each-node-cache-and-no-connection
  (let [cache-a (atom :a) cache-b (atom :b)
        parent {:conn-a {:conn :connection-a :count 2 :acquisition-key :ka :physical-store-key :pa
                         :node-store-key :store-a :threshold 10 :node-cache cache-a :write-hooks (atom {})}
                :conn-b {:conn :connection-b :count 1 :acquisition-key :kb :physical-store-key :pb
                         :node-store-key :store-b :threshold 10 :node-cache cache-b :write-hooks (atom {})}
                ;; one still opening in the parent has no cache worth keeping yet
                :conn-c {:opening? true :count 0 :acquisition-key :kc :node-store-key :store-c :threshold 10}}]
    (binding [connections/*connections* (atom parent)]
      (connections/after-fork!)
      (let [after @connections/*connections*]
        (testing "no connection is kept, none counted"
          (is (not-any? :conn (vals after)))
          (is (every? #(zero? (:count %)) (vals after)))
          (is (nil? (connections/active-connection :conn-a))))
        (testing "each node cache is kept, the same object, held by node store and threshold"
          (is (= #{cache-a cache-b} (into #{} (map :node-cache) (vals after))))
          (is (= #{[:store-a 10] [:store-b 10]}
                 (into #{} (map (juxt :node-store-key :threshold)) (vals after)))))
        (testing "the first connect to that store adopts its parent's cache; another store gets a fresh one"
          (let [adopted (connections/reserve-connection-opening!
                         :conn-a :completion :ka :pa :store-a 10 (fn [] :fresh))
                other (connections/reserve-connection-opening!
                       :conn-x :completion :kx :px :store-x 10 (fn [] :fresh))]
            (is (= :owner (:state adopted)))
            (is (identical? cache-a (:node-cache adopted)))
            (is (= :fresh (:node-cache other)))))))))
