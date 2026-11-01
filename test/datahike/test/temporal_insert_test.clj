(ns datahike.test.temporal-insert-test
  "A datom asserted and retracted in one transaction leaves both entries in the
  temporal indexes (upstream 5bacf93b, #949). The temporal indexes order by
  [e a v tx added]; -temporal-insert passed the current comparator, [e a v tx], under
  which the two compare equal, so the second insert was a no-op and the retraction
  was lost from the stored history. Read from the trees, since the history view
  unions in the current datoms."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.test.utils :as utils]))

(defn- with-conn [f]
  (let [conn (utils/setup-db {:store {:backend :memory :id (random-uuid)}
                              :keep-history? true :schema-flexibility :write})]
    (try
      (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one
                         :db/unique :db.unique/identity}
                        {:db/ident :tag :db/valueType :db.type/keyword
                         :db/cardinality :db.cardinality/many}
                        {:db/ident :mood :db/valueType :db.type/keyword
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:db/id -1 :name "a"}])
      (f conn (:e (first (d/datoms @conn :avet :name "a"))))
      (finally (d/release conn)))))

(defn- temporal-entries [db tree a v]
  (vec (filter #(and (= a (:a %)) (= v (:v %))) (seq (get db tree)))))

(deftest assert-and-retract-in-one-transaction-keeps-both-entries
  (doseq [a [:tag :mood]]
    (testing (str a)
      (with-conn
        (fn [conn e]
          (d/transact conn [[:db/add e a :x] [:db/retract e a :x]])
          (let [db @conn]
            (is (empty? (d/datoms db :eavt e a :x)) "the current index holds no datom")
            (doseq [tree [:temporal-eavt :temporal-aevt]]
              (testing (name tree)
                (is (= #{true false} (set (map :added (temporal-entries db tree a :x))))
                    "one assertion and one retraction")))
            (is (= #{true false}
                   (set (map :added (filter #(and (= a (:a %)) (= :x (:v %)))
                                            (d/datoms (d/history db) :eavt e)))))
                "the history view carries both")))))))
