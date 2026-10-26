(ns datahike.test.time-predicate-order-test
  "A db read through a time predicate (as-of, since) answers its datoms in index order and
  lazily: post-process-datoms grouped them by [e a] into a map, so past eight groups the order
  was the hash's, and over rseek-datoms' open range it read the index down to its start."
  (:require
   [clojure.test :refer [is deftest testing]]
   [datahike.api :as d]))

(defn- with-family
  "f on a :keep-history? connection holding root and ten children, each written in its own
  transaction with [:parent root], and the db after each child: (f conn root dbs)."
  [f]
  (let [cfg {:store {:backend :memory :id (random-uuid)} :keep-history? true :schema-flexibility :write}]
    (d/create-database (assoc cfg :initial-tx [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                                               {:db/ident :parent :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
                                               {:db/ident :tag :db/valueType :db.type/string :db/cardinality :db.cardinality/many}
                                               {:db/ident :rank :db/valueType :db.type/long :db/cardinality :db.cardinality/one}]))
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn [{:name "root"}])
        (let [root (:db/id (d/entity @conn [:name "root"]))
              dbs (vec (for [i (range 10)] (:db-after (d/transact conn [{:name (str "c" i) :parent root}]))))]
          (f conn root dbs))
        (finally (d/release conn) (d/delete-database cfg))))))

(defn- es [datoms] (mapv :e datoms))

(deftest a-time-predicate-read-keeps-index-order
  (with-family
    (fn [conn root dbs]
      (let [kid #(:db/id (d/entity @conn [:name (str "c" %)]))
            asof (d/as-of @conn (:max-tx (dbs 5)))]
        (testing "as of the sixth child"
          (is (= (mapv kid (range 6)) (es (d/datoms asof :avet :parent root)))
              "datoms: the six children then, in index order")
          (is (= (mapv kid (reverse (range 6))) (es (take-while #(= root (:v %)) (d/rseek-datoms asof :avet :parent root))))
              "rseek-datoms: the same, last first")
          (is (= (kid 5) (:e (first (d/rseek-datoms asof :avet :parent root))))
              "rseek's first datom is the last child, read from the seek point")
          (is (= (mapv kid (reverse (range 3 6))) (mapv :db/id (:_parent (d/pull asof '[{[:_parent :limit 3 :reverse true] [:db/id]}] root))))
              "pull's newest three, read by rseek"))
        (testing "a since db holds what came after, in order"
          (let [since (d/since @conn (:max-tx (dbs 5)))]
            (is (= (mapv kid (range 6 10)) (es (d/datoms since :avet :parent root))))))
        (testing "eavt over every child: entity order past eight groups"
          (is (= (into [root] (map kid) (range 10)) (es (d/datoms (d/as-of @conn (:max-tx (dbs 9))) :aevt :name)))
              "eleven names (root's and ten children's) as of the last child, in :aevt order")
          (is (= (sort (es (d/datoms (d/as-of @conn (:max-tx (dbs 9))) :avet :parent root))) (es (d/datoms (d/as-of @conn (:max-tx (dbs 9))) :avet :parent root)))))))))

(deftest a-value-as-of-is-the-one-held-then
  (with-family
    (fn [conn root _]
      (let [c0 (:db/id (d/entity @conn [:name "c0"]))
            t-rank-1 (:max-tx (:db-after (d/transact conn [[:db/add c0 :rank 1]])))
            t-rank-2 (:max-tx (:db-after (d/transact conn [[:db/add c0 :rank 2]])))
            t-tags (:max-tx (:db-after (d/transact conn [[:db/add c0 :tag "a"] [:db/add c0 :tag "b"]])))
            t-untag (:max-tx (:db-after (d/transact conn [[:db/retract c0 :tag "a"]])))
            _ (d/transact conn [[:db/retract c0 :parent root]])
            vals-at (fn [t a] (mapv :v (d/datoms (d/as-of @conn t) :eavt c0 a)))]
        (is (= [1] (vals-at t-rank-1 :rank)) "card-one: the value of its time")
        (is (= [2] (vals-at t-rank-2 :rank)) "card-one replaced: the new value only")
        (is (= ["a" "b"] (vals-at t-tags :tag)) "card-many: both, in value order")
        (is (= ["b"] (vals-at t-untag :tag)) "card-many retracted: the rest")
        (is (= [root] (vals-at t-untag :parent)) "a ref retracted later is held before")
        (is (= [] (mapv :v (d/datoms @conn :eavt c0 :parent))) "and gone now")
        (is (= {:rank 2 :tag ["b"]} (select-keys (d/pull (d/as-of @conn t-untag) '[:rank :tag] c0) [:rank :tag]))
            "pull as of then")))))
