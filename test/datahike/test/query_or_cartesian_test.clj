(ns datahike.test.query-or-cartesian-test
  "An or branch is handed only the relations it shares a variable with (upstream
  8028d1f4, #956). execute-or passed the caller's whole relation set to each branch,
  which folded it in with hash-join, a Cartesian product on relations sharing no
  attribute: a rule that is an or over two named sub-rules multiplied by the :in
  collection once per nested or level. The answers were right, so the test counts
  the Cartesian joins (with-redefs on hash-join, the one place a join is made)."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.set :as set]
            [datahike.api :as d]
            [datahike.query.relation :as rel]))

(def ^:private schema
  [{:db/ident :concept/id :db/valueType :db.type/string
    :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}
   {:db/ident :relation/from :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :relation/to :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :relation/type :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

(def ^:private rules
  '[[(-fwd ?from ?type ?to ?r)
     [(ground ["broader" "related"]) [?type ...]]
     [?r :relation/from ?from]
     [?r :relation/type ?type]
     [?r :relation/to ?to]]
    [(-rev ?from ?type ?to ?r)
     [(ground ["narrower" "related"]) [?type ...]]
     [?r :relation/to ?from]
     [?r :relation/type ?type]
     [?r :relation/from ?to]]
    [(edge ?from ?type ?to ?r)
     (or (-fwd ?from ?type ?to ?r)
         (-rev ?from ?type ?to ?r))]])

(defn- cid [i] (str "c" (+ 1000 i)))

(defn- conn-with [n fanout]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? false :schema-flexibility :write}]
    (d/create-database cfg)
    (let [c (d/connect cfg)]
      (d/transact c schema)
      (d/transact c (vec (for [i (range n)] {:concept/id (cid i)})))
      (d/transact c (vec (for [i (range n) k (range 1 (inc fanout))
                               :let [j (mod (+ i k) n)]]
                           {:relation/from [:concept/id (cid i)]
                            :relation/type "broader"
                            :relation/to [:concept/id (cid j)]})))
      c)))

(defn- run [db k]
  (let [n (atom 0)
        orig rel/hash-join
        rows (with-redefs [rel/hash-join
                           (fn [r1 r2]
                             (when (empty? (set/intersection (set (keys (:attrs r1)))
                                                             (set (keys (:attrs r2)))))
                               (swap! n inc))
                             (orig r1 r2))]
               (count (d/q {:query '{:find [?from-id ?id]
                                     :in [$ % [?from-id ...] ?rt]
                                     :where [[?c :concept/id ?from-id]
                                             (edge ?c ?rt ?related ?r)
                                             [?related :concept/id ?id]]}
                            :args [db rules (into #{} (map cid) (range k)) "broader"]})))]
    [rows @n]))

(deftest or-branches-are-not-handed-disconnected-relations
  (let [c (conn-with 40 3)]
    (try
      (doseq [k [5 20 40]]
        (testing (str k " from-ids")
          (let [[rows cartesians] (run @c k)]
            (is (= (* 3 k) rows) "k from-ids x fanout 3")
            (is (zero? cartesians) "no or branch joined a relation it shares no variable with"))))
      (finally (d/release c)))))
