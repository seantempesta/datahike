(ns datahike.test.content-id-test
  "Content entity ids: an entity whose :db/contentId attribute holds a value is the entity at
  content-eid of [attribute value], however deep its map sits in the transaction, and one
  identity in one transaction is one entity."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.constants :refer [ce0 emax tx0]]
            [datahike.db.transaction :refer [content-eid]]
            [datahike.versioning :as v]))

(def ^:private schema
  [{:db/ident :agent/key :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity :db/contentId true}
   {:db/ident :agent/turn :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :agent/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :agent/parent :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :msg/id :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity :db/contentId true}
   {:db/ident :msg/body :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :msg/from :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :msg/to :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   {:db/ident :msg/part :db/valueType :db.type/ref :db/cardinality :db.cardinality/one
    :db/isComponent true}])

(defn- cfg [tx]
  {:store {:backend :memory :id (random-uuid)}
   :keep-history? false
   :schema-flexibility :write
   :initial-tx tx})

(defn- with-conn [tx f]
  (let [c (cfg tx)]
    (d/create-database c)
    (let [conn (d/connect c)]
      (try (f conn) (finally (d/release conn) (d/delete-database c))))))

(defn- refusal [f]
  (try (f) nil
       (catch Exception e (some (comp :error ex-data) (take-while some? (iterate ex-cause e))))))

(defn- eav
  "The datoms a report added, as [e a v]."
  [r]
  (set (keep (fn [d] (when (:added d) [(:e d) (:a d) (:v d)])) (:tx-data r))))

(defn- agent-eid [db k] (:db/id (d/entity db [:agent/key k])))

(def ^:private a (content-eid :agent/key "a"))
(def ^:private b (content-eid :agent/key "b"))
(def ^:private c (content-eid :agent/key "c"))
(def ^:private m (content-eid :msg/id "m"))

(deftest a-content-eid-is-a-content-id
  (is (<= ce0 a emax))
  (is (not= a b)))

(deftest a-nested-map-takes-its-content-id
  (testing "under a cardinality-one ref"
    (with-conn schema
      (fn [conn]
        (let [r (d/transact conn [{:msg/id "m" :msg/body "x" :msg/from {:agent/key "a"}}])]
          (is (= a (agent-eid @conn "a")))
          (is (= #{[m :msg/id "m"] [m :msg/body "x"] [m :msg/from a] [a :agent/key "a"]}
                 (disj (eav r) (first (filter #(= :db/txInstant (second %)) (eav r))))))))))
  (testing "inside a vector of maps under a cardinality-many ref"
    (with-conn schema
      (fn [conn]
        (d/transact conn [{:msg/id "m" :msg/to [{:agent/key "a"} {:agent/key "b"}]}])
        (is (= #{a b} (set (map :db/id (:msg/to (d/entity @conn m)))))))))
  (testing "under a component ref"
    (with-conn schema
      (fn [conn]
        (d/transact conn [{:agent/key "a" :agent/name "x"}
                          {:msg/id "n" :msg/part {:msg/id "m" :msg/from [:agent/key "a"]}}])
        (is (= m (:db/id (:msg/part (d/entity @conn [:msg/id "n"]))))))))
  (testing "under a reverse ref"
    (with-conn schema
      (fn [conn]
        (d/transact conn [{:agent/key "a" :msg/_from {:msg/id "m" :msg/body "x"}}])
        (is (= #{[m a]} (d/q '[:find ?m ?a :where [?m :msg/from ?a] [?m :msg/body "x"]] @conn))))))
  (testing "three levels deep"
    (with-conn schema
      (fn [conn]
        (d/transact conn [{:msg/id "m" :msg/from {:agent/key "a" :agent/parent {:agent/key "b" :agent/parent {:agent/key "c"}}}}])
        (is (= [a b c] (map #(agent-eid @conn %) ["a" "b" "c"])))
        (is (= c (-> (d/entity @conn b) :agent/parent :db/id))))))
  (testing "with a tempid of its own, named by a ref before its map"
    (with-conn schema
      (fn [conn]
        (let [r (d/transact conn [{:msg/id "n" :msg/from "t"}
                                  {:msg/id "m" :msg/to [{:db/id "t" :agent/key "a"}]}])]
          (is (= a (get-in r [:tempids "t"])))
          (is (= a (-> (d/entity @conn [:msg/id "n"]) :msg/from :db/id))))))))

(deftest one-identity-in-one-transaction-is-one-entity
  (testing "a nested map, then a top-level map, of one identity"
    (with-conn schema
      (fn [conn]
        (d/transact conn [{:msg/id "m" :msg/body "x" :msg/from {:agent/key "a"}}
                          {:agent/key "a" :agent/turn 1}])
        (is (= {:db/id a :agent/key "a" :agent/turn 1} (d/pull @conn '[*] a))))))
  (testing "a top-level map, then a nested map"
    (with-conn schema
      (fn [conn]
        (d/transact conn [{:agent/key "a" :agent/turn 1}
                          {:msg/id "m" :msg/body "x" :msg/from {:agent/key "a"}}])
        (is (= {:db/id a :agent/key "a" :agent/turn 1} (d/pull @conn '[*] a))))))
  (testing "two nested maps, their attributes merged"
    (with-conn schema
      (fn [conn]
        (d/transact conn [{:msg/id "m" :msg/from {:agent/key "a" :agent/turn 1}}
                          {:msg/id "n" :msg/from {:agent/key "a" :agent/name "x"}}])
        (is (= {:db/id a :agent/key "a" :agent/turn 1 :agent/name "x"} (d/pull @conn '[*] a)))
        (is (= 1 (count (d/datoms @conn :avet :agent/key)))))))
  (testing "two maps of one identity in one cardinality-many value"
    (with-conn schema
      (fn [conn]
        (d/transact conn [{:msg/id "m" :msg/to [{:agent/key "a"} {:agent/key "a" :agent/turn 2} {:agent/key "b"}]}])
        (is (= #{a b} (set (map :db/id (:msg/to (d/entity @conn m))))))
        (is (= 2 (:agent/turn (d/entity @conn a))))))))

(deftest an-identity-held-at-a-counter-id-keeps-it
  ;; data written before its attribute gained :db/contentId: an upsert finds the entity by
  ;; its value first, so no second entity is made and nothing is refused
  (with-conn (mapv #(dissoc % :db/contentId) schema)
    (fn [conn]
      (d/transact conn [{:agent/key "a"}])
      (let [old (agent-eid @conn "a")]
        (is (< old tx0))
        (d/transact conn [{:db/ident :agent/key :db/contentId true}])
        (d/transact conn [{:msg/id "m" :msg/from {:agent/key "a" :agent/turn 1}}
                          {:agent/key "a" :agent/name "x"}
                          {:msg/id "n" :msg/from {:agent/key "b"}}])
        (is (= old (agent-eid @conn "a")))
        (is (= {:db/id old :agent/key "a" :agent/turn 1 :agent/name "x"} (d/pull @conn '[*] old)))
        (is (= b (agent-eid @conn "b")))
        (is (= 2 (count (d/datoms @conn :avet :agent/key))))))))

(deftest one-identity-is-one-entity-on-every-branch
  ;; a branch made before either transaction mints the nested identities at the same ids
  (with-conn schema
    (fn [conn]
      (v/branch! conn :db :other)
      (let [other (d/connect (assoc (:config @conn) :branch :other))
            tx [{:msg/id "m" :msg/from {:agent/key "a" :agent/parent {:agent/key "b"}}}
                {:msg/id "n" :msg/to [{:agent/key "c"}]}]]
        (try
          (let [r1 (d/transact conn tx)
                r2 (d/transact other tx)
                ids (fn [db] (mapv #(agent-eid db %) ["a" "b" "c"]))
                strip (fn [r] (set (remove #(= :db/txInstant (second %)) (eav r))))]
            (is (= [a b c] (ids @conn) (ids @other)))
            (is (= (strip r1) (strip r2))))
          (finally (d/release other)))))))

(deftest an-identity-is-refused-a-second-value
  (with-conn schema
    (fn [conn]
      (d/transact conn [{:agent/key "a"}])
      (is (= :entity-id/collision (refusal #(d/transact conn [[:db/add a :agent/key "b"]])))))))
