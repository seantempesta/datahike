(ns datahike.test.datoms-diff-test
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.versioning :as v]))

(defn- config []
  {:store {:backend :memory :id (random-uuid)} :keep-history? true :schema-flexibility :write
   :index-config {:branching-factor 32 :diff-buf-size 8}
   :initial-tx [{:db/ident :k/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                {:db/ident :k/tags :db/valueType :db.type/string :db/cardinality :db.cardinality/many}]})

(defn- tuples [db attribute]
  (into #{} (map (juxt :e :a :v :tx)) (d/datoms db {:index :aevt :components [attribute]})))

(defn- plain [a b attribute]
  (let [ta (tuples a attribute) tb (tuples b attribute)]
    [(set/difference ta tb) (set/difference tb ta)]))

(defn- answered [a b attribute]
  (mapv #(into #{} (map (juxt :e :a :v :tx)) %) (d/datoms-diff a b :aevt [attribute])))

(deftest the-diff-of-two-commits-is-the-set-difference-of-their-datoms
  ;; A deep tree, then small transactions on two branches, so buffered slot edits and shared subtrees both occur.
  (doseq [seed (range 4)]
    (let [rnd (java.util.Random. seed) cfg (config) _ (d/create-database cfg) main (d/connect cfg)]
      (d/transact main (vec (for [i (range 3000)] {:k/n (long i) :k/tags [(str "t" (mod i 7)) (str "u" (mod i 11))]})))
      (d/branch! main :db :chain)
      (let [chain (d/connect (assoc cfg :branch :chain)) commits (atom [(d/commit-id @main)])]
        (dotimes [i 40]
          (let [conn (if (zero? (.nextInt rnd 2)) main chain)
                some-e (:e (nth (vec (d/datoms @conn {:index :aevt :components [:k/n]})) (.nextInt rnd 2000)))]
            (d/transact conn (cond-> [{:k/n (long (+ 10000 (.nextInt rnd 100000))) :k/tags [(str "n" i)]}]
                               (zero? (.nextInt rnd 3)) (conj [:db/retractEntity some-e])
                               (zero? (.nextInt rnd 2)) (conj [:db/add some-e :k/tags (str "x" i)])))
            (swap! commits conj (d/commit-id @conn))))
        (let [dbs (mapv #(v/commit-as-db main %) (take 10 (shuffle @commits)))]
          (doseq [a dbs b dbs attribute [:k/n :k/tags]]
            (is (= (plain a b attribute) (answered a b attribute)))))
        (testing "a value against itself differs in nothing"
          (is (= [[] []] (d/datoms-diff @chain @chain :aevt [:k/tags]))))
        (testing "the other attribute's changes are outside the range"
          (d/transact chain [{:k/n 7777777}])
          (is (= [[] []] (d/datoms-diff @main @chain :aevt [:db/ident]))))))))

(deftest a-diff-of-one-changed-leaf-reads-that-leaf-not-the-range
  (let [cfg (config) _ (d/create-database cfg) main (d/connect cfg)]
    (d/transact main (vec (for [i (range 20000)] {:k/n (long i)})))
    (d/branch! main :db :chain)
    (let [chain (d/connect (assoc cfg :branch :chain))
          e (:e (first (d/datoms @chain {:index :aevt :components [:k/n]})))]
      (d/transact chain [[:db/add e :k/n -1]])
      (let [[only-a only-b] (d/datoms-diff @main @chain :aevt [:k/n])]
        (is (= [[e :k/n 0]] (mapv (juxt :e :a :v) only-a)))
        (is (= [[e :k/n -1]] (mapv (juxt :e :a :v) only-b)))))))
