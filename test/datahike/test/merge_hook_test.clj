(ns datahike.test.merge-hook-test
  "A merge is a transaction plus parents: it runs through the writer's own
   'transact!, so what a connection's `:write-fn-map` puts on transact holds for
   a merge too, unless the map supplies its own 'merge!."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.versioning :as v]
            [datahike.writing :as dw]))

(defn- conn-with [backend write-fn-map]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true
             :initial-tx [{:db/ident :n :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}]
             :writer {:backend backend :write-fn-map write-fn-map}}]
    (d/create-database cfg)
    [cfg (d/connect cfg)]))

(defn- outcome
  "What f did: its value, or the :error of the first ex-data on the thrown chain."
  [f]
  (try (let [r (f)]
         (if (instance? Throwable r)
           {:refused (some (comp :error ex-data) (take-while some? (iterate ex-cause r)))}
           {:value r}))
       (catch Exception e
         {:refused (some (comp :error ex-data) (take-while some? (iterate ex-cause e)))})))

(defn- checked-transact
  "A transact hook: records each arg-map it sees and refuses one asserting
   :n -1."
  [seen]
  (fn [db arg-map]
    (swap! seen conj arg-map)
    (when (some #(= -1 (:n %)) (:tx-data arg-map))
      (throw (ex-info "refused by the hook" {:error :hook/refused})))
    (dw/transact! db arg-map)))

(deftest a-merge-runs-through-the-hooked-transact
  (doseq [backend [:self :sync]]
    (testing (name backend)
      (let [seen (atom [])
            [cfg conn] (conn-with backend {'transact! (checked-transact seen)})]
        (v/branch! conn :db :foo)
        (let [foo (d/connect (assoc cfg :branch :foo))]
          (d/transact foo [{:db/id 1 :n 1}])
          (d/release foo))
        (reset! seen [])
        (testing "a merge passes the hook, with its arg-map as given"
          (let [r (outcome #(v/merge! conn {:parents #{:foo} :tx-data [{:db/id 1 :n 1}]}))]
            (is (contains? r :value))
            (is (= 1 (count @seen)))
            (is (= #{:foo} (:parents (first @seen))))
            (is (= 2 (count (v/parent-commit-ids @conn))) "the merge commit has both parents")))
        (testing "a merge the hook refuses is not written"
          (let [head (d/commit-id @conn)
                r (outcome #(v/merge! conn #{:foo} [{:db/id 1 :n -1}]))]
            (is (= :hook/refused (:refused r)))
            (is (= head (d/commit-id @conn)))
            (is (= 1 (:n (d/pull @conn [:n] 1))))))
        (d/release conn)))))

(deftest a-supplied-merge-is-kept
  (doseq [backend [:self :sync]]
    (testing (name backend)
      (let [seen (atom [])
            merges (atom 0)
            [cfg conn] (conn-with backend {'transact! (checked-transact seen)
                                           'merge! (fn [db arg-map]
                                                     (swap! merges inc)
                                                     (dw/merge-writer! db arg-map))})]
        (v/branch! conn :db :foo)
        (reset! seen [])
        (v/merge! conn #{:foo} [{:db/id 1 :n 3}])
        (is (= 1 @merges) "the supplied merge ran")
        (is (empty? @seen) "and the merge it supplied does not pass the transact hook")
        (is (= 3 (:n (d/pull @conn [:n] 1))))
        (d/release conn)))))
