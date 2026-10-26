(ns datahike.test.pull-reverse-test
  "An attribute's :reverse option in a pull pattern: its datoms read last in index order
  first (rseek-datoms), so [attr :limit n :reverse true] is its newest n, for a forward
  card-many attribute and a reverse ref alike, on current, as-of and history dbs."
  (:require
   [clojure.test :refer [is deftest testing]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.datom :as dd]
   [datahike.constants :refer [tx0]]))

(def schema
  {:parent {:db/valueType :db.type/ref}
   :friend {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :aka    {:db/cardinality :db.cardinality/many}
   :name   {}})

(defn- family-db [n] (db/init-db (cons (dd/datom 1 :name "root" tx0)
                                       (mapcat (fn [i] (let [e (+ 2 i)]
                                                         [(dd/datom e :parent 1 tx0) (dd/datom 1 :friend e tx0)
                                                          (dd/datom 1 :aka (format "a-%03d" i) tx0)]))
                                               (range n)))
                                 schema))

(deftest newest-n-is-the-last-n-in-index-order-last-first
  (let [db (family-db 30)
        all-children (map :db/id (:_parent (d/pull db '[{:_parent [:db/id]}] 1)))
        all-friends (map :db/id (:friend (d/pull db '[{:friend [:db/id]}] 1)))]
    (testing "a reverse ref"
      (is (= (take 5 (reverse all-children)) (map :db/id (:_parent (d/pull db '[{[:_parent :limit 5 :reverse true] [:db/id]}] 1)))))
      (is (= [31 30 29 28 27] (map :db/id (:_parent (d/pull db '[{[:_parent :limit 5 :reverse true] [:db/id]}] 1))))))
    (testing "a forward card-many ref, with and without a subpattern"
      (is (= (take 3 (reverse all-friends)) (map :db/id (:friend (d/pull db '[{[:friend :limit 3 :reverse true] [:db/id]}] 1)))))
      (is (= [{:db/id 31} {:db/id 30}] (:friend (d/pull db '[[:friend :limit 2 :reverse true]] 1)))))
    (testing "a forward card-many value"
      (is (= ["a-029" "a-028"] (:aka (d/pull db '[[:aka :limit 2 :reverse true]] 1)))))
    (testing "inside a wildcard"
      (is (= ["a-029" "a-028"] (:aka (d/pull db '[* [:aka :limit 2 :reverse true]] 1))))
      (is (= "root" (:name (d/pull db '[* [:aka :limit 2 :reverse true]] 1)))))
    (testing "with :as and a :limit given after it"
      (is (= ["a-029"] (:newest (d/pull db '[[:aka :reverse true :as :newest :limit 1]] 1)))))
    (testing "no limit: every member, last first"
      (is (= (reverse all-children) (map :db/id (:_parent (d/pull db '[{[:_parent :reverse true] [:db/id]}] 1))))))
    (testing ":reverse false is the pattern without it"
      (is (= (d/pull db '[{[:_parent :limit 5] [:db/id]}] 1) (d/pull db '[{[:_parent :limit 5 :reverse false] [:db/id]}] 1))))
    (testing "q's pull takes it too"
      (is (= [[{:_parent [{:db/id 31}]}]] (d/q '[:find (pull ?e [{[:_parent :limit 1 :reverse true] [:db/id]}]) :where [?e :name "root"]] db))))))

(deftest fewer-than-n-and-none
  (let [db (family-db 3)]
    (is (= [4 3 2] (map :db/id (:_parent (d/pull db '[{[:_parent :limit 50 :reverse true] [:db/id]}] 1))))
        "fewer than n: all of them, last first")
    (is (= {:db/id 2} (d/pull db '[:db/id {[:_parent :limit 5 :reverse true] [:db/id]}] 2))
        "an empty collection: absent, as without :reverse")
    (is (= {:db/id 2 :_parent :none} (d/pull db '[:db/id {[:_parent :limit 5 :reverse true :default :none] [:db/id]}] 2))
        "an empty collection takes its :default")))

(deftest an-invalid-reverse-value-is-refused
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #":reverse"
                        (d/pull (family-db 3) '[[:aka :reverse 1]] 1))))

(deftest as-of-history-and-a-retraction-in-the-window
  (let [cfg {:store {:backend :memory :id (random-uuid)} :keep-history? true :schema-flexibility :write :attribute-refs? false}
        _ (d/create-database (assoc cfg :initial-tx [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                                                    {:db/ident :parent :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}]))
        conn (d/connect cfg)
        _ (d/transact conn [{:name "root"}])
        root (:db/id (d/entity @conn [:name "root"]))
        ;; ten children, one transaction each
        txs (vec (for [i (range 10)] (:db-after (d/transact conn [{:name (str "c" i) :parent root}]))))
        kid (fn [db i] (:db/id (d/entity db [:name (str "c" i)])))
        newest (fn [db n] (map :db/id (:_parent (d/pull db [{[:_parent :limit n :reverse true] [:db/id]}] root))))
        db-at-6 (txs 5)
        t6 (:max-tx db-at-6)]
    (is (= (map #(kid @conn %) [9 8 7]) (newest @conn 3)) "the newest three")
    (is (= (map #(kid @conn %) [5 4 3]) (newest (d/as-of @conn t6) 3)) "as of the sixth child: the three newest then")
    (d/transact conn [[:db/retract (kid @conn 8) :parent root]])
    (is (= (map #(kid @conn %) [9 7 6]) (newest @conn 3)) "a retraction in the window: it leaves, the next older enters")
    (is (= (map #(kid @conn %) [9 8 7]) (newest (d/as-of @conn (:max-tx (txs 9))) 3)) "as of before the retraction it is still there")
    (let [h (d/history @conn)
          all (map :db/id (:_parent (d/pull h [{:_parent [:db/id]}] root)))]
      (is (= (take 3 (reverse all)) (newest h 3)) "a history db: the last three its pull holds, last first"))
    (d/release conn)
    (d/delete-database cfg)))
