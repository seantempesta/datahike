(ns datahike.test.purge-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing]])
   [datahike.api :as d]
   [datahike.test.utils :as tu]))

#?(:cljs (def Throwable js/Error))

(def schema-tx [{:db/ident       :name
                 :db/valueType   :db.type/string
                 :db/unique      :db.unique/identity
                 :db/index       true
                 :db/cardinality :db.cardinality/one}
                {:db/ident       :age
                 :db/valueType   :db.type/long
                 :db/cardinality :db.cardinality/one}
                {:name "Alice"
                 :age  25}
                {:name "Bob"
                 :age  35}])

(def cfg-template {:store {:backend :memory
                           :id #uuid "001b0000-0000-0000-0000-00000000001b"}
                   :keep-history? true
                   :schema-flexibility :write
                   :initial-tx schema-tx})

(defn find-age [db name]
  (d/q '[:find ?a . :in $ ?n :where [?e :name ?n] [?e :age ?a]] db name))

(defn find-entity [db name]
  (d/q '[:find (pull ?e [:name :age]) :in $ ?n :where [?e :name ?n]] db name))

(defn find-entities [db]
  (into #{}
        (d/q '[:find [(pull ?e [:name :age]) ...] :where [?e :name _]] db)))

(deftest test-purge
  (let [conn (tu/setup-db (assoc-in cfg-template [:store :id] #uuid "09000000-0000-0000-0000-000000000001"))]
    (testing "retract datom, data is removed from current db and found in history"
      (let [name "Alice"]
        (d/transact conn [[:db/retract [:name name] :age 25]])
        (are [x y] (= x y)
          true (nil? (find-age @conn name))
          25 (find-age (d/history @conn) name))))
    (testing "purge datom from current index and from history"
      (let [name "Bob"]
        (d/transact conn [[:db/purge [:name name] :age 35]])
        (are [x y] (= x y)
          true (nil? (find-age @conn name))
          true (nil? (find-age (d/history @conn) name)))))
    (testing "purge retracted datom"
      (let [name "Alice"]
        (d/transact conn [[:db/purge [:name name] :age 25]])
        (are [x y] (= x y)
          nil (find-age @conn name)
          nil (find-age (d/history @conn) name))))
    (d/release conn)))

(deftest test-purge-attribute
  (let [conn (tu/setup-db (assoc-in cfg-template [:store :id] #uuid "09000000-0000-0000-0000-000000000002"))]
    (testing "purge attribute from current index"
      (let [name "Alice"]
        (d/transact conn [[:db.purge/attribute [:name name] :age]])
        (are [x y] (= x y)
          true (nil? (find-age @conn name))
          true (nil? (find-age (d/history @conn) name))
          #{["Alice"] ["Bob"]} (d/q '[:find ?n :where [_ :name ?n]] @conn))))
    (testing "retract attribute from current index and purge from history"
      (let [name "Bob"]
        (testing "retracting from current index"
          (d/transact conn [[:db.fn/retractAttribute [:name name] :age]])
          (are [x y] (= x y)
            true (nil? (find-age @conn name))
            35 (find-age (d/history @conn) name)))
        (testing "purging from history"
          (d/transact conn [[:db.purge/entity [:name name] :age]])
          (are [x y] (= x y)
            true (nil? (find-age @conn name))
            true (nil? (find-age (d/history @conn) name))))))
    (d/release conn)))

(deftest test-purge-entity
  (let [conn (tu/setup-db (assoc-in cfg-template [:store :id] #uuid "09000000-0000-0000-0000-000000000003"))]
    (testing "purge entity from current index"
      (is (= #{{:name "Alice" :age 25} {:name "Bob" :age 35}} (find-entities @conn)))
      (let [report (d/transact conn [[:db.purge/entity [:name "Alice"]]])]
        (is (= #{[:name "Alice" false]
                 [:age 25 false]}
               (into #{}
                     (map (juxt :a :v :added))
                     (remove #(= :db/txInstant (:a %)) (:tx-data report))))))
      (is (= #{{:name "Bob" :age 35}} (find-entities @conn)))
      (is (= #{{:name "Bob" :age 35}} (find-entities (d/history @conn)))))
    (testing "retract entity from current index and purge from history"
      (let [name "Bob"]
        (testing "retracting from current index"
          (d/transact conn [[:db/retractEntity [:name name]]])
          (is (= #{} (find-entities @conn)))
          (is (= #{{:name "Bob" :age 35}} (find-entities (d/history @conn)))))
        (testing "purging from history"
          (d/transact conn [[:db.purge/entity [:name name]]])
          (is (= #{} (find-entities @conn)))
          (is (= #{} (find-entities (d/history @conn)))))))
    (testing "purge something that is not present in the database"
      (is (thrown-with-msg? Throwable
                            #"Can't find entity with ID \[:name \"Alice\"\] to be purged"
                            (d/transact conn [[:db.purge/entity [:name "Alice"]]]))))
    (d/release conn)))

(deftest test-purge-non-temporal-database
  (let [conn (tu/setup-db (-> (assoc-in cfg-template [:store :id] #uuid "09000000-0000-0000-0000-000000000004")
                              (assoc :keep-history? false)))]
    (testing "purge data in non temporal database"
      (is (thrown-with-msg? Throwable #"Purge entity is only available in temporal databases\."
                            (d/transact conn [[:db.purge/entity [:name "Alice"]]]))))
    (d/release conn)))

(defn find-ages [db name]
  (d/q '[:find ?a ?op
         :in $ ?n
         :where
         [?e :name ?n]
         [?e :age ?a ?t ?op]]
       db
       name))

(deftest test-history-purge-before
  (let [conn (tu/setup-db (assoc-in cfg-template [:store :id] #uuid "09000000-0000-0000-0000-000000000005"))
        name "Alice"]
    (testing "remove all historical data before date"
      (is (= #{[25 true]}
             (find-ages @conn name)))
      (let [upsert-date (java.util.Date.)]
        (d/transact conn [{:db/id [:name name] :age 30}])
        (is (= #{[30 true]}
               (find-ages @conn name)))
        (is (= #{[25 true] [25 false] [30 true]}
               (find-ages (d/history @conn) name)))
        (d/transact conn [[:db.history.purge/before upsert-date]])
        (is (= #{[30 true]}
               (find-ages @conn name)))
        (is (= #{[25 false] [30 true]}
               (find-ages (d/history @conn) name)))
        (d/transact conn [[:db.history.purge/before (java.util.Date.)]])
        (is (= #{[30 true]}
               (find-ages (d/history @conn) name)))))
    (d/release conn)))

#?(:clj
   (defn- retype-value!
     "Reinstall :value as :db.type/long after `clear` removed entity \"a\"'s
      string value; return the long write's outcome and :value's history."
     [clear]
     (let [id (random-uuid)
           cfg {:store {:backend :file :id id
                        :path (str (System/getProperty "java.io.tmpdir") "/dh-retype-" id)}
                :keep-history? true
                :schema-flexibility :write
                :index-config {:branching-factor 32 :diff-buf-size 8}}
           attr (fn [t] {:db/ident :value :db/valueType t :db/cardinality :db.cardinality/one})
           conn (do (d/create-database cfg) (d/connect cfg))]
       (try
         (d/transact conn [{:db/ident :id :db/valueType :db.type/string
                            :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}
                           (attr :db.type/string)])
         ;; Enough datoms that the stored temporal roots are Branches, whose
         ;; diff buffer keeps the purge's removal of the string datom.
         (d/transact conn (vec (for [i (range 300)] {:id (str "pad" i)})))
         (d/transact conn [{:id "a" :value "one"}])
         (let [e (:db/id (d/entity @conn [:id "a"]))]
           (d/transact conn (clear e))
           (d/transact conn [[:db/retractEntity :value]])
           (d/transact conn [(attr :db.type/long)])
           (d/transact conn [{:id "a" :value 42}])
           [(:value (d/entity @conn [:id "a"]))
            (d/q '[:find [?v ...] :in $ ?e :where [?e :value ?v]] (d/history @conn) e)])
         (finally (d/release conn) (d/delete-database cfg))))))

#?(:clj
   (deftest test-retype-attribute-keeps-a-total-value-order
     ;; An attribute reinstalled with another :db/valueType shares its index
     ;; prefix (a, e) with old-typed datoms that history keeps (retract) or a
     ;; stored Branch's diff buffer keeps (purge). `compare-value` orders the
     ;; two types by class name instead of throwing ClassCastException.
     (testing "retracted old values stay in history beside the new type"
       (let [[v hist] (retype-value! (fn [e] [[:db/retract e :value "one"]]))]
         (is (= 42 v))
         (is (= #{"one" 42} (set hist)))))
     (testing "purged old values leave a buffered removal beside the new type"
       (is (= [42 [42]] (retype-value! (fn [e] [[:db.purge/attribute e :value]])))))))
