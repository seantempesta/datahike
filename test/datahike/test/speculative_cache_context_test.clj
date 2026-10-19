(ns datahike.test.speculative-cache-context-test
  "Attribute revisions of transaction-function commits and of the uncommitted
   values a transaction function or `with` observes."
  (:require
   [clojure.test :refer [is deftest]]
   [datahike.api :as d]
   [datahike.core :as dc]
   [datahike.db :as db]))

(def ^:private schema
  [{:db/ident :s/id
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident :s/a
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :s/b
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}])

(defn- with-conn [f]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? false}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      (d/transact conn schema)
      (d/transact conn [{:s/id "x" :s/a 1 :s/b 1}])
      (f conn)
      (finally
        (d/release conn)
        (d/delete-database cfg)))))

(defn- revision [database attribute]
  (get-in (:cache-context database) [:datahike.cache/attribute-revisions attribute]))

(deftest fn-call-commit-advances-only-the-attributes-it-returned
  (with-conn
    (fn [conn]
      (let [before @conn
            _ (d/transact conn [[:db.fn/call (fn [_] [{:s/id "x" :s/a 2}])]])
            after @conn]
        (is (not= (revision before :s/a) (revision after :s/a)))
        (is (= (revision before :s/b) (revision after :s/b)))
        (is (= (:datahike.cache/conservative-revision (:cache-context before))
               (:datahike.cache/conservative-revision (:cache-context after)))
            "a known expansion never advances the conservative revision")))))

(deftest in-transaction-value-inherits-untouched-revisions
  (with-conn
    (fn [conn]
      (let [seen (atom [])
            observe (fn [database]
                      (swap! seen conj (:cache-context database))
                      [])]
        ;; Two queued writes to :s/a; the second is built on the first
        ;; report's uncommitted value, as the writer threads it.
        (d/transact conn [{:s/id "x" :s/a 3} [:db.fn/call observe]])
        (d/transact conn [{:s/id "x" :s/a 4} [:db.fn/call observe]])
        (let [[first-context second-context] @seen]
          (doseq [context @seen]
            (is (false? (:datahike.cache/committed? context)))
            (is (nil? (:datahike.cache/commit-id context)))
            (is (= (select-keys (:cache-context @conn)
                                [:datahike.cache/connection-id
                                 :datahike.cache/generation])
                   (select-keys context [:datahike.cache/connection-id
                                         :datahike.cache/generation]))))
          (is (some? (get-in first-context [:datahike.cache/attribute-revisions :s/b])))
          (is (= (get-in first-context [:datahike.cache/attribute-revisions :s/b])
                 (get-in second-context [:datahike.cache/attribute-revisions :s/b]))
              "an unrelated write keeps an untouched attribute's revision")
          (is (not= (get-in first-context [:datahike.cache/attribute-revisions :s/a])
                    (get-in second-context [:datahike.cache/attribute-revisions :s/a]))
              "an attribute an earlier operation wrote gets a fresh revision"))))))

(deftest speculative-values-have-no-committed-identity
  (with-conn
    (fn [conn]
      (let [basis @conn
            speculative (:db-after (d/with basis [{:s/id "x" :s/a 4}]))]
        (is (nil? (db/committed-value-identity speculative))
            "the query cache and committed readers keep ignoring it")
        (is (= (revision basis :s/b) (revision speculative :s/b)))
        (is (not= (revision basis :s/a) (revision speculative :s/a)))
        (is (= #{[4]} (d/q '[:find ?v :where [_ :s/a ?v]] speculative)))))))

(deftest schema-change-in-flight-is-conservative
  (with-conn
    (fn [conn]
      (let [basis @conn
            seen (atom nil)]
        (d/transact conn [{:db/ident :s/c
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}
                          [:db.fn/call (fn [database]
                                         (reset! seen (:cache-context database))
                                         [])]])
        (is (some? (:datahike.cache/conservative-revision @seen)))
        (is (not= (:datahike.cache/conservative-revision (:cache-context basis))
                  (:datahike.cache/conservative-revision @seen)))))))

(deftest basis-without-connection-identity-stays-detached
  (let [seen (atom ::unset)
        basis (dc/empty-db)]
    (d/with basis [[:db.fn/call (fn [database]
                                  (reset! seen (:cache-context database))
                                  [])]])
    (is (nil? @seen)
        "no inherited revisions means no claim of currency")
    (is (nil? (:cache-context (:db-after (d/with basis [[:db/add -1 :x 1]])))))))
