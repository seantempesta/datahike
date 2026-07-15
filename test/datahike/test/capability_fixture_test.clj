(ns datahike.test.capability-fixture-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.committed-report :as committed-report]
            [datahike.connector :as connector]
            [datahike.datom :as datom]
            [datahike.db.utils :as dbu]))

(defn- forbidden-host-value? [value]
  (or (dbu/db? value)
      (connector/connection? value)
      (datom/datom? value)
      (fn? value)
      (instance? clojure.lang.IDeref value)
      (instance? java.lang.Thread value)
      (instance? java.util.concurrent.Future value)
      (instance? Throwable value)))

(defn- ordinary-namespaced-data? [value]
  (cond
    (forbidden-host-value? value) false
    (map? value) (and (every? #(and (keyword? %) (namespace %)) (keys value))
                      (every? ordinary-namespaced-data? (vals value)))
    (coll? value) (every? ordinary-namespaced-data? value)
    :else (or (nil? value) (boolean? value) (number? value)
              (string? value) (keyword? value) (uuid? value)
              (inst? value))))

(deftest transport-free-capability-fixture-returns-only-ordinary-data
  (let [configuration {:store {:backend :memory :id (random-uuid)}
                       :schema-flexibility :read}]
    (d/create-database configuration)
    (let [connection (d/connect configuration)
          {:datahike.cache/keys [connection-id generation]}
          (:cache-context @(:wrapped-atom connection))
          source (committed-report/open! connection-id generation 4)]
      (try
        (d/transact connection
                    [{:db/id 1 :fixture/value 42}])
        (let [database @connection
              report (committed-report/poll! source)
              identity (d/committed-value-identity database)
              query (d/q-with-evidence
                     '[:find ?value . :where [_ :fixture/value ?value]]
                     database)
              pull (d/pull database [:db/id :fixture/value] 1)
              release (d/close-query-cache-generation!
                       connection-id generation)
              result
              {:datahike.fixture/capabilities (d/capabilities)
               :datahike.fixture/value-identity identity
               :datahike.fixture/query query
               :datahike.fixture/pull pull
               :datahike.fixture/committed-event
               {:datahike.committed-report/commit-id
                (get-in report [:tx-meta :db/commitId])
                :datahike.committed-report/tx-data
                (mapv (fn [item]
                        {:datahike.datom/e (:e item)
                         :datahike.datom/a (:a item)
                         :datahike.datom/v (:v item)
                         :datahike.datom/tx (:tx item)
                         :datahike.datom/added? (:added item)})
                      (:tx-data report))}
               :datahike.fixture/cancel
               (d/cancel-query! (str (random-uuid)))
               :datahike.fixture/cache-evidence (d/query-cache-evidence)
               :datahike.fixture/release release}]
          (is (= 42 (:datahike.query/result query)))
          (is (= 42 (:fixture/value pull)))
          (is (= (:datahike.value/commit-id identity)
                 (get-in result
                         [:datahike.fixture/committed-event
                          :datahike.committed-report/commit-id])))
          (is (:datahike.cache.release/current-generation? release))
          (is (= 1
                 (:datahike.cache.release/completed-snapshots-evicted
                  release)))
          (is (ordinary-namespaced-data? result)))
        (finally
          (committed-report/close! source false)
          (d/release connection)
          (d/delete-database configuration))))))
