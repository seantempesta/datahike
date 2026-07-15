(ns datahike.test.capability-catalog-test
  (:require
   #?(:cljs [cljs.test :refer-macros [deftest is testing]]
      :clj [clojure.test :refer [deftest is testing]])
   [datahike.api.impl :as api.impl]
   [datahike.api.specification :as specification]
   [datahike.db :as db]))

(defn- plain-data? [value]
  (cond
    (map? value) (and (every? plain-data? (keys value))
                      (every? plain-data? (vals value)))
    (coll? value) (every? plain-data? value)
    :else (or (nil? value) (boolean? value) (number? value)
              (string? value) (keyword? value) (uuid? value))))

(deftest capability-catalog-is-a-bounded-semantic-projection
  (let [catalog (specification/capability-catalog)
        operations (:datahike.capability/operations catalog)
        query (get operations :datahike.operation/query)
        evidence (get operations :datahike.operation/query-with-evidence)]
    (is (= 1 (:datahike.capability/version catalog)))
    (is (plain-data? catalog))
    (is (= :datahike.api/q
           (:datahike.capability.operation/api-name query)))
    (is (:datahike.capability.operation/cacheable? query))
    (is (:datahike.capability.operation/cancellable? query))
    (is (= #{:max-work :max-results :max-result-weight}
           (:datahike.capability.operation/resource-options evidence)))
    (is (every? namespace (mapcat keys (vals operations))))
    (is (every? #(not (contains? % :impl)) (vals operations)))
    (is (every? #(not (contains? % :args)) (vals operations)))
    (is (every? #(not (contains? % :supports-remote?)) (vals operations)))))

(deftest committed-value-identity-is-public-data-with-an-internal-vector-key
  (let [connection-id [::store :db]
        generation (random-uuid)
        commit-id (random-uuid)
        identity {:datahike.value/connection-id connection-id
                  :datahike.value/generation generation
                  :datahike.value/commit-id commit-id}
        committed (assoc (db/empty-db nil {:keep-history? true}) :cache-context
                         {:datahike.cache/connection-id connection-id
                          :datahike.cache/generation generation
                          :datahike.cache/commit-id commit-id
                          :datahike.cache/committed? true})]
    (testing "only an attached committed raw value has identity"
      (is (nil? (db/committed-value-identity (db/empty-db))))
      (is (nil? (db/committed-value-identity (api.impl/history committed))))
      (is (= identity (db/committed-value-identity committed))))
    (testing "the existing cache key remains the compact internal vector"
      (is (= [connection-id generation commit-id]
             (db/committed-cache-identity committed))))))
