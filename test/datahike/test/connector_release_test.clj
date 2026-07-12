(ns datahike.test.connector-release-test
  "Regression tests for draining connection release."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.connections :as connections]
            [datahike.connector :as connector]
            [datahike.readers :as readers]
            [datahike.store :as store]
            [datahike.writer :as writer]
            [datahike.writing :as writing]
            [clojure.core.async :as async]
            [clojure.edn :as edn]
            [konserve.store :as ks])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(defn- connection-entry [conn]
  (let [db @(:wrapped-atom conn)
        config (:config db)
        conn-id (store/connection-id config)]
    (get @connections/*connections* conn-id)))

(defn- reference-count [conn]
  (:count (connection-entry conn)))

(defn- config-connection-id [cfg]
  (store/connection-id (assoc cfg :branch (or (:branch cfg) :db))))

(defn- registered-connection [cfg]
  (get-in @connections/*connections* [(config-connection-id cfg) :conn]))

(deftest branch-opening-reservations-share-one-physical-hook-registry
  (let [physical-key {:backend :file :path "/same/store" :id (random-uuid)}
        completion-a (async/promise-chan)
        completion-b (async/promise-chan)
        id-a [(random-uuid) :db]
        id-b [(first id-a) :branch]
        a (connections/reserve-connection-opening! id-a completion-a
                                                   {:branch :db} physical-key)
        b (connections/reserve-connection-opening! id-b completion-b
                                                   {:branch :branch} physical-key)]
    (try
      (is (= :owner (:state a)))
      (is (= :owner (:state b)))
      (is (identical? (:write-hooks a) (:write-hooks b))
          "separate branch reservations converge atomically on one hook atom")
      (finally
        (connections/fail-connection-opening! id-a completion-a)
        (connections/fail-connection-opening! id-b completion-b)))))

(deftest release-awaits-an-accepted-write
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? true
             :schema-flexibility :read}
        _ (d/create-database cfg)
        conn (d/connect cfg)
        commit-entered (CountDownLatch. 1)
        continue-commit (CountDownLatch. 1)
        original-commit writing/commit!]
    (try
      (with-redefs [writing/commit!
                    (fn [& args]
                      (.countDown commit-entered)
                      (.await continue-commit 10 TimeUnit/SECONDS)
                      (apply original-commit args))]
        (let [write-result (future (d/transact conn [{:db/id 1 :value :accepted}]))]
          (is (.await commit-entered 10 TimeUnit/SECONDS)
              "the write must be accepted before release starts")
          (let [release-result (future (d/release conn))]
            (loop [attempts 100]
              (when (and (pos? attempts)
                         (not= 0 (reference-count conn)))
                (Thread/sleep 10)
                (recur (dec attempts))))
            (let [concurrent-release (future (d/release conn))]
              (testing "release remains blocked while an accepted write is committing"
                (is (= ::waiting (deref release-result 100 ::waiting)))
                (is (= ::waiting (deref concurrent-release 100 ::waiting))
                    "a concurrent release awaits the same drain"))
              (testing "connect cannot resurrect a zero-reference closing connection"
                (is (= :connection-is-being-released
                       (try
                         (d/connect cfg)
                         (catch Exception e (:type (ex-data e)))))))
            (.countDown continue-commit)
            (is (map? (deref write-result 10000 ::write-timeout)))
            (is (nil? (deref release-result 10000 ::release-timeout)))
              (is (nil? (deref concurrent-release 10000 ::release-timeout)))
            (testing "release returns only after the durable branch head includes the write"
              (let [reconnected (d/connect cfg)]
                (try
                  (is (= #{:accepted}
                         (set (d/q '[:find [?v ...] :where [_ :value ?v]]
                                   @reconnected))))
                  (finally
                      (d/release reconnected)))))))))
      (finally
        (.countDown continue-commit)
        (try (d/release conn) (catch Exception _))
        (d/delete-database cfg)))))

(deftest concurrent-releasers-observe-the-same-failure
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :read}
        _ (d/create-database cfg)
        conn (d/connect cfg)
        original-shutdown writer/shutdown
        gate (async/promise-chan)
        failure (ex-info "synthetic shutdown failure" {:type :synthetic-shutdown})]
    (try
      (with-redefs [writer/shutdown
                    (fn [w]
                      (let [drained (original-shutdown w)
                            result (async/promise-chan)]
                        (async/go
                          (async/<! drained)
                          (async/<! gate)
                          (async/>! result failure))
                        result))]
        (let [release-one (future
                            (try (d/release conn) :unexpected-success
                                 (catch Exception e (:type (ex-data e)))))]
          (loop [attempts 100]
            (when (and (pos? attempts)
                       (not= 0 (reference-count conn)))
              (Thread/sleep 10)
              (recur (dec attempts))))
          (let [release-two (future
                              (try (d/release conn) :unexpected-success
                                   (catch Exception e (:type (ex-data e)))))]
            (async/>!! gate true)
            (is (= :connection-release-failed
                   (deref release-one 10000 ::timeout)))
            (is (= :connection-release-failed
                   (deref release-two 10000 ::timeout))
                "the duplicate releaser receives the owner's failure")
            (is (nil? (get @connections/*connections* (config-connection-id cfg))))
            (let [reconnected (d/connect cfg)]
              (with-redefs [writer/shutdown original-shutdown]
                (d/release reconnected))))))
      (finally
        (async/put! gate true)
        (try (d/release conn) (catch Exception _))
        (d/delete-database cfg)))))

(deftest release-all-closes-every-shared-reference
  (let [cfg {:store {:backend :memory :id (random-uuid)}}
        _ (d/create-database cfg)
        conn-a (d/connect cfg)
        conn-b (d/connect cfg)]
    (try
      (is (identical? conn-a conn-b))
      (is (= 2 (reference-count conn-a)))
      (d/release conn-a)
      (is (= 1 (reference-count conn-b)))
      (is (map? @conn-b) "ordinary release retains the shared connection")

      ;; Reacquire the second reference, then force all references closed.
      (d/connect cfg)
      (is (= 2 (reference-count conn-b)))
      (connector/release conn-b true)
      (is (nil? (get @connections/*connections* (config-connection-id cfg))))
      (is (= :connection-has-been-released
             (try @conn-a
                  (catch Exception e (:type (ex-data e))))))
      (finally
        (try (d/release conn-a) (catch Exception _))
        (d/delete-database cfg)))))

(deftest concurrent-first-connects-share-one-opening
  (let [cfg {:store {:backend :memory :id (random-uuid)}}
        _ (d/create-database cfg)
        connect-entered (CountDownLatch. 1)
        continue-connect (CountDownLatch. 1)
        connect-count (atom 0)
        original-connect-store ks/connect-store]
    (try
      (with-redefs [ks/connect-store
                    (fn [store-config opts]
                      (swap! connect-count inc)
                      (.countDown connect-entered)
                      (.await continue-connect 10 TimeUnit/SECONDS)
                      (original-connect-store store-config opts))]
        (let [first-connect (future (d/connect cfg))]
          (is (.await connect-entered 10 TimeUnit/SECONDS))
          (let [second-connect (future (d/connect cfg))]
            (is (= ::waiting (deref second-connect 100 ::waiting))
                "the second caller waits on the opening reservation")
            (.countDown continue-connect)
            (let [conn-a (deref first-connect 10000 ::timeout)
                  conn-b (deref second-connect 10000 ::timeout)]
              (is (identical? conn-a conn-b))
              (is (= 1 @connect-count) "only the reservation owner opens the store")
              (is (= 2 (reference-count conn-a)))
              (d/release conn-a)
              (is (= 1 (reference-count conn-b))
                  "the waiter's pre-reserved reference survives an immediate owner release")
              (is (map? @conn-b))
              (d/release conn-b)))))
      (finally
        (.countDown continue-connect)
        (when-let [conn (registered-connection cfg)]
          (try (connector/release conn true) (catch Exception _)))
        (d/delete-database cfg)))))

(deftest mismatched-opening-does-not-reserve-a-reference
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? true}
        _ (d/create-database cfg)
        connect-entered (CountDownLatch. 1)
        continue-connect (CountDownLatch. 1)
        original-connect-store ks/connect-store]
    (try
      (with-redefs [ks/connect-store
                    (fn [store-config opts]
                      (.countDown connect-entered)
                      (.await continue-connect 10 TimeUnit/SECONDS)
                      (original-connect-store store-config opts))]
        (let [owner (future (d/connect cfg))]
          (is (.await connect-entered 10 TimeUnit/SECONDS))
          (is (= :config-does-not-match-existing-connections
                 (try
                   (d/connect (assoc cfg :keep-history? false))
                   (catch Exception e (:type (ex-data e)))))
              "a mismatched waiter is rejected before it acquires a ref")
          (.countDown continue-connect)
          (let [conn (deref owner 10000 ::timeout)]
            (is (= 1 (reference-count conn)))
            (d/release conn))))
      (finally
        (.countDown continue-connect)
        (when-let [conn (registered-connection cfg)]
          (try (connector/release conn true) (catch Exception _)))
        (d/delete-database cfg)))))

(deftest config-mismatch-returns-its-acquired-reference
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? true}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      (is (= :config-does-not-match-existing-connections
             (try
               (d/connect (assoc cfg :keep-history? false))
               (catch Exception e (:type (ex-data e))))))
      (is (= 1 (reference-count conn))
          "a rejected shared acquisition does not leak its count")
      (finally
        (d/release conn)
        (d/delete-database cfg)))))

(deftest failed-opening-closes-restored-secondary-resources
  (let [cfg {:store {:backend :memory :id (random-uuid)}}
        _ (d/create-database cfg)
        closed (atom 0)
        original-stored->db writing/stored->db
        secondary (reify java.io.Closeable
                    (close [_] (swap! closed inc)))]
    (try
      (with-redefs [writing/stored->db
                    (fn [& args]
                      (assoc (apply original-stored->db args)
                             :secondary-indices {:idx/test secondary}))
                    writer/create-writer
                    (fn [& _]
                      (throw (ex-info "synthetic writer construction failure"
                                      {:type :synthetic-writer-construction})))]
        (is (= :synthetic-writer-construction
               (try
                 (d/connect cfg)
                 (catch Exception e (:type (ex-data e)))))))
      (is (= 1 @closed))
      (is (nil? (get @connections/*connections* (config-connection-id cfg)))
          "a failed owner removes its opening reservation")
      (let [conn (d/connect cfg)]
        (d/release conn))
      (finally
        (when-let [conn (registered-connection cfg)]
          (try (connector/release conn true) (catch Exception _)))
        (d/delete-database cfg)))))

(deftest release-awaits-accepted-asynchronous-writer-ops
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :writer {:backend :self
                      :write-fn-map
                      {'blocked-op (fn [_db started result]
                                     (async/put! started true)
                                     result)}}}
        _ (d/create-database cfg)
        conn (d/connect cfg)
        started (async/promise-chan)
        result (async/promise-chan)
        callback (writer/dispatch! (:writer @conn)
                                   {:op 'blocked-op :args [started result]})]
    (try
      (is (true? (async/<!! started)))
      (let [released (future (d/release conn))]
        (is (= ::waiting (deref released 100 ::waiting))
            "release joins accepted channel-returning writer ops")
        (async/>!! result :finished)
        (is (= :finished (async/<!! callback)))
        (is (nil? (deref released 10000 ::timeout))))
      (finally
        (async/put! result :finished)
        (try (d/release conn) (catch Exception _))
        (d/delete-database cfg)))))

(deftest dispatch-after-writer-shutdown-resolves-with-an-error
  (let [cfg {:store {:backend :memory :id (random-uuid)}}
        _ (d/create-database cfg)
        conn (d/connect cfg)
        stale-writer (:writer @conn)]
    (try
      (d/release conn)
      (let [result-ch (writer/dispatch! stale-writer
                                        {:op 'transact! :args [{:tx-data []}]})
            timeout-ch (async/timeout 2000)
            [value port] (async/alts!! [result-ch timeout-ch])
            result (if (= port timeout-ch) ::timeout value)]
        (is (instance? Throwable result))
        (is (= :writer-shut-down (:type (ex-data result)))))
      (finally
        (d/delete-database cfg)))))

(deftest db-reader-borrows-the-active-store-without-owning-it
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :read}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      (d/transact conn [{:db/id 1 :value :round-trip}])
      (let [before (reference-count conn)
            restored (edn/read-string {:readers readers/edn-readers}
                                      (pr-str @conn))]
        (is (= #{:round-trip}
               (set (d/q '[:find [?v ...] :where [_ :value ?v]] restored))))
        (is (= before (reference-count conn))
            "deserializing an immutable DB does not leak a connection ref"))
      (finally
        (d/release conn)
        (d/delete-database cfg)))))
