(ns datahike.test.writer-error-test
  "Fatal Errors (AssertionError, OOM, ...) thrown inside the async commit
   pipeline must fail transacts LOUDLY, not hang them: go-try- catches
   Exception only, so an escaping Error used to kill the dispatch thread and
   leave the writer's commit loop parked forever on a silent channel — every
   queued transact hung. commit! now converts Errors to ex-info at the go
   boundary, so callbacks receive the error and the writer shuts down."
  (:require [datahike.api :as d]
            [datahike.core :as core]
            [datahike.writer :as writer]
            [datahike.writing :as dw]
            [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [taoensso.trove :as trove]
            [taoensso.trove.console :as trove-console]))

(defn- take-with-timeout [channel]
  (let [timeout (async/timeout 10000)
        [value port] (async/alts!! [channel timeout])]
    (if (= port timeout) ::timeout value)))

(defn- line-count [text]
  (if (empty? text)
    0
    (inc (count (filter #{\newline} text)))))

(defn- oversized-refusal [_]
  (throw
   (ex-info
    (str (apply str (repeat 2048 "x"))
         "\n"
         (apply str (repeat 2048 "y")))
    {:error :transact/schema
     :attribute :test/value})))

(defn- listener-operation!
  [operation conn value]
  (case operation
    :transact (d/transact! conn [{:db/id -1 :listener/value value}])
    :merge (d/merge-db! conn #{:listener-parent}
                        [{:db/id -1 :listener/value value}])))

(defn- prepare-listener-operation!
  [operation conn]
  (when (= :merge operation)
    (d/branch! conn :db :listener-parent)))

(deftest listener-failure-cannot-suppress-committed-results-or-other-listeners
  (doseq [operation [:transact :merge]]
    (testing (name operation)
      (let [cfg {:store {:backend :memory :id (random-uuid)}
                 :schema-flexibility :read}
            _ (d/create-database cfg)
            conn (d/connect cfg)
            survivor-count (atom 0)
            diagnostic (promise)
            listener-error (ex-info "synthetic listener failure"
                                    {:type :test/listener-failure})
            log-fn (fn [_logger-ns _coordinates level id lazy-data]
                     (let [data (force (force lazy-data))
                           payload (or (:msg data) (:data data))]
                       (when (= :datahike/listener-error id)
                         (deliver diagnostic {:level level
                                              :payload payload}))))]
        (try
          (prepare-listener-operation! operation conn)
          (core/listen! conn ::throwing (fn [_] (throw listener-error)))
          (core/listen! conn ::survivor (fn [_] (swap! survivor-count inc)))
          (binding [trove/*log-fn* log-fn]
            (let [first-report (deref (listener-operation! operation conn 1)
                                      10000 ::timeout)
                  logged (deref diagnostic 10000 ::timeout)
                  next-report (deref (d/transact! conn
                                                  [{:db/id -1
                                                    :listener/value 2}])
                                     10000 ::timeout)]
              (is (map? first-report)
                  "the committed operation result is realized")
              (is (= :error (:level logged)))
              (is (= ::throwing (get-in logged [:payload :listener-key])))
              (is (identical? listener-error
                              (get-in logged [:payload :exception])))
              (is (map? next-report)
                  "a later transaction succeeds with the faulty listener installed")
              (is (= 2 @survivor-count)
                  "other listeners receive both committed reports")))
          (finally
            (core/unlisten! conn ::throwing)
            (core/unlisten! conn ::survivor)
            (d/release conn)
            (d/delete-database cfg)))))))

(deftest committed-results-are-realized-before-blocked-listeners-return
  (doseq [operation [:transact :merge]]
    (testing (name operation)
      (let [cfg {:store {:backend :memory :id (random-uuid)}
                 :schema-flexibility :read}
            _ (d/create-database cfg)
            conn (d/connect cfg)
            entered (java.util.concurrent.CountDownLatch. 1)
            release (java.util.concurrent.CountDownLatch. 1)
            finished (java.util.concurrent.CountDownLatch. 1)]
        (try
          (prepare-listener-operation! operation conn)
          (core/listen!
           conn ::blocked
           (fn [_]
             (.countDown entered)
             (try
               (.await release 10 java.util.concurrent.TimeUnit/SECONDS)
               (finally
                 (.countDown finished)))))
          (let [pending (listener-operation! operation conn 1)]
            (is (.await entered 10 java.util.concurrent.TimeUnit/SECONDS)
                "the listener entered its bounded block")
            (is (map? (deref pending 10000 ::timeout))
                "the committed result is available before listener release"))
          (finally
            (.countDown release)
            (is (.await finished 10 java.util.concurrent.TimeUnit/SECONDS)
                "the listener exits after release")
            (core/unlisten! conn ::blocked)
            (d/release conn)
            (d/delete-database cfg)))))))

(deftest expected-refusal-log-is-bounded-and-unexpected-error-stays-complete
  (let [events (atom [])
        console-log (trove-console/get-log-fn {:min-level :error})
        log-fn (fn [namespace coordinates level id lazy-data]
                 (let [data (force (force lazy-data))]
                   (swap! events conj
                          {:namespace namespace
                           :coordinates coordinates
                           :level level
                           :id id
                           :payload (or (:msg data) (:data data))})
                   (when (= :datahike/write-rejected id)
                     (console-log namespace coordinates level id data))))
        cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :read
             :writer
             {:backend :self
              :write-fn-map
              {'unexpected-op
               (fn [_]
                 (throw (ex-info "unexpected writer failure"
                                 {:type :test/unexpected-writer-failure})))}}}]
    (binding [trove/*log-fn* log-fn]
      (let [stderr (java.io.StringWriter.)
            _ (binding [*err* stderr]
                (d/create-database cfg)
                (let [conn (d/connect cfg)]
                  (try
                    (is (thrown? Throwable
                                 (d/transact conn
                                             [[:db.fn/call oversized-refusal]])))
                    (let [error (take-with-timeout
                                 (writer/dispatch! (:writer @conn)
                                                   {:op 'unexpected-op
                                                    :args []}))]
                      (is (instance? Throwable error)))
                    (finally
                      (try (d/release conn) (catch Throwable _)))))
                (when (d/database-exists? cfg)
                  (d/delete-database cfg)))
            output (str stderr)
            refusal-events (filterv #(= :datahike/write-rejected (:id %)) @events)
            unexpected-events (filterv #(= :datahike/write-error (:id %)) @events)
            refusal-payload (:payload (first refusal-events))
            unexpected-payload (:payload (first unexpected-events))]
        (is (= 1 (count refusal-events)))
        (is (= :transact/schema (:kind refusal-payload))
            (pr-str (first refusal-events)))
        (is (= :test/value (:attribute refusal-payload))
            (pr-str (first refusal-events)))
        (is (<= (count (:cause refusal-payload)) 256))
        (is (<= (line-count output) 3)
            (str "expected refusal emitted " (line-count output) " stderr lines"))
        (is (<= (count output) 512)
            (str "expected refusal emitted " (count output) " stderr characters"))
        (is (= 1 (count unexpected-events)))
        (is (instance? Throwable (:error unexpected-payload)))
        (is (map? (:invocation unexpected-payload)))
        (is (= [] (:args unexpected-payload)))))))

(deftest final-report-validation-is-atomic-and-preserves-composition
  (doseq [history? [true false]]
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :write :keep-history? history?}
          _ (d/create-database cfg)
          conn (d/connect cfg)
          calls (atom 0)
          observations (atom [])
          notifications (atom 0)
          refusal {:test/entity "incomplete" :test/key :test/y}
          validate (fn [report]
                     (swap! observations conj report)
                     (when (some (fn [datom]
                                   (and (= :test/id (:a datom))
                                        (= "incomplete" (:v datom))))
                                 (:datahike/attempted-tx-data report))
                       refusal))]
      (try
        (d/transact conn [{:db/ident :test/id :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                          {:db/ident :test/x :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :test/y :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :test/xy :db/tupleAttrs [:test/x :test/y]
                           :db/valueType :db.type/tuple :db/cardinality :db.cardinality/one}])
        (core/listen! conn ::validation (fn [_] (swap! notifications inc)))
        (let [basis (:max-tx @conn)
              rejected (try
                         (d/transact conn
                           {:tx-data [{:test/id "valid" :test/x 1 :test/y 2}
                                      [:db/add -1 :test/id "incomplete"]]
                            :tx-meta {:datahike/validate-report validate}})
                         nil
                         (catch Exception failure
                           (some #(when (:error (ex-data %)) (ex-data %))
                                 (take-while some? (iterate ex-cause failure)))))]
          (is (= :transaction/validation-rejected (:error rejected)))
          (is (= refusal (:datahike/validation-refusal rejected)))
          (is (= basis (:max-tx @conn)))
          (is (empty? (d/q '[:find ?e :where [?e :test/id]] @conn)))
          (is (zero? @notifications)))
        (reset! observations [])
        (let [result (d/transact conn
                       {:tx-data [{:test/id "composed" :test/x 3}
                                  [:db.fn/call
                                   (fn [_]
                                     (swap! calls inc)
                                     [[:db.fn/call
                                       (fn [_]
                                         [[:db/add [:test/id "composed"] :test/y 4]])]])]]
                        :tx-meta {:datahike/validate-report validate}})]
          (is (= 1 @calls) "validation does not replay transaction functions")
          (is (= 1 (count @observations)))
          (is (= [3 4] (:test/xy (d/pull (:db-after (first @observations))
                                       '[*] [:test/id "composed"]))))
          (is (not (contains? (:tx-meta result) :datahike/validate-report)))
          (is (not (contains? result :datahike/attempted-tx-data))))
        (reset! observations [])
        (d/transact conn {:tx-data [[:db/add [:test/id "composed"] :test/x 3]]
                          :tx-meta {:datahike/validate-report validate}})
        (is (some #(= :test/x (:a %))
                  (:datahike/attempted-tx-data (first @observations)))
            "idempotent assertions still reach validation")
        (reset! observations [])
        (d/transact conn {:tx-data [[:db/add -1 :test/x 5]
                                   [:db/add -1 :test/id "composed"]]
                          :tx-meta {:datahike/validate-report validate}})
        (is (= 1 (count @observations))
            "native tempid retries preserve the final callback and invoke it once")
        (is (= 5 (:test/x (d/pull @conn '[*] [:test/id "composed"]))))
        (reset! observations [])
        (d/transact conn {:tx-data [[:db/retract [:test/id "composed"] :test/y 4]]
                          :tx-meta {:datahike/validate-report validate}})
        (is (nil? (:test/y (d/pull (:db-after (first @observations))
                                 '[*] [:test/id "composed"]))))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(deftest queued-expected-basis-observes-the-threaded-uncommitted-head
  (let [processed (atom 0)
        both-processed (promise)
        cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :read
             :writer
             {:backend :self
              :write-fn-map
              {'transact!
               (fn [db argument]
                 (try
                   (dw/transact! db argument)
                   (finally
                     (when (= 2 (swap! processed inc))
                       (deliver both-processed true)))))}}}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      (d/transact conn [{:db/id 1 :n 0}])
      (reset! processed 0)
      (let [expected (:max-tx @conn)
            original-create-commit-id dw/create-commit-id
            commit-entered (promise)
            release-commit (promise)
            results
            (with-redefs
              [dw/create-commit-id
               (fn [& arguments]
                 (deliver commit-entered true)
                 @release-commit
                 (apply original-create-commit-id arguments))]
              (let [left
                    (d/transact! conn
                                 {:tx-data [{:db/id 2 :n 1}]
                                  :datahike/expected-basis-t expected})
                    _ (is (true? (deref commit-entered 10000 false))
                          "the first report waits before durable publication")
                    right
                    (d/transact! conn
                                 {:tx-data [{:db/id 3 :n 2}]
                                  :datahike/expected-basis-t expected})
                    _ (is (true? (deref both-processed 10000 false))
                          "both uncompleted calls reach the LocalWriter apply seam")]
                (deliver release-commit true)
                (mapv take-with-timeout [left right])))
            reports (filterv map? results)
            errors (filterv #(instance? Throwable %) results)]
        (is (= 1 (count reports))
            "exactly one same-basis transaction commits")
        (is (= 1 (count errors))
            "the second same-basis transaction is rejected")
        (is (= :transaction/stale-basis
               (:error (ex-data (first errors)))))
        (is (= #{0 1}
               (set (d/q '[:find [?n ...] :where [_ :n ?n]] @conn)))
            "the baseline and winning fact land; the rejected fact does not"))
      (finally
        (try (d/release conn) (catch Throwable _))
        (when (d/database-exists? cfg)
          (d/delete-database cfg))))))

(deftest fatal-error-in-commit-fails-loudly
  (testing "a fatal Error during commit propagates to the caller within a bounded time"
    (let [cfg {:store {:backend :file
                       :path (str (System/getProperty "java.io.tmpdir") "/dh-fatal-commit")
                       :id #uuid "d1ffb000-0000-0000-0000-00000000fa7a"}
               :schema-flexibility :read :keep-history? false}]
      (when (d/database-exists? cfg) (d/delete-database cfg))
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn [{:db/id 1 :n 1}])
        (let [orig dw/db->stored
              result (with-redefs [dw/db->stored (fn [& _] (throw (AssertionError. "synthetic fatal error")))]
                       (let [f (future (try (d/transact conn [{:db/id 2 :n 2}]) :no-error
                                            (catch Exception _ :failed-loudly)
                                            (catch AssertionError _ :failed-loudly)))]
                         (deref f 15000 :HUNG)))]
          (is (= :failed-loudly result)
              "transact must complete exceptionally, not hang")
          ;; writer shut down; the connection refuses further writes explicitly
          (is (thrown? Exception (d/transact conn [{:db/id 3 :n 3}]))
              "subsequent transacts on the dead writer fail loudly")
          ;; durable state is the last good commit; a fresh connection works
          (try (d/release conn) (catch Exception _))
          (let [conn2 (d/connect cfg)]
            (is (= 1 (d/q '[:find (count ?e) . :where [?e :n _]] @conn2))
                "store intact at the last successful commit")
            (d/transact conn2 [{:db/id 2 :n 2}])
            (is (= 2 (d/q '[:find (count ?e) . :where [?e :n _]] @conn2))
                "fresh connection transacts normally")
            (d/release conn2))
          (is (fn? orig))))
      (d/delete-database cfg))))

(deftest fatal-error-in-commit-loop-fails-loudly
  (testing "an Error thrown on the COMMIT thread (create-commit-id — only commit! calls it)
            reaches the caller instead of parking the writer forever"
    (let [cfg {:store {:backend :file
                       :path (str (System/getProperty "java.io.tmpdir") "/dh-fatal-commit2")
                       :id #uuid "d1ffb000-0000-0000-0000-00000000fa7b"}
               :schema-flexibility :read :keep-history? false}]
      (when (d/database-exists? cfg) (d/delete-database cfg))
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn [{:db/id 1 :n 1}])
        (let [result (with-redefs [dw/create-commit-id (fn [& _] (throw (AssertionError. "synthetic commit-thread error")))]
                       (let [f (future (try (d/transact conn [{:db/id 2 :n 2}]) :no-error
                                            (catch Exception _ :failed-loudly)
                                            (catch AssertionError _ :failed-loudly)))]
                         (deref f 15000 :HUNG)))]
          (is (= :failed-loudly result) "commit-thread Error must not hang the transact"))
        (try (d/release conn) (catch Exception _)))
      (d/delete-database cfg))))

(deftest commit-failure-resolves-every-accepted-write
  (testing "a failed commit terminates the writer without losing queued callbacks"
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :read
               :writer {:backend :self}}
          _ (d/create-database cfg)
          processed (atom 0)
          all-processed (promise)
          commit-entered (promise)
          release-failure (promise)
          total 8
          conn (d/connect
                (assoc-in cfg [:writer :write-fn-map]
                          {'tracked-transact
                           (fn [db arg]
                             (let [result (dw/transact! db arg)]
                               (when (= total (swap! processed inc))
                                 (deliver all-processed true))
                               result))}))
          local-writer (:writer @conn)]
      (try
        ;; Establish a durable baseline that the synthetic failure must not
        ;; move. The redefinition starts only after this commit completes.
        (d/transact conn [{:db/id 1 :n 0}])
        (let [callbacks
              (with-redefs [dw/create-commit-id
                            (fn [& _]
                              (deliver commit-entered true)
                              @release-failure
                              (throw (ex-info "synthetic commit failure"
                                              {:type :synthetic-commit-failure})))]
                (let [first-callback
                      (writer/dispatch! local-writer
                                        {:op 'tracked-transact
                                         :args [{:tx-data [{:db/id 2 :n 1}]}]})]
                  (is (true? (deref commit-entered 10000 false))
                      "the first commit is blocked before more writes are queued")
                  (let [remaining
                        (mapv (fn [n]
                                (writer/dispatch! local-writer
                                                  {:op 'tracked-transact
                                                   :args [{:tx-data [{:db/id (+ n 2)
                                                                      :n (inc n)}]}]}))
                              (range (dec total)))]
                    (is (true? (deref all-processed 10000 false))
                        "all accepted writes reach the commit stage behind the blocked failure")
                    (deliver release-failure true)
                    (into [first-callback] remaining))))
              results (mapv take-with-timeout callbacks)]
          (is (every? #(and (instance? Throwable %)
                            (not= ::timeout %))
                      results)
              "every accepted callback resolves with a failure")
          (let [released (future
                           (try
                             (d/release conn)
                             :released
                             (catch Throwable _ :failed-loudly)))]
            (is (not= ::timeout (deref released 10000 ::timeout))
                "writer shutdown joins both failed loops"))
          (let [fresh (d/connect cfg)]
            (try
              (is (= #{0}
                     (set (d/q '[:find [?n ...] :where [_ :n ?n]] @fresh)))
                  "no staged transaction commits after the first failure")
              (finally
                (d/release fresh)))))
        (finally
          (deliver release-failure true)
          (try (d/release conn) (catch Throwable _))
          (when (d/database-exists? cfg)
            (d/delete-database cfg)))))))

(deftest fatal-processing-error-resolves-buffered-writes
  (testing "a fatal writer operation resolves invocations accepted behind it"
    (let [started (promise)
          release-error (promise)
          cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :read
               :writer {:backend :self
                        :write-fn-map
                        {'fatal-op
                         (fn [_db]
                           (deliver started true)
                           @release-error
                           (throw (AssertionError. "synthetic processing error")))}}}
          _ (d/create-database cfg)
          conn (d/connect cfg)
          local-writer (:writer @conn)]
      (try
        (d/transact conn [{:db/id 1 :n 0}])
        (let [fatal-callback (writer/dispatch! local-writer
                                                {:op 'fatal-op :args []})]
          (is (true? (deref started 10000 false)))
          ;; The fatal op owns the only processing loop until released. These
          ;; puts therefore enter the open buffered transaction queue before
          ;; the loop can close it.
          (let [buffered-callbacks
                (mapv (fn [n]
                        (writer/dispatch! local-writer
                                          {:op 'transact!
                                           :args [{:tx-data [{:db/id (+ n 2)
                                                              :n (inc n)}]}]}))
                      (range 7))]
            (deliver release-error true)
            (let [results (mapv take-with-timeout
                                (into [fatal-callback] buffered-callbacks))]
              (is (every? #(and (instance? Throwable %)
                                (not= ::timeout %))
                          results)
                  "the failed invocation and every buffered invocation resolve"))))
        (let [released (future
                         (try
                           (d/release conn)
                           :released
                           (catch Throwable _ :failed-loudly)))]
          (is (not= ::timeout (deref released 10000 ::timeout))
              "fatal processing shutdown completes"))
        (let [fresh (d/connect cfg)]
          (try
            (is (= #{0}
                   (set (d/q '[:find [?n ...] :where [_ :n ?n]] @fresh)))
                "buffered writes were failed rather than processed")
            (finally
              (d/release fresh))))
        (finally
          (deliver release-error true)
          (try (d/release conn) (catch Throwable _))
          (when (d/database-exists? cfg)
            (d/delete-database cfg)))))))
