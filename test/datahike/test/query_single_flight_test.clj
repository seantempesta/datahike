(ns datahike.test.query-single-flight-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.query.single-flight :as single-flight])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(defn- cache-reader [cache]
  #(when (contains? @cache :value) {:value (:value @cache)}))

(deftest identical-cold-misses-compute-once
  (doseq [n [2 8 32]]
    (single-flight/clear!)
    (let [start (CountDownLatch. 1)
          calls (atom 0)
          cache (atom {})
          workers (doall
                   (for [_ (range n)]
                     (future
                       (.await start)
                       (single-flight/execute!
                        [[:db :generation :commit] :same]
                        (cache-reader cache)
                        (fn [_] (swap! calls inc) (Thread/sleep 75) :result)
                        #(swap! cache assoc :value %)))))]
      (.countDown start)
      (is (= (vec (repeat n :result)) (mapv deref workers)))
      (is (= 1 @calls) (str n " identical misses must share one owner"))
      (is (zero? (:active-flights (single-flight/metrics)))))))

(deftest distinct-keys-progress-in-parallel
  (single-flight/clear!)
  (let [n 8
        entered (CountDownLatch. n)
        release (CountDownLatch. 1)
        active (atom 0)
        peak (atom 0)
        workers
        (doall
         (for [i (range n)]
           (future
             (single-flight/execute!
              [[(keyword (str "db-" i)) :generation :commit] :same-query]
              (constantly nil)
              (fn [_]
                (let [now (swap! active inc)]
                  (swap! peak max now)
                  (.countDown entered)
                  (.await release 10 TimeUnit/SECONDS)
                  (swap! active dec)
                  i))
              (fn [_])))))]
    (is (.await entered 10 TimeUnit/SECONDS))
    (.countDown release)
    (is (= (vec (range n)) (mapv deref workers)))
    (is (= n @peak))
    (is (zero? (:active-flights (single-flight/metrics))))))

(deftest shared-failure-cleans-up-and-retries
  (single-flight/clear!)
  (let [start (CountDownLatch. 1)
        calls (atom 0)
        run (fn [compute]
              (single-flight/execute!
               [[:db :generation :commit] :failure]
               (constantly nil) compute (fn [_])))
        workers (doall
                 (for [_ (range 8)]
                   (future
                     (.await start)
                     (try (run (fn [_] (swap! calls inc)
                                 (Thread/sleep 75)
                                 (throw (ex-info "shared" {}))))
                          (catch Exception _ :failed)))))]
    (.countDown start)
    (is (= (vec (repeat 8 :failed)) (mapv deref workers)))
    (is (= 1 @calls))
    (is (zero? (:active-flights (single-flight/metrics))))
    (is (= :recovered (run (fn [_] (swap! calls inc) :recovered))))
    (is (= 2 @calls))))

(deftest cancellation-reentrancy-and-overflow-remain-bounded
  (testing "requests detach independently and the final request signals"
    (single-flight/clear!)
    (let [before (single-flight/metrics)
          owner (single-flight/acquire! [[:db :g :c] :cancel])
          waiter @(future (single-flight/acquire! [[:db :g :c] :cancel]))]
      (is (= :owner (:state owner)))
      (is (= :waiter (:state waiter)))
      (is (single-flight/cancel-waiter! [[:db :g :c] :cancel]
                                        (:request-id waiter)))
      (is (single-flight/cancel-waiter! [[:db :g :c] :cancel]
                                        (:request-id owner)))
      (is (true? @(:cancel (:entry owner))))
      (is (= (inc (:last-waiters before))
             (:last-waiters (single-flight/metrics))))
      (single-flight/complete! [[:db :g :c] :cancel]
                               (:entry owner) {:status :ok :value :done})
      (is (zero? (:active-flights (single-flight/metrics))))))

  (testing "same-owner recursion bypasses its own completion cell"
    (single-flight/clear!)
    (let [calls (atom 0)
          key [[:db :g :c] :recursive]
          run (fn run []
                (single-flight/execute!
                 key (constantly nil)
                 (fn [_] (if (= 1 (swap! calls inc)) (run) :inner))
                 (fn [_])))]
      (is (= :inner (run)))
      (is (= 2 @calls))
      (is (zero? (:active-flights (single-flight/metrics))))))

  (testing "overflow bypass is explicit and retains no extra entry"
    (single-flight/clear!)
    (let [entered (CountDownLatch. 1)
          release (CountDownLatch. 1)
          before (single-flight/metrics)]
      (binding [single-flight/*max-active-flights* 1]
        (let [owner (future
                      (single-flight/execute!
                       [[:db :g :c] :owner] (constantly nil)
                       (fn [_] (.countDown entered)
                         (.await release 10 TimeUnit/SECONDS)
                         :owner)
                       (fn [_])))]
          (is (.await entered 10 TimeUnit/SECONDS))
          (is (= :overflow
                 (single-flight/execute!
                  [[:db :g :c] :overflow] (constantly nil)
                  (constantly :overflow) (fn [_]))))
          (is (= 1 (:active-flights (single-flight/metrics))))
          (.countDown release)
          (is (= :owner @owner))))
      (is (= (inc (:overflows before))
             (:overflows (single-flight/metrics))))
      (is (zero? (:active-flights (single-flight/metrics)))))))

(deftest exact-scope-close-fails-waiters-and-rejects-stale-owner-cleanup
  (single-flight/clear!)
  (let [key [[::connection ::generation ::commit] :query]
        owner (single-flight/acquire! key)
        waiter (future
                 (let [handle (single-flight/acquire! key)
                       completion @(:completion handle)]
                   (:type (ex-data (:throwable completion)))))]
    (loop [attempts 100]
      (when (and (pos? attempts)
                 (< (:active-callers (single-flight/metrics)) 2))
        (Thread/sleep 5)
        (recur (dec attempts))))
    (single-flight/close-scope! ::connection ::generation)
    (is (= :datahike/query-cache-scope-closed @waiter))
    (is (zero? (:active-flights (single-flight/metrics))))
    (single-flight/complete! key (:entry owner) {:status :ok :value :late})
    (is (zero? (:active-flights (single-flight/metrics))))))

(deftest clear-fences-late-publication-and-stale-owner-cannot-remove-successor
  (single-flight/clear!)
  (let [key [[:db :generation :commit] :clear]
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        cache (atom {})
        stores (atom 0)
        old-owner
        (future
          (single-flight/execute!
           key (cache-reader cache)
           (fn [cancel]
             (.countDown entered)
             (.await release 10 TimeUnit/SECONDS)
             (is (true? @cancel))
             :old)
           (fn [value]
             (swap! stores inc)
             (swap! cache assoc :value value)
             true)))]
    (is (.await entered 10 TimeUnit/SECONDS))
    (single-flight/clear!)
    (let [successor (single-flight/acquire! key)]
      (is (= :owner (:state successor)))
      (.countDown release)
      (is (= :old @old-owner))
      (is (zero? @stores) "a cleared owner cannot publish a completed value")
      (is (= 1 (:active-flights (single-flight/metrics)))
          "stale completion cannot remove a successor with the same key")
      (single-flight/complete! key (:entry successor)
                               {:status :ok :value :new})
      (is (zero? (:active-flights (single-flight/metrics)))))))

(deftest external-request-cancellation-wakes-only-that-caller
  (single-flight/clear!)
  (let [key [[:db :generation :commit] :addressable]
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        owner-id (random-uuid)
        waiter-id (random-uuid)
        owner (future
                (single-flight/execute!
                 key owner-id (constantly nil)
                 (fn [cancel]
                   (.countDown entered)
                   (.await release 10 TimeUnit/SECONDS)
                   (is (true? @cancel))
                   :owner-result)
                 (fn [_] false) nil))]
    (is (.await entered 10 TimeUnit/SECONDS))
    (let [waiter (future
                   (try
                     (single-flight/execute!
                      key waiter-id (constantly nil)
                      (fn [_] :must-not-compute) (fn [_] false) nil)
                     (catch Exception error (:type (ex-data error)))))]
      (loop [attempts 100]
        (when (and (pos? attempts)
                   (< (:active-callers (single-flight/metrics)) 2))
          (Thread/yield)
          (recur (dec attempts))))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"already active"
           (single-flight/acquire! key waiter-id)))
      (let [cancelled (single-flight/cancel! waiter-id)]
        (is (:datahike.query.cancel/detached? cancelled))
        (is (false? (:datahike.query.cancel/last-waiter? cancelled))))
      (is (= :datahike/query-canceled @waiter))
      (let [cancelled (single-flight/cancel! owner-id)]
        (is (:datahike.query.cancel/last-waiter? cancelled))
        (is (:datahike.query.cancel/cooperative-signal-set? cancelled)))
      (.countDown release)
      (is (= :owner-result @owner))
      (is (false? (:datahike.query.cancel/found?
                   (single-flight/cancel! waiter-id))))
      (is (zero? (:active-flights (single-flight/metrics)))
          "completion removes the flight after all callers detach"))))

(deftest acquired-owner-binds-the-run-thread-and-hands-off-once
  (single-flight/clear!)
  (let [key [[:db :generation :commit] :transferred]
        calls (atom 0)
        completion (promise)
        run (fn run []
              (single-flight/execute!
               key nil (constantly nil)
               (fn [_]
                 (if (= 1 (swap! calls inc))
                   (run)
                   :inner))
               (fn [_] false) nil nil nil))
        call
        (single-flight/acquire-execution!
         key "transferred-owner" (constantly nil)
         (fn [_]
           (if (= 1 (swap! calls inc))
             (run)
             :inner))
         (fn [_] false) nil nil nil false)]
    (is (= :run (single-flight/call-state call)))
    (is (true? (single-flight/on-call-complete! call #(deliver completion %))))
    (is (false? (single-flight/on-call-complete! call (fn [_] nil)))
        "one call retains at most one nonblocking handoff")
    (is (= :inner @(future (single-flight/run-call! call))))
    (is (= {:status :ok :value :inner} @completion))
    (is (= 2 @calls)
        "the nested same-key call bypasses on the actual run thread")
    (is (zero? (:active-flights (single-flight/metrics))))))

(deftest unstarted-owner-cancel-removes-the-flight-before-run
  (single-flight/clear!)
  (let [key [[:db :generation :commit] :unstarted]
        request-id "unstarted-owner"
        calls (atom 0)
        completion (promise)
        call
        (single-flight/acquire-execution!
         key request-id (constantly nil)
         (fn [_] (swap! calls inc) :must-not-run)
         (fn [_] false) nil nil nil false)]
    (single-flight/on-call-complete! call #(deliver completion %))
    (let [cancelled (single-flight/cancel! request-id)]
      (is (:datahike.query.cancel/detached? cancelled))
      (is (:datahike.query.cancel/last-waiter? cancelled)))
    (is (= :datahike/query-canceled
           (:type (ex-data (:throwable @completion)))))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"canceled"
                          (single-flight/run-call! call)))
    (is (zero? @calls))
    (is (zero? (:active-flights (single-flight/metrics))))
    (is (zero? (:active-callers (single-flight/metrics))))))

(deftest final-waiter-cancel-identifies-the-unstarted-owner-job
  (single-flight/clear!)
  (let [key [[:db :generation :commit] :detached-owner]
        owner-id "detached-owner"
        waiter-id "final-waiter"
        calls (atom 0)
        owner
        (single-flight/acquire-execution!
         key owner-id (constantly nil)
         (fn [_] (swap! calls inc))
         (fn [_] false) nil nil nil false)
        waiter
        (single-flight/acquire-execution!
         key waiter-id (constantly nil)
         (fn [_] (swap! calls inc))
         (fn [_] false) nil nil nil false)]
    (is (= :run (single-flight/call-state owner)))
    (is (= :waiting (single-flight/call-state waiter)))
    (let [owner-cancel (single-flight/cancel! owner-id)]
      (is (false? (:datahike.query.cancel/last-waiter? owner-cancel)))
      (is (false? (:datahike.query.cancel/unstarted-owner? owner-cancel)))
      (is (not (contains? owner-cancel
                          :datahike.query.cancel/owner-request-id))))
    (let [waiter-cancel (single-flight/cancel! waiter-id)]
      (is (:datahike.query.cancel/last-waiter? waiter-cancel))
      (is (:datahike.query.cancel/unstarted-owner? waiter-cancel))
      (is (= owner-id
             (:datahike.query.cancel/owner-request-id waiter-cancel))))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"canceled"
                          (single-flight/run-call! owner)))
    (is (zero? @calls))
    (is (zero? (:active-flights (single-flight/metrics))))))

(deftest running-owner-cancel-retains-physical-ownership-until-finish
  (single-flight/clear!)
  (let [key [[:db :generation :commit] :running-cancel]
        request-id "running-owner"
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        completion (promise)
        call
        (single-flight/acquire-execution!
         key request-id (constantly nil)
         (fn [cancel]
           (.countDown entered)
           (.await release 10 TimeUnit/SECONDS)
           (is (true? @cancel))
           :late-value)
         (fn [_] false) nil nil nil false)
        running (future (single-flight/run-call! call))]
    (single-flight/on-call-complete! call #(deliver completion %))
    (is (.await entered 10 TimeUnit/SECONDS))
    (let [cancelled (single-flight/cancel! request-id)]
      (is (:datahike.query.cancel/last-waiter? cancelled))
      (is (false? (:datahike.query.cancel/unstarted-owner? cancelled)))
      (is (not (contains? cancelled
                          :datahike.query.cancel/owner-request-id)))
      (is (:datahike.query.cancel/cooperative-signal-set? cancelled)))
    (is (= :datahike/query-canceled
           (:type (ex-data (:throwable @completion)))))
    (is (= 1 (:active-flights (single-flight/metrics)))
        "running physical work retains the exact entry")
    (.countDown release)
    (is (= :late-value @running)
        "synchronous owner semantics remain unchanged after detachment")
    (is (zero? (:active-flights (single-flight/metrics))))))

(deftest host-overflow-rejects-without-direct-computation
  (single-flight/clear!)
  (binding [single-flight/*max-active-flights* 1]
    (let [owner (single-flight/acquire! [[:db :g :c] :held] "held")
          calls (atom 0)
          rejected
          (single-flight/acquire-execution!
           [[:db :g :c] :rejected] "rejected" (constantly nil)
           (fn [_] (swap! calls inc)) (fn [_] false) nil nil nil true)
          completion (promise)]
      (is (= :rejected (single-flight/call-state rejected)))
      (single-flight/on-call-complete! rejected #(deliver completion %))
      (is (= :datahike/query-coordinator-full
             (:type (ex-data (:throwable @completion)))))
      (is (zero? @calls))
      (single-flight/complete! [[:db :g :c] :held] (:entry owner)
                               {:status :ok :value :done})
      (is (zero? (:active-flights (single-flight/metrics)))))))
