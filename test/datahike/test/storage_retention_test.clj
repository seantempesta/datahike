(ns datahike.test.storage-retention-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.core.async :as async]
            [datahike.api :as d]
            [datahike.writer :as writer])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(deftest history-setting-is-adopted-or-refused-by-the-store
  (doseq [history? [true false]]
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :keep-history? history?}]
      (d/create-database cfg)
      (try
        (doseq [request [(dissoc cfg :keep-history?) cfg]]
          (let [c (d/connect request)]
            (try (is (= history? (get-in @c [:config :keep-history?])))
                 (let [shared (d/connect (dissoc cfg :keep-history?))]
                   (is (identical? c shared))
                   (d/release shared))
                 (is (= :create-time-fixed-index-config-mismatch
                        (try (d/connect (assoc cfg :keep-history? (not history?)))
                             (catch Exception e (:type (ex-data e))))))
                 (finally (d/release c)))))
        (let [failure (try (d/connect (assoc cfg :keep-history? (not history?)))
                           (catch Exception e (ex-data e)))]
          (is (= :create-time-fixed-index-config-mismatch (:type failure)))
          (is (= {:given (not history?) :stored history?}
                 (get-in failure [:conflicts :keep-history?]))))
        (finally (d/delete-database cfg))))))

(deftest shared-holder-survives-and-draining-open-refuses
  (let [cfg {:store {:backend :memory :id (random-uuid)}}
        _ (d/create-database cfg)
        a (d/connect cfg)
        b (d/connect cfg)
        entered (CountDownLatch. 1)
        proceed (async/promise-chan)
        shutdown writer/shutdown]
    (try
      (is (identical? a b))
      (d/release a)
      (is (map? @b))
      (with-redefs [writer/shutdown
                    (fn [w]
                      (let [result (async/promise-chan)]
                        (.countDown entered)
                        (async/go
                          (async/<! proceed)
                          (async/>! result (or (async/<! (shutdown w)) true)))
                        result))]
        (let [released (future (d/release b))]
          (try
            (is (.await entered 2 TimeUnit/SECONDS))
            (is (= :connection-is-being-released
                   (try (d/connect cfg) (catch Exception e (:type (ex-data e))))))
            (finally (async/>!! proceed true)))
          (is (nil? (deref released 2000 ::timeout)))))
      (let [c (d/connect cfg)]
        (try (is (map? @c)) (finally (d/release c))))
      (finally
        (async/put! proceed true)
        (d/delete-database cfg)))))
