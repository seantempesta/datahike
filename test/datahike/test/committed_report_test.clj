(ns datahike.test.committed-report-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datahike.api :as d]
            [datahike.committed-report :as reports]))

(use-fixtures :each
  (fn [test-fn]
    (reports/clear!)
    (try (test-fn)
         (finally (reports/clear!)))))

(deftest bounded-offer-is-ordered-and-fail-closed
  (let [generation (random-uuid)
        source (reports/open! :connection generation 2)]
    (is (= :accepted (reports/offer! source generation {:sequence 1})))
    (is (= :accepted (reports/offer! source generation {:sequence 2})))
    (is (= :overflow (reports/offer! source generation {:sequence 3})))
    (is (= :closed (reports/offer! source generation {:sequence 4})))
    (is (= {:sequence 1} (reports/poll! source)))
    (is (= {:sequence 2} (reports/poll! source)))
    (is (nil? (reports/poll! source)))
    (is (= {:status :gapped :queued 0 :offered 2 :delivered 2
            :overflowed 1 :stale-rejected 1 :abandoned 0}
           (reports/evidence source)))))

(deftest generation-fence-and-compare-remove-prevent-aba
  (let [old-generation (random-uuid)
        new-generation (random-uuid)
        old-source (reports/open! :connection old-generation 2)]
    (is (= :stale
           (reports/offer! old-source new-generation {:sequence :stale})))
    (is (= :accepted
           (reports/offer! old-source old-generation {:sequence :old})))
    (is (= :closed (:status (reports/close! old-source false))))
    (let [new-source (reports/open! :connection new-generation 2)]
      (reports/close! old-source false)
      (is (= :accepted
             (reports/offer! new-source new-generation {:sequence :new})))
      (is (= {:sequence :new} (reports/poll! new-source)))
      (is (= 1 (reports/active-source-count))))))

(deftest close-reports-drain-or-abandon-evidence
  (let [draining (reports/open! :drain (random-uuid) 2)
        abandoning (reports/open! :abandon (random-uuid) 2)]
    (reports/offer! draining (second (:scope draining)) {:sequence 1})
    (reports/offer! abandoning (second (:scope abandoning)) {:sequence 2})
    (is (= {:status :closed :queued 1 :offered 1 :delivered 0
            :overflowed 0 :stale-rejected 0 :abandoned 0}
           (reports/close! draining true)))
    (is (= {:sequence 1} (reports/poll! draining)))
    (is (= {:status :closed :queued 0 :offered 1 :delivered 0
            :overflowed 0 :stale-rejected 0 :abandoned 1}
           (reports/close! abandoning false)))
    (is (nil? (reports/poll! abandoning)))
    (is (zero? (reports/active-source-count)))))

(deftest publication-is-demand-opened-and-never-runs-consumer-work
  (let [generation (random-uuid)
        db {:cache-context {:datahike.cache/connection-id :connection
                            :datahike.cache/generation generation}}
        report {:sequence 1}]
    (is (nil? (reports/offer-committed! db report)))
    (is (zero? (reports/active-source-count)))
    (let [source (reports/open! :connection generation 1)]
      (is (= :accepted (reports/offer-committed! db report)))
      (is (identical? report (reports/poll! source))))))

(deftest durable-publication-offers-once-in-commit-order
  (let [configuration {:store {:backend :memory :id (random-uuid)}
                       :schema-flexibility :read}]
    (d/create-database configuration)
    (let [connection (d/connect configuration)
          {:datahike.cache/keys [connection-id generation]}
          (:cache-context @(:wrapped-atom connection))
          source (reports/open! connection-id generation 3)
          legacy (atom [])]
      (try
        (d/listen connection ::legacy
                  #(swap! legacy conj (get-in % [:tx-meta :sequence])))
        (doseq [sequence (range 3)]
          (d/transact connection
                      {:tx-data [{:db/id (+ sequence 1) :value sequence}]
                       :tx-meta {:sequence sequence}}))
        (is (= [0 1 2]
               (mapv #(get-in % [:tx-meta :sequence])
                     (repeatedly 3 #(reports/poll! source))))
            "the source observes the durable writer publication order")
        (is (= [0 1 2] @legacy)
            "the publication source preserves legacy listeners")
        (is (= {:status :open :queued 0 :offered 3 :delivered 3
                :overflowed 0 :stale-rejected 0 :abandoned 0}
               (reports/evidence source)))
        (finally
          (reports/close! source false)
          (d/unlisten connection ::legacy)
          (d/release connection)
          (d/delete-database configuration))))))
