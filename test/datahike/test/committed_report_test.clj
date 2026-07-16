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
    (is (identical? source (reports/poll-ready!)))
    (is (= :closed (reports/offer! source generation {:sequence 4})))
    (is (= {:sequence 1} (reports/poll! source)))
    (is (= {:sequence 2} (reports/poll! source)))
    (is (nil? (reports/poll! source)))
    (is (= {:datahike.committed-report/status
            :datahike.committed-report.status/gapped
            :datahike.committed-report/queued 0
            :datahike.committed-report/offered 2
            :datahike.committed-report/delivered 2
            :datahike.committed-report/overflowed 1
            :datahike.committed-report/stale-rejected 1
            :datahike.committed-report/abandoned 0}
           (reports/evidence source)))))

(deftest readiness-handoff-is-transition-driven-and-bounded
  (let [generation (random-uuid)
        source (reports/open! :connection generation 3)]
    (is (= {:datahike.committed-report.readiness/queued 0
            :datahike.committed-report.readiness/capacity 4096
            :datahike.committed-report.readiness/active-sources 1}
           (reports/readiness-evidence)))
    (is (= :accepted (reports/offer! source generation {:sequence 1})))
    (is (= :accepted (reports/offer! source generation {:sequence 2})))
    (is (= 1 (:datahike.committed-report.readiness/queued
              (reports/readiness-evidence))))
    (is (identical? source (reports/poll-ready!)))
    (is (nil? (reports/poll-ready!)))
    (is (= [{:sequence 1} {:sequence 2}]
           [(reports/poll! source) (reports/poll! source)]))
    (is (= :accepted (reports/offer! source generation {:sequence 3})))
    (is (identical? source (reports/poll-ready!)))
    (is (= {:sequence 3} (reports/poll! source)))))

(deftest gapped-source-closes-and-reopens-without-stale-readiness
  (let [generation (random-uuid)
        source (reports/open! :connection generation 1)]
    (is (= :accepted (reports/offer! source generation {:sequence 1})))
    (is (identical? source (reports/poll-ready!)))
    (is (= :overflow (reports/offer! source generation {:sequence 2})))
    (is (= :datahike.committed-report.status/gapped
           (:datahike.committed-report/status (reports/evidence source))))
    (is (= {:sequence 1} (reports/poll! source)))
    (reports/close! source false)
    (let [reopened (reports/open! :connection generation 1)]
      (is (not (identical? source reopened)))
      (is (= :accepted (reports/offer! reopened generation {:sequence 3})))
      (is (identical? reopened (reports/poll-ready!)))
      (is (= {:sequence 3} (reports/poll! reopened))))))

(deftest generation-fence-and-compare-remove-prevent-aba
  (let [old-generation (random-uuid)
        new-generation (random-uuid)
        old-source (reports/open! :connection old-generation 2)]
    (is (= :stale
           (reports/offer! old-source new-generation {:sequence :stale})))
    (is (= :accepted
           (reports/offer! old-source old-generation {:sequence :old})))
    (is (= :datahike.committed-report.status/closed
           (:datahike.committed-report/status
            (reports/close! old-source false))))
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
    (is (= {:datahike.committed-report/status
            :datahike.committed-report.status/closed
            :datahike.committed-report/queued 1
            :datahike.committed-report/offered 1
            :datahike.committed-report/delivered 0
            :datahike.committed-report/overflowed 0
            :datahike.committed-report/stale-rejected 0
            :datahike.committed-report/abandoned 0}
           (reports/close! draining true)))
    (is (= {:sequence 1} (reports/poll! draining)))
    (is (= {:datahike.committed-report/status
            :datahike.committed-report.status/closed
            :datahike.committed-report/queued 0
            :datahike.committed-report/offered 1
            :datahike.committed-report/delivered 0
            :datahike.committed-report/overflowed 0
            :datahike.committed-report/stale-rejected 0
            :datahike.committed-report/abandoned 1}
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
        (is (= {:datahike.committed-report/status
                :datahike.committed-report.status/open
                :datahike.committed-report/queued 0
                :datahike.committed-report/offered 3
                :datahike.committed-report/delivered 3
                :datahike.committed-report/overflowed 0
                :datahike.committed-report/stale-rejected 0
                :datahike.committed-report/abandoned 0}
               (reports/evidence source)))
        (finally
          (reports/close! source false)
          (d/unlisten connection ::legacy)
          (d/release connection)
          (d/delete-database configuration))))))

(deftest final-connection-release-fences-and-abandons-its-source
  (let [configuration {:store {:backend :memory :id (random-uuid)}
                       :schema-flexibility :read}]
    (d/create-database configuration)
    (let [connection (d/connect configuration)
          {:datahike.cache/keys [connection-id generation]}
          (:cache-context @(:wrapped-atom connection))]
      (reports/open! connection-id generation 2)
      (d/transact connection [{:db/id 1 :value :accepted}])
      (is (= 1 (reports/active-source-count)))
      (is (= 1 (:datahike.committed-report.readiness/queued
                (reports/readiness-evidence))))
      (d/release connection)
      (is (zero? (reports/active-source-count)))
      (is (zero? (:datahike.committed-report.readiness/queued
                  (reports/readiness-evidence))))
      (d/delete-database configuration))))
