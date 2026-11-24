(ns datahike.test.recount-jolt-test
  "Verify PSS 0.5.148 subtree count preservation and datahike.index.recount on Jolt :sync LMDB stores."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.index.recount :as recount]
            [datahike.query.estimate :as est]
            [datahike.versioning :as dv]
            [konserve.jolt.store]))

(deftest fresh-sync-lmdb-store-keeps-all-branch-counts
  (let [dir (str "/tmp/dh-recount-jolt-" (System/nanoTime))
        cfg {:store {:backend :lmdb :path dir :compressor :zstd :id (java.util.UUID/randomUUID)}
             :schema-flexibility :write
             :keep-history? true
             :index :datahike.index/persistent-set
             :index-config {:branching-factor 8 :diff-buf-size 16 :edge-append? true}
             :value-out-of-line-bytes 512
             :writer {:backend :sync}}
        schema [{:db/ident :item/k :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                {:db/ident :item/tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one :db/index true}
                {:db/ident :item/body :db/valueType :db.type/string :db/cardinality :db.cardinality/one}]]
    (try
      (d/create-database cfg)
      (let [c (d/connect cfg)]
        (d/transact c schema)
        (d/transact c (for [i (range 300)] {:item/k i :item/tag (if (even? i) :even :odd) :item/body (apply str (repeat 600 "z"))}))
        (d/release c))
      (dotimes [g 4]
        (let [c (d/connect cfg)]
          (d/transact c (for [j (range 40)] {:item/k (+ 1000 (* g 40) j) :item/tag :added :item/body (str "g-" g "-" j)}))
          (d/transact c (for [j (range 15)] [:db/retractEntity [:item/k (+ (* g 15) j)]]))
          (d/release c)))
      (let [c (d/connect cfg)
            db @c
            cen (recount/census c)]
        (is (:complete? cen))
        (is (every? (comp zero? :unknown) (vals (:indices cen))))
        (is (est/counted-indices? db))
        (is (= 160 (d/q '[:find (count ?e) . :where [?e :item/tag :added]] db)))
        (is (:complete? (recount/recount! c)))
        (d/release c))
      (finally
        (d/delete-database cfg)))))

(deftest recount-commit-records-empty-changes-for-d-changes
  (let [dir (str "/tmp/dh-recount-changes-" (System/nanoTime))
        cfg {:store {:backend :lmdb :path dir :compressor :zstd :id (java.util.UUID/randomUUID)}
             :schema-flexibility :write
             :keep-history? true
             :index :datahike.index/persistent-set
             :index-config {:branching-factor 8 :diff-buf-size 16 :edge-append? true}
             :writer {:backend :sync}}]
    (try
      (d/create-database cfg)
      (let [c (d/connect cfg)
            _ (d/transact c [{:db/ident :k/v :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
            _ (d/transact c [{:k/v 1} {:k/v 2}])
            cid-before (get-in @c [:meta :datahike/commit-id])
            ;; Clear :datahike/index-counts mark in memory so recount! performs a commit
            _ (swap! (:wrapped-atom c) update :meta dissoc :datahike/index-counts)
            _ (recount/recount! c)
            cid-after (get-in @c [:meta :datahike/commit-id])]
        (is (not= cid-before cid-after))
        (is (= [] (vec (dv/changes c cid-before cid-after))))
        (d/release c))
      (finally
        (d/delete-database cfg)))))
