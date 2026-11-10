(ns datahike.test.gc-value-key-test
  "A value out of line that a collection swept is written again by the next commit that
  holds it: the node cache's value keys (persistent-set value-key) are trusted only within
  the collection generation they were found in (datahike.index.interface/collection-key),
  read once for the nodes a commit writes. On Jolt (values go out of line only there), on LMDB, the store Seon runs."
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.index.interface :as di]
            [konserve.core :as k]
            [konserve.store] [konserve.jolt.store] [konserve.jolt.lmdb]))

;; a path prefix of this run's own: parallel runs (two worktrees' Seon forks steps) must not
;; delete-database each other's stores (as stress-test, fc2136b4)
(def ^:private run-dir (str (System/getProperty "java.io.tmpdir") "/dh-gcvk-" (random-uuid)))

(deftest a-commit-after-a-collection-rewrites-a-swept-value
  ;; [E :big V] committed on :x, then on :w (V found stored, its key cached under the datom);
  ;; both deleted and collected (V swept); then committed on :y. Before the generation,
  ;; :y's node named V by its cached key and was never re-written: :value-not-found on read.
  (let [cfg {:store {:backend :lmdb :path (str run-dir "-value-key-test") :default-serializer :FaslSerializer
                     :id #uuid "9c000000-0000-0000-0000-00000000c0de"}
             :keep-history? true :schema-flexibility :write
             :index-config {:value-out-of-line-bytes 1000}}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        c0 (d/connect cfg)
        _ (d/transact c0 [{:db/ident :big :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
        V (apply str "payload " (repeat 300 "abcd"))
        put! (fn [b] (d/branch! c0 :db b)
               (let [c (d/connect (assoc cfg :branch b))] (d/transact c [[:db/add 4000000 :big V]]) c))]
    (run! d/release [(put! :x) (put! :w)])
    (run! #(d/delete-branch! c0 %) [:x :w])
    (is (pos? (count @(d/gc-storage c0))) "the collection swept V with :x's and :w's nodes")
    (d/release (put! :y))
    (d/release c0)
    ;; every connection released: :y is read from the store, its node cache a new one
    (let [cy (d/connect (assoc cfg :branch :y))]
      (is (= #{[V]} (d/q '[:find ?v :where [_ :big ?v]] @cy)))
      (d/release cy))
    (d/delete-database cfg)))

(deftest a-commit-reads-the-collection-generation-once
  ;; the nodes one commit writes check their value keys against one read of the store's
  ;; generation, not one read a node: here four nodes (:eavt, :aevt and their temporal
  ;; twins) hold the three values the commit names again
  (let [cfg {:store {:backend :lmdb :path (str run-dir "-generation-once") :default-serializer :FaslSerializer
                     :id #uuid "9c000000-0000-0000-0000-00000000c0df"}
             :keep-history? true :schema-flexibility :write
             :index-config {:value-out-of-line-bytes 1000}}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)
        V #(apply str "payload " % " " (repeat 300 "abcd"))
        reads (atom 0)
        get k/get]
    (try
      (d/transact conn [{:db/ident :big :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                        {:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
      (d/transact conn (mapv (fn [i] {:db/id (- -1 i) :big (V i)}) (range 3)))
      (let [es (mapv first (d/q '[:find ?e :where [?e :big]] @conn))]
        (with-redefs [k/get (fn [store key & args]
                              (when (= di/collection-key key) (swap! reads inc))
                              (apply get store key args))]
          (d/transact conn (mapv (fn [e] [:db/add e :n 1]) es))
          (is (= 1 @reads) "one read of the generation for the commit's nodes")
          ;; a commit that failed after its flush leaves its queue, and the generation read
          ;; with it, undrained: the next flush reads its own
          (swap! (-> @conn :store :storage :pending-writes) vary-meta assoc
                 :datahike.index.persistent-set/generation (delay -1))
          (d/transact conn (mapv (fn [e] [:db/add e :n 2]) es))
          (is (= 2 @reads) "a flush reads the generation again, never one a failed commit left")))
      (finally
        (d/release conn)
        (d/delete-database cfg)))))
