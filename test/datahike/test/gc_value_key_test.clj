(ns datahike.test.gc-value-key-test
  "A value out of line that a collection swept is written again by the next commit that
  holds it: the node cache's value keys (persistent-set value-key) are trusted only within
  the collection generation they were found in (datahike.index.interface/collection-key).
  On Jolt (values go out of line only there), on LMDB, the store Seon runs."
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
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
