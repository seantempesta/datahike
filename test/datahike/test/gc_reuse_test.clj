(ns datahike.test.gc-reuse-test
  "A value out of line that a commit reuses (its content key cached, or found in the store: nothing written,
  persistent-set value-key) while a collection runs is never swept under that commit. The sweep deletes an
  unreachable key written before its cutoff; a reused key keeps its first write's time, so the collection's
  guard spares it by key. On Jolt (values go out of line only there), on LMDB, the store Seon runs."
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [konserve.store] [konserve.jolt.store] [konserve.jolt.lmdb]))

(def ^:private run-dir (str (System/getProperty "java.io.tmpdir") "/dh-gcreuse-" (random-uuid)))

(deftest a-value-reused-during-a-sweep-survives-it
  ;; runtime, 2026-11-16: collection now runs beside live commits; a commit between the mark and the sweep that reused a value
  ;; the mark found unreachable lost it (:value-not-found when its head is read)
  (let [cfg {:store {:backend :lmdb :path (str run-dir "-reuse") :default-serializer :FaslSerializer
                     :id #uuid "9c000000-0000-0000-0000-0000000c0de2"}
             :keep-history? true :schema-flexibility :write
             :index-config {:value-out-of-line-bytes 1000}}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        c0 (d/connect cfg)
        _ (d/transact c0 [{:db/ident :big :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
        V (apply str "payload " (repeat 300 "abcd"))
        _ (d/branch! c0 :db :x)
        _ (d/branch! c0 :db :y)
        cx (d/connect (assoc cfg :branch :x))
        cy (d/connect (assoc cfg :branch :y))]
    ;; V is written once, on :x, and unreachable once :x is gone
    (d/transact cx [[:db/add 4000000 :big V]])
    (d/release cx)
    (d/delete-branch! c0 :x)
    ;; the reachable set is decided (the mark is done); before the sweep, :y commits V again: its key is in the store, so
    ;; nothing is written and its last write is :x's
    @(d/gc-storage c0 (java.util.Date. 0)
                   {:datahike.gc/reachable-extension (fn [_] (d/transact cy [[:db/add 4000001 :big V]]) #{})})
    (d/release cy)
    (d/release c0)
    (let [cy (d/connect (assoc cfg :branch :y))]
      (is (= #{[V]} (try (d/q '[:find ?v :where [_ :big ?v]] @cy) (catch Exception e (ex-data e)))) "the reused value is read back")
      (d/release cy))
    (d/delete-database cfg)))
