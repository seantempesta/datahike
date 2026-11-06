(ns datahike.test.gc-keep-test
  "A collection with a recent cutoff leaves the commit graph whole, so a merge base is still found, and
  keeps whole the commits it is told to, whatever their age."
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [konserve.store] [konserve.jolt.store] [konserve.jolt.lmdb]))

(def ^:private run-dir (str (System/getProperty "java.io.tmpdir") "/dh-gckeep-" (random-uuid)))

(def ^:private schema [{:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(defn- store-of []
  {:store {:backend :lmdb :path (str run-dir "-keep") :default-serializer :FaslSerializer :id #uuid "9c000000-0000-0000-0000-00000000c1de"}
   :keep-history? true :schema-flexibility :write})

(defn- ns-of [db] (sort (map :v (d/datoms db :aevt :n))))

(deftest a-sweep-keeps-the-graph-and-the-commits-it-is-told-to
  (let [cfg (store-of)
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)
        tx! (fn [c n] (d/transact c {:tx-data [{:n n}]}) (d/commit-id @c))
        _ (d/transact conn {:tx-data schema})
        _ (tx! conn 1)
        forked (d/commit-id @conn)
        _ (d/branch! conn :db :b)
        b (d/connect (assoc cfg :branch :b))
        _ (tx! b 2)
        between (tx! conn 3)
        _ (tx! conn 4)
        base (d/merge-base conn :b :db)
        head-b (d/commit-id @b)
        before (ns-of @conn)
        hist (count (d/datoms (d/history @conn) :eavt))]
    (is (= forked base) "the branch point")
    (is (seq @(d/gc-storage conn (java.util.Date. (+ 1000 (System/currentTimeMillis))) {:datahike.gc/keep-commits #{base}}))
        "the sweep deleted something: the commits past the cutoff")
    (is (= base (d/merge-base conn :b :db)) "the merge base is still found: the graph stayed")
    (is (= [1] (ns-of (d/commit-as-db conn base))) "and a commit it was told to keep reads whole")
    (is (= before (ns-of @conn)) "the head reads the same")
    (is (= hist (count (d/datoms (d/history @conn) :eavt))) "and so does its history")
    (is (= [1 2] (ns-of (d/commit-as-db conn head-b))) "the other branch's head too")
    (is (thrown? Throwable (ns-of (d/commit-as-db conn between))) "a commit not kept is gone, and reading it says so")
    (d/release b) (d/release conn) (d/delete-database cfg)))
