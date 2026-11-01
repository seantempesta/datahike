(ns datahike.test.gc-fail-closed-test
  "A collection whose mark did not complete must not sweep (upstream 51cf69d3, #1117).

   Upstream lost a whole store: a mark walk died, the merge of the walks yielded one
   result fewer, and the sweep ran against a whitelist of the roster alone. Ours had the
   same holes, and one more: a walk on the async path that threw came back as its
   exception, which the vector of marks kept as a mark with no keys. Each test injects
   one way the mark can come back short, on both paths (the :sync writer's collection
   runs :sync? true), and asserts that not one key was deleted."
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.gc :as gc]
            [datahike.roster]
            [datahike.writing]
            [konserve.core :as k]
            [superv.async :refer [<?? S]])
  (:import [java.util Date]))

(def ^:private run-dir (str (System/getProperty "java.io.tmpdir") "/dh-gc-fail-closed-" (random-uuid)))

(def ^:private schema [{:db/ident       :age
                        :db/cardinality :db.cardinality/one
                        :db/valueType   :db.type/long}])

(defn- fresh-conn [label]
  (let [cfg {:store {:backend :file
                     :path (str run-dir "-" label)
                     :id (random-uuid)}
             :keep-history? true
             :schema-flexibility :write}]
    (d/delete-database cfg)
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn schema)
      (dotimes [i 20] (d/transact conn [{:age i}]))
      [cfg conn])))

(defn- store-keys [conn]
  (set (map :key (k/keys (:store @conn) {:sync? true}))))

(defn- collect [conn sync?]
  (try (let [r (gc/gc-storage! @conn (Date. 0) {:sync? sync?})]
         (if sync? r (<?? S r))
         nil)
       (catch Throwable e e)))

(defn- untouched? [cfg conn before e expected-type]
  (is (some? e) "the collection refuses instead of sweeping")
  (is (= expected-type (:type (ex-data e))))
  (is (= 0 (count (remove (store-keys conn) before))) "not one key was deleted")
  (let [c2 (d/connect cfg)]
    (try (is (= 20 (count (d/q '[:find ?e :where [?e :age]] @c2))))
         (finally (d/release c2)))))

(defn- closed-chan [] (doto (async/chan) async/close!))

(deftest a-mark-whose-walk-throws-does-not-sweep
  (doseq [sync? [false true]]
    (testing (str "a branch walk throws, :sync? " sync?)
      (let [[cfg conn] (fresh-conn (str "throws-" sync?))
            before (store-keys conn)
            e (with-redefs [datahike.writing/read-head
                            (fn [& _] (throw (ex-info "injected read failure" {:type :injected})))]
                (collect conn sync?))]
        (try (untouched? cfg conn before e :injected)
             (finally (d/release conn)))))))

(deftest an-unreadable-roster-does-not-sweep
  (doseq [sync? [false true]]
    (testing (str "the roster reads back as no branches, :sync? " sync?)
      (let [[cfg conn] (fresh-conn (str "roster-" sync?))
            before (store-keys conn)
            e (with-redefs [datahike.roster/roster (fn [& _] #{})]
                (collect conn sync?))]
        (try (untouched? cfg conn before e :gc-branches-unreadable)
             (finally (d/release conn)))))))

(deftest an-unreadable-branch-head-does-not-sweep
  (doseq [sync? [false true]]
    (testing (str "the branch head reads back as nothing, :sync? " sync?)
      (let [[cfg conn] (fresh-conn (str "head-" sync?))
            before (store-keys conn)
            orig datahike.writing/read-head
            e (with-redefs [datahike.writing/read-head
                            (fn [store k opts]
                              (if (= k :db)
                                (if (:sync? opts) nil (closed-chan))
                                (orig store k opts)))]
                (collect conn sync?))]
        (try (untouched? cfg conn before e :gc-branch-head-unreadable)
             (finally (d/release conn)))))))

(deftest a-healthy-collection-still-collects
  (doseq [sync? [false true]]
    (testing (str "a complete mark sweeps as before, :sync? " sync?)
      (let [[cfg conn] (fresh-conn (str "healthy-" sync?))]
        (try
          (let [r (gc/gc-storage! @conn (Date.) {:sync? sync?})
                swept (if sync? r (<?? S r))]
            (is (seq swept) "superseded snapshots are collected")
            (let [c2 (d/connect cfg)]
              (try (is (= 20 (count (d/q '[:find ?e :where [?e :age]] @c2))))
                   (finally (d/release c2)))))
          (finally (d/release conn)))))))
