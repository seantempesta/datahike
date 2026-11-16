(ns datahike.test.changes-test
  "What each commit keeps beside its record (datahike.writing/commit-changes-key): {:base the commit its value
  was built on, :datoms what the commit added}, and d/changes walking them by :base. The writer applies a
  transaction to the one before it while that one commits (its transaction loop runs ahead of its commit loop),
  so a commit's record must name the commit before it and hold its own batch's datoms only, never every datom
  since the writer began."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.writing :as w]
            [konserve.core :as k]))

(defn- cfg [] {:store {:backend :memory :id (random-uuid)} :schema-flexibility :read :keep-history? true})

(defn- changes-of [conn cid] (k/get (:store @conn) (w/commit-changes-key cid) nil {:sync? true}))

(deftest a-commits-changes-name-the-commit-before-it
  (let [c (cfg) _ (d/create-database c) conn (d/connect c)]
    (try
      (d/transact conn [{:db/id -1 :n 1}])
      (let [a (d/commit-id @conn)]
        (d/transact conn [{:db/id -1 :n 2}])
        (let [b (d/commit-id @conn)
              r (changes-of conn b)]
          (testing "the second commit's record"
            (is (= a (:base r)) "names the first commit as its base")
            (is (= #{2} (set (keep #(when (= :n (:a %)) (:v %)) (:datoms r)))) "and holds only its own transaction's datoms"))
          (is (= #{2} (set (keep #(when (= :n (:a %)) (:v %)) (d/changes conn a b)))) "d/changes from the first reads the second's")))
      (finally (d/release conn)))))

(deftest changes-from-a-parent-branchs-commit-reach-a-childs-head
  ;; data, 2026-11-16 (seon.agent-test/a-wake-shows-what-moved-not-the-page-again): from a commit on a branch's parent to the
  ;; branch's head, the :base walk passed it and ended at the store's first commit (:datahike/changes-unrecorded)
  (let [c (cfg) _ (d/create-database c) m (d/connect c)]
    (d/transact m [{:db/id -1 :n 1}])
    (d/branch! m :db :p)
    (let [p (d/connect (assoc c :branch :p))]
      (try
        (d/transact p [{:db/id -1 :n 2}])
        (let [from (d/commit-id @p)]
          (d/transact p [{:db/id -1 :n 3}])
          (d/branch! m :p :c)
          (let [ch (d/connect (assoc c :branch :c))]
            (try
              (d/transact ch [{:db/id -1 :n 4}])
              (is (= #{3 4} (set (keep #(when (= :n (:a %)) (:v %)) (d/changes ch from (d/commit-id @ch))))))
              (finally (d/release ch)))))
        (finally (d/release p) (d/release m))))))

(deftest changes-from-a-commit-off-the-chain-refuses-where-it-passes-it
  ;; context, 2026-11-16: a from not on to's :base chain read every commit to the store's first before refusing
  ;; (59M ticks between two of root's turns); the walk stops at the first commit no newer than from's generation
  (let [c (cfg) _ (d/create-database c) m (d/connect c)]
    (d/transact m [{:db/id -1 :n 1}])
    (d/branch! m :db :side)
    (let [side (d/connect (assoc c :branch :side))]
      (try
        (d/transact side [{:db/id -1 :n 2}])
        (let [from (d/commit-id @side)]
          (dotimes [i 5] (d/transact m [{:db/id -1 :n (+ 10 i)}]))
          (let [e (try (d/changes m from (d/commit-id @m)) nil (catch Exception e (ex-data e)))]
            (is (= :datahike/changes-not-ancestor (:type e)) (pr-str e))))
        (finally (d/release side) (d/release m))))))
