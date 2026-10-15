(ns datahike.test.merge-base-test
  "datahike.versioning/merge-base: git's merge base over per-commit graph
   records (parents + generation)."
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.versioning :as v]
            [datahike.writing :as w]
            [konserve.core :as k]
            [superv.async :refer [<?? S]])
  (:import [java.util Date Random]))

(defn- cfg []
  {:store {:backend :memory :id (random-uuid)}
   :keep-history? true
   :schema-flexibility :write
   :initial-tx [{:db/ident :k/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}]})

(defn- tx! [conn n] (d/transact conn [{:k/n (long n)}]) (d/commit-id @conn))

(defn- graph-record [conn cid]
  (k/get (:store @conn) (w/commit-graph-key cid) nil {:sync? true}))

(defn- forward-merge!
  "What Seon's forward merge writes: one transaction on `chain` carrying `main`'s head as a parent."
  [chain main n]
  @(d/merge-db! chain {:parents #{(d/commit-id @main)} :tx-data [{:k/n (long n)}]})
  (d/commit-id @chain))

(defn- refusal [f] (try (f) nil (catch Exception e (ex-data e))))

(defn- with-store [c f]
  (d/create-database c)
  (let [main (d/connect c)]
    (try (f main) (finally (d/release main) (d/delete-database c)))))

(defn- on-branch [main branch f]
  (let [conn (d/connect (assoc (:config @main) :branch branch))]
    (try (f conn) (finally (d/release conn)))))

(deftest every-commit-writes-its-parents-and-generation
  (with-store (cfg)
    (fn [main]
      (let [initial (d/commit-id @main)
            root (first (d/parent-commit-ids @main))
            c1 (tx! main 1)]
        (is (= {:datahike/parents #{} :datahike/generation 1} (graph-record main root)))
        (is (= {:datahike/parents #{root} :datahike/generation 2} (graph-record main initial)))
        (is (= {:datahike/parents #{initial} :datahike/generation 3} (graph-record main c1)))))))

(deftest the-merge-base-follows-the-chain-lifecycle
  (with-store (cfg)
    (fn [main]
      (tx! main 0)
      (let [fork (d/commit-id @main)]
        (v/branch! main :db :chain)
        (on-branch main :chain
          (fn [chain]
            (dotimes [i 3] (tx! main i) (tx! chain i))
            (testing "fork, then both advance: the base is the fork point"
              (is (= fork (d/merge-base main :chain :db)))
              (is (= fork (d/merge-base main (d/commit-id @chain) (d/commit-id @main)))))
            (let [m (d/commit-id @main)]
              (forward-merge! chain main 10)
              (testing "a forward merge carrying main's head: the base becomes that main commit"
                (is (= m (d/merge-base main :chain :db))))
              (tx! main 11) (tx! main 12) (tx! chain 13)
              (testing "main advances again: the base is unchanged"
                (is (= m (d/merge-base main :chain :db))))
              (testing "relaunch: a branch forked from the chain's end commit has the same base"
                (v/branch! main (d/commit-id @chain) :relaunch)
                (on-branch main :relaunch
                  (fn [relaunch]
                    (tx! relaunch 20) (tx! relaunch 21)
                    (is (= m (d/merge-base main :relaunch :db))))))
              (testing "one side an ancestor of the other, and a commit with itself"
                (is (= m (d/merge-base main m (d/commit-id @chain))))
                (is (= m (d/merge-base main m m)))))))))))

(deftest a-criss-cross-refuses-naming-both-bases
  (with-store (cfg)
    (fn [main]
      (tx! main 0)
      (v/branch! main :db :chain)
      (on-branch main :chain
        (fn [chain]
          (let [m1 (tx! main 1) c1 (tx! chain 1)]
            (forward-merge! chain main 2)                              ; chain takes m1
            @(d/merge-db! main {:parents #{c1} :tx-data [{:k/n 3}]})   ; main takes c1
            (let [r (refusal #(d/merge-base main :chain :db))]
              (is (= :merge-base-criss-cross (:type r)))
              (is (= #{m1 c1} (:merge-bases r))))))))))

(defn- ancestors-of [parents c]
  (loop [seen #{} todo [c]]
    (if-let [x (peek todo)]
      (if (seen x) (recur seen (pop todo)) (recur (conj seen x) (into (pop todo) (parents x))))
      seen)))

(defn- graph-keys [store]
  (->> (k/keys store {:sync? true}) (map :key)
       (filter #(and (vector? %) (= :datahike/commit-graph (first %))))))

(deftest legacy-commits-without-a-record-give-the-same-answers
  (with-store (cfg)
    (fn [main]
      (tx! main 0)
      (v/branch! main :db :chain)
      (on-branch main :chain
        (fn [chain]
          (let [store (:store @main)]
            (dotimes [i 3] (tx! main i) (tx! chain i))
            (forward-merge! chain main 5)
            (tx! main 6) (tx! chain 7)
            (let [heads [(d/commit-id @chain) (d/commit-id @main)]
                  before (apply d/merge-base main heads)
                  records (into {} (map (fn [gk] [gk (k/get store gk nil {:sync? true})])) (graph-keys store))]
              ;; the store as it was before graph records existed
              (doseq [gk (keys records)] (k/dissoc store gk {:sync? true}))
              (testing "with no record anywhere the walk derives generations from the commit records"
                (is (= before (apply d/merge-base main heads)))
                (is (empty? (graph-keys store)) "a read writes nothing"))
              (testing "the next commit writes its own record and its ancestors' derived ones, equal to the originals"
                (tx! chain 8)
                (let [head (d/commit-id @chain)
                      parents #(get-in (k/get store % nil {:sync? true}) [:meta :datahike/parents])
                      lineage (disj (ancestors-of parents head) head)]
                  (is (= (select-keys records (map w/commit-graph-key lineage))
                         (into {} (map (fn [c] [(w/commit-graph-key c) (graph-record main c)])) lineage)))
                  (is (= (count lineage) (dec (count (graph-keys store))))
                      "only the new commit's lineage is written; main's newer commits stay without a record")
                  (is (= (inc (:datahike/generation (get records (w/commit-graph-key (first heads)))))
                         (:datahike/generation (graph-record main head)))))
                (is (= before (d/merge-base main (d/commit-id @chain) (second heads))))))))))))

(deftest gc-reclaims-the-record-with-its-commit
  (with-store (assoc (cfg) :store {:backend :file :id (random-uuid)
                                   :path (str "target/merge-base-gc-" (random-uuid))})
    (fn [main]
      (tx! main 0)
      (v/branch! main :db :gone)
      (let [own (on-branch main :gone (fn [gone] (vec (repeatedly 3 #(tx! gone 1)))))
            kept (tx! main 2)]
        (v/delete-branch! main :gone)
        (<?? S (d/gc-storage main (Date. 0)))
        (is (every? #(nil? (k/get (:store @main) % nil {:sync? true})) own) "the commits are swept")
        (is (every? #(nil? (graph-record main %)) own) "and their records with them")
        (is (some? (graph-record main kept)) "a live commit keeps its record")))))

(deftest a-store-without-commit-records-refuses-by-name
  (with-store (assoc (cfg) :commit-graph? false)
    (fn [main]
      (tx! main 0)
      (v/branch! main :db :other)
      (is (empty? (graph-keys (:store @main))) "no record is written")
      (is (= :commit-graph-disabled (:type (refusal #(d/merge-base main :other :db)))))
      (is (= :commit-graph-disabled (:type (refusal #(d/merge-base (:store @main) :other :db))))))))

(deftest unknown-inputs-refuse-by-name
  (with-store (cfg)
    (fn [main]
      (is (= :commit-not-found (:type (refusal #(d/merge-base main (random-uuid) :db)))))
      (is (= :branch-does-not-exist (:type (refusal #(d/merge-base main :nope :db))))))))

;; Property: on random histories (transactions, branches, cross merges) the walk equals the maximal elements of the
;; brute-force common-ancestor set, or refuses naming exactly that set when it has several.

(defn- brute-bases [parents a b]
  (let [common (set/intersection (ancestors-of parents a) (ancestors-of parents b))]
    (into #{} (remove (fn [x] (some #(and (not= x %) (contains? (ancestors-of parents %) x)) common))) common)))

(defn- random-history [^Random rnd main n-ops]
  (loop [i 0 conns {:db main}]
    (if (= i n-ops)
      conns
      (let [names (vec (keys conns)) pick #(nth % (.nextInt rnd (count %)))]
        (recur (inc i)
               (case (.nextInt rnd 5)
                 (0 1 2) (do (tx! (conns (pick names)) i) conns)
                 3 (let [from (pick names) nb (keyword (str "b" i))]
                     (v/branch! main from nb)
                     (assoc conns nb (d/connect (assoc (:config @main) :branch nb))))
                 4 (let [into-b (pick names) others (vec (remove #{into-b} names))]
                     (when (seq others)
                       (forward-merge! (conns into-b) (conns (pick others)) i))
                     conns)))))))

(deftest the-walk-equals-the-brute-force-best-common-ancestors
  (doseq [seed (range 6)]
    (with-store (cfg)
      (fn [main]
        (let [conns (random-history (Random. seed) main 40)
              store (:store @main)
              parents (memoize #(get-in (k/get store % nil {:sync? true}) [:meta :datahike/parents]))
              heads (distinct (map #(d/commit-id @%) (vals conns)))
              wrong (for [a heads b heads
                          :let [expected (brute-bases parents a b)
                                r (try {:base (d/merge-base main a b)} (catch Exception e (ex-data e)))]
                          :when (not (if (= 1 (count expected))
                                       (= {:base (first expected)} r)
                                       (and (= :merge-base-criss-cross (:type r)) (= expected (:merge-bases r)))))]
                      [a b expected r])]
          (try
            (is (empty? wrong) (str "seed " seed))
            (finally (doseq [[b c] conns :when (not= b :db)] (d/release c)))))))))

(deftest the-backfill-writes-every-missing-record-once
  (with-store (cfg)
    (fn [main]
      (tx! main 0)
      (v/branch! main :db :chain)
      (on-branch main :chain
        (fn [chain]
          (let [store (:store @main)]
            (dotimes [i 3] (tx! main i) (tx! chain i))
            (forward-merge! chain main 5)
            (tx! main 6) (tx! chain 7)
            (v/branch! main (d/commit-id @chain) :relaunch)
            (let [heads [(d/commit-id @chain) (d/commit-id @main)]
                  before (apply d/merge-base main heads)
                  records (into {} (map (fn [gk] [gk (k/get store gk nil {:sync? true})])) (graph-keys store))]
              ;; the store as it was before graph records existed
              (doseq [gk (keys records)] (k/dissoc store gk {:sync? true}))
              (testing "a legacy commit's record is derived on read and nothing is written"
                (is (= (get records (w/commit-graph-key (first heads))) (d/commit-graph-record main (first heads))))
                (is (empty? (graph-keys store))))
              (testing "one run writes every reachable commit's record, equal to what the writer wrote"
                (is (= {:datahike/branches 3 :datahike/written (count records)}
                       (d/backfill-commit-graph! main {:batch-size 2})))
                (is (= records (into {} (map (fn [gk] [gk (k/get store gk nil {:sync? true})])) (graph-keys store)))))
              (testing "merge-base answers are unchanged, also through a record reader shared across calls"
                (is (= before (apply d/merge-base main heads)))
                (let [shared (memoize #(d/commit-graph-record main %))]
                  (is (= before (d/merge-base main (first heads) (second heads) shared)))
                  (is (= before (d/merge-base main :relaunch :db shared))))
                (is (= before (d/merge-base main :relaunch :db))))
              (testing "a second run writes nothing"
                (is (= {:datahike/branches 3 :datahike/written 0} (d/backfill-commit-graph! (:store @main))))))))))))

(deftest the-backfill-writes-parents-first
  (with-store (cfg)
    (fn [main]
      (dotimes [i 4] (tx! main i))
      (let [store (:store @main)
            gks (graph-keys store)
            records (into {} (map (fn [gk] [gk (k/get store gk nil {:sync? true})])) gks)]
        (doseq [gk gks] (k/dissoc store gk {:sync? true}))
        (testing "a torn run that wrote only the oldest records resumes to the full set"
          (d/backfill-commit-graph! main {:batch-size 2})
          (let [by-gen (sort-by (comp :datahike/generation val) records)]
            (doseq [[gk _] (drop 2 by-gen)] (k/dissoc store gk {:sync? true}))
            (is (= (- (count records) 2) (:datahike/written (d/backfill-commit-graph! main))))
            (is (= records (into {} (map (fn [gk] [gk (k/get store gk nil {:sync? true})])) (graph-keys store))))))))))
