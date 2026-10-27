(ns datahike.test.datoms-diff-test
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [datahike.api :as d]
            [datahike.datom :as dd]
            [datahike.versioning :as v]))

(defn- config []
  {:store {:backend :memory :id (random-uuid)} :keep-history? true :schema-flexibility :write
   :index-config {:branching-factor 32 :diff-buf-size 8}
   :initial-tx [{:db/ident :k/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                {:db/ident :k/tags :db/valueType :db.type/string :db/cardinality :db.cardinality/many}]})

(defn- tuples [db attribute]
  (into #{} (map (juxt :e :a :v :tx)) (d/datoms db {:index :aevt :components [attribute]})))

(defn- plain [a b attribute]
  (let [ta (tuples a attribute) tb (tuples b attribute)]
    [(set/difference ta tb) (set/difference tb ta)]))

(defn- answered [a b attribute]
  (mapv #(into #{} (map (juxt :e :a :v :tx)) %) (d/datoms-diff a b :aevt [attribute])))

(deftest the-diff-of-two-commits-is-the-set-difference-of-their-datoms
  ;; A deep tree, then small transactions on two branches, so buffered slot edits and shared subtrees both occur.
  (doseq [seed (range 4)]
    (let [rnd (java.util.Random. seed) cfg (config) _ (d/create-database cfg) main (d/connect cfg)]
      (d/transact main (vec (for [i (range 3000)] {:k/n (long i) :k/tags [(str "t" (mod i 7)) (str "u" (mod i 11))]})))
      (d/branch! main :db :chain)
      (let [chain (d/connect (assoc cfg :branch :chain)) commits (atom [(d/commit-id @main)])]
        (dotimes [i 40]
          (let [conn (if (zero? (.nextInt rnd 2)) main chain)
                some-e (:e (nth (vec (d/datoms @conn {:index :aevt :components [:k/n]})) (.nextInt rnd 2000)))]
            (d/transact conn (cond-> [{:k/n (long (+ 10000 (.nextInt rnd 100000))) :k/tags [(str "n" i)]}]
                               (zero? (.nextInt rnd 3)) (conj [:db/retractEntity some-e])
                               (zero? (.nextInt rnd 2)) (conj [:db/add some-e :k/tags (str "x" i)])))
            (swap! commits conj (d/commit-id @conn))))
        (let [dbs (mapv #(v/commit-as-db main %) (take 10 (shuffle @commits)))]
          (doseq [a dbs b dbs attribute [:k/n :k/tags]]
            (is (= (plain a b attribute) (answered a b attribute)))))
        (testing "a value against itself differs in nothing"
          (is (= [[] []] (d/datoms-diff @chain @chain :aevt [:k/tags]))))
        (testing "the other attribute's changes are outside the range"
          (d/transact chain [{:k/n 7777777}])
          (is (= [[] []] (d/datoms-diff @main @chain :aevt [:db/ident]))))))))

(deftest a-diff-of-one-changed-leaf-reads-that-leaf-not-the-range
  (let [cfg (config) _ (d/create-database cfg) main (d/connect cfg)]
    (d/transact main (vec (for [i (range 20000)] {:k/n (long i)})))
    (d/branch! main :db :chain)
    (let [chain (d/connect (assoc cfg :branch :chain))
          e (:e (first (d/datoms @chain {:index :aevt :components [:k/n]})))]
      (d/transact chain [[:db/add e :k/n -1]])
      (let [[only-a only-b] (d/datoms-diff @main @chain :aevt [:k/n])]
        (is (= [[e :k/n 0]] (mapv (juxt :e :a :v) only-a)))
        (is (= [[e :k/n -1]] (mapv (juxt :e :a :v) only-b)))))))

;; ---------------------------------------------------------------------------
;; Every node shape: branches restored without a buffer, unstored (`with`) nodes,
;; trees of different heights, whole-index ranges.

(defn- tuples-on [db index]
  (into #{} (map (juxt :e :a :v :tx)) (d/datoms db {:index index :components []})))

(defn- exact? [a b index]
  (let [ta (tuples-on a index) tb (tuples-on b index)]
    (= [(set/difference ta tb) (set/difference tb ta)]
       (mapv #(into #{} (map (juxt :e :a :v :tx)) %) (d/datoms-diff a b index [])))))

(defn- shaped-config [store branching buf]
  {:store store :keep-history? true :schema-flexibility :write
   :index-config {:branching-factor branching :diff-buf-size buf}
   :initial-tx [{:db/ident :k/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/index true}
                {:db/ident :k/tags :db/valueType :db.type/string :db/cardinality :db.cardinality/many}]})

(defn- file-store []
  {:backend :file :id (random-uuid) :config {:defer-immutable-sync? true}
   :path (str (java.nio.file.Files/createTempDirectory "datoms-diff" (make-array java.nio.file.attribute.FileAttribute 0)) "/store")})

(defn- churn!
  "Rows, then `n` small transactions (an insert, sometimes a retraction): buffered slot edits, flushes and splits."
  [conn rows n seed]
  (let [rnd (java.util.Random. seed)]
    (d/transact conn (vec (for [i (range rows)] {:k/n (long i) :k/tags [(str "t" (mod i 7))]})))
    (dotimes [i n]
      (let [es (vec (d/datoms @conn {:index :aevt :components [:k/n]}))]
        (d/transact conn (cond-> [{:k/n (long (+ 10000 (.nextInt rnd 100000))) :k/tags [(str "n" i)]}]
                           (and (seq es) (zero? (.nextInt rnd 3)))
                           (conj [:db/retractEntity (:e (nth es (.nextInt rnd (count es))))])))))))

(defn- lineage [conn n]
  (loop [cid (d/commit-id @conn) acc []]
    (if (or (nil? cid) (>= (count acc) n))
      acc
      (recur (first (v/parent-commit-ids (v/commit-as-db conn cid))) (conj acc cid)))))

(deftest a-diff-over-branches-restored-without-a-buffer-is-exact
  ;; A branch read back from storage with no buffered edits has no slots (`.slots` is nil); every
  ;; pair below expands such a branch, and threw a NullPointerException before.
  (let [cfg (shaped-config (file-store) 32 8) _ (d/create-database cfg) conn (d/connect cfg)]
    (churn! conn 3000 300 1)
    (let [cids (lineage conn 40)]
      (d/release conn)
      (let [conn (d/connect cfg) head @conn
            es (vec (distinct (map :e (d/datoms head :eavt))))
            rnd (java.util.Random. 3)]
        (testing "commit pairs of the restored store, whole index"
          (doseq [x (take 4 cids) y (take-last 4 cids) index [:eavt :aevt :avet]]
            (is (exact? (v/commit-as-db conn x) (v/commit-as-db conn y) index))))
        (testing "a `with` overlay (unstored nodes) against its restored parent"
          (doseq [_ (range 10)
                  :let [e (nth es (.nextInt rnd (count es)))
                        w (:db-after (d/with head [[:db/retractEntity e] {:k/n -1 :k/tags ["w"]}]))]
                  index [:eavt :aevt :avet]]
            (is (exact? head w index))
            (is (exact? w head index))))
        (testing "a value against a `with` overlay of itself that adds one entity"
          (let [w (:db-after (d/with head [{:k/n -2}]))
                [only-a only-b] (d/datoms-diff head w :eavt [])]
            (is (= [] only-a))
            (is (= [[:k/n -2]] (into [] (comp (map (juxt :a :v)) (remove #(= :db/txInstant (first %)))) only-b)))))
        (d/release conn)))))

(deftest a-diff-of-trees-of-different-heights-is-exact
  ;; The chain forks from a one-leaf tree; main grows three levels; both sides keep writing.
  (let [cfg (shaped-config {:backend :memory :id (random-uuid)} 8 4) _ (d/create-database cfg) main (d/connect cfg)]
    (d/transact main [{:k/n 1} {:k/n 2}])
    (let [fork @main]
      (d/branch! main :db :chain)
      (let [chain (d/connect (assoc cfg :branch :chain))]
        (churn! main 600 20 5)
        (d/transact chain [{:k/n 3 :k/tags ["c"]}])
        (doseq [[a b] [[fork @main] [@main fork] [@chain @main] [@main @chain] [fork @chain]]
                index [:eavt :aevt :avet]]
          (is (exact? a b index)))))))

(def ^:private history-gen
  ;; rows, then ops: [side kind n] with side 0 main / 1 chain / 2 a `with` overlay of the chain
  (gen/tuple (gen/choose 0 400)
             (gen/vector (gen/tuple (gen/choose 0 2) (gen/choose 0 2) (gen/choose 0 1000)) 0 25)
             gen/boolean))

(defn- replay [[rows ops restore?]]
  (let [cfg (shaped-config (if restore? (file-store) {:backend :memory :id (random-uuid)}) 8 4)
        _ (d/create-database cfg) main (d/connect cfg)]
    (d/transact main (vec (for [i (range rows)] {:k/n (long i) :k/tags [(str "t" (mod i 5))]})))
    (d/branch! main :db :chain)
    (let [chain (d/connect (assoc cfg :branch :chain))
          tx (fn [db kind n]
               (let [es (vec (d/datoms db {:index :aevt :components [:k/n]}))
                     e (when (seq es) (:e (nth es (mod n (count es)))))]
                 (case (long kind)
                   0 [{:k/n (long (+ 5000 n)) :k/tags [(str "n" n)]}]
                   1 (if e [[:db/retractEntity e]] [{:k/n (long n)}])
                   2 (if e [[:db/add e :k/tags (str "x" n)] [:db/add e :k/n (long (- n))]] [{:k/n (long n)}]))))
          values (reduce (fn [acc [side kind n]]
                           (case (long side)
                             0 (do (d/transact main (tx @main kind n)) (conj acc [:commit (d/commit-id @main)]))
                             1 (do (d/transact chain (tx @chain kind n)) (conj acc [:commit (d/commit-id @chain)]))
                             2 (conj acc [:with (d/commit-id @chain) kind n])))
                         [[:commit (d/commit-id @main)]] ops)
          conn (if restore? (do (d/release chain) (d/release main) (d/connect cfg)) main)
          ;; an overlay is built on the connection read from, so a restored store's overlays sit on restored nodes
          dbs (mapv (fn [[k cid kind n]]
                      (let [db (v/commit-as-db conn cid)]
                        (if (= k :commit) db (:db-after (d/with db (tx db kind n))))))
                    values)
          picks (vec (take 6 (distinct (concat [(first dbs) (peek dbs)] (take-nth 3 dbs)))))]
      (try
        (every? true? (for [a picks b picks index [:eavt :aevt :avet]] (exact? a b index)))
        (finally (d/release conn) (when-not restore? (d/release chain)))))))

(deftest datoms-diff-equals-the-plain-set-difference-over-generated-histories
  (let [{:keys [pass? fail seed]} (tc/quick-check 40 (prop/for-all [h history-gen] (replay h)) :seed 20261015 :max-size 30)]
    (is pass? (str "seed " seed " counterexample " (pr-str fail)))))

(deftest a-diff-records-the-range-it-read
  ;; A read built on a diff answers from both values over the diffed range, so with-reads
  ;; records that range, as it does a slice's: a datom added in it touches the read, a datom
  ;; of another attribute does not.
  (let [cfg (config) _ (d/create-database cfg) conn (d/connect cfg)]
    (d/transact conn (vec (for [i (range 200)] {:k/n (long i) :k/tags [(str "t" i)]})))
    (let [a @conn
          b (:db-after (d/transact conn [{:k/n 1000 :k/tags ["new"]}]))
          [diff reads] (d/with-reads #(d/datoms-diff a b :aevt [:k/tags]))
          idx (d/reads-index reads)
          added-tag (dd/datom 999999 :k/tags "later" 536870999)
          added-n (dd/datom 999999 :k/n 7 536870999)]
      (is (= 1 (count (second diff))) "the diff itself is unchanged: one tag added on b")
      (is (contains? idx [:a :k/tags]) "the diffed attribute is a read")
      (is (d/reads-touched? idx [added-tag]) "a tag added later touches what the diff read")
      (is (not (d/reads-touched? idx [added-n])) "a datom of another attribute does not"))))
