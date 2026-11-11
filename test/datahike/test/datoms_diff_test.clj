(ns datahike.test.datoms-diff-test
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [datahike.api :as d]
            [datahike.datom :as dd]
            [datahike.index.persistent-set :as psi]
            [datahike.versioning :as v]
            ;; Jolt's :file backend (konserve.jolt.filestore): without it :file is konserve's JVM
            ;; store, whose Fressian a Jolt process cannot read, and every restored history threw
            [konserve.jolt.store]))

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

(deftest a-diff-of-consecutive-commits-is-exact-where-datoms-move-between-leaves
  ;; The unshared leaves' merge is the answer, with no lookup in either whole set: these
  ;; commits retract a datom and assert it again in a later transaction, retract a run of
  ;; entities (leaves merge with their neighbours), grow one entity (a leaf splits, its datoms
  ;; move to a new neighbour) and replace values, with history kept and not.
  (doseq [history? [true false]]
    (let [cfg (assoc (shaped-config (file-store) 8 0) :keep-history? history?)
          _ (d/create-database cfg)
          conn (d/connect cfg)
          tx! (fn [data] (:db-after (d/transact conn data)))
          db0 (tx! (vec (for [i (range 400)] {:k/n (long i) :k/tags [(str "t" (mod i 5))]})))
          es (mapv :e (d/datoms db0 {:index :aevt :components [:k/n]}))
          e (nth es 200)
          tag (first (map :v (d/datoms db0 {:index :eavt :components [e :k/tags]})))
          dbs [db0
               (tx! [[:db/retract e :k/tags tag]])
               (tx! [[:db/add e :k/tags tag]])
               (tx! (vec (for [x (subvec es 100 160)] [:db/retractEntity x])))
               (tx! (vec (for [i (range 60)] [:db/add (nth es 300) :k/tags (str "many" i)])))
               (tx! (vec (for [[i x] (map-indexed vector (take-nth 37 es))] [:db/add x :k/n (- -1 i)])))
               (tx! [{:k/n 5000 :k/tags ["new"]}])]]
      (testing (str "history " history? ": the datom asserted again is the same e, a, v in another transaction")
        (let [[gone added] (d/datoms-diff (nth dbs 1) (nth dbs 2) :eavt [e :k/tags])]
          (is (= [] gone))
          (is (= [[e :k/tags tag]] (mapv (juxt :e :a :v) added))))
        (let [[gone added] (d/datoms-diff (nth dbs 0) (nth dbs 2) :eavt [e :k/tags])]
          (is (= [[e :k/tags tag]] (mapv (juxt :e :a :v) gone)))
          (is (= [[e :k/tags tag]] (mapv (juxt :e :a :v) added)))
          (is (not= (:tx (first gone)) (:tx (first added))))))
      (testing (str "history " history? ": consecutive and further commits, both ways, every index")
        (doseq [[a b] (concat (partition 2 1 dbs) [[(first dbs) (peek dbs)] [(nth dbs 2) (nth dbs 4)]])
                [x y] [[a b] [b a]]]
          (doseq [index [:eavt :aevt :avet]]
            (is (exact? x y index)))
          (doseq [attribute [:k/n :k/tags]]
            (is (= (plain x y attribute) (answered x y attribute))))))
      (d/release conn))))

;; ---------------------------------------------------------------------------
;; What a diff costs: the work follows the children of the branches that differ.

(defn- node-widths
  "Address -> entry count (children or datoms) of every node of the stored set `s`, its root
  under the set's own address."
  [s]
  (let [storage (.-_storage s)]
    (loop [level [[(.-_address s) (.root s)]] acc {}]
      (let [acc (into acc (map (fn [[address node]] [address (.len node)])) level)]
        (if (or (empty? level) (zero? (.level (second (first level)))))
          acc
          (recur (vec (for [[_ node] level i (range (.len node))]
                        [(.address node (int i)) (.child node storage (int i))]))
                 acc))))))

(defn- entries-compared
  "The entries of the nodes one side holds and the other does not: the children and datoms
  an address-skipping diff of the two sets has to compare."
  [a b]
  (let [wa (node-widths a) wb (node-widths b)
        only (fn [x y] (reduce + 0 (keep (fn [[address n]] (when-not (contains? y address) n)) x)))]
    (+ (only wa wb) (only wb wa))))

(defn- ticks
  "The engine ticks `f` runs on Jolt: the work it does, never the time it takes."
  [f]
  (let [n (volatile! 0)]
    (jolt.host/ticks-of (fn [] (let [t0 (jolt.host/ticks)] (f) (vreset! n (- (jolt.host/ticks) t0)))))
    @n))

(deftest a-diff-costs-the-same-per-entry-it-compares-at-any-size
  ;; 20, 200 and 2,000 datoms replaced on entities spread over a stored tree of 40,000:
  ;; the work per entry of the nodes that differ stays flat. Which addresses both sides
  ;; hold was decided by java.util.HashSet.retainAll, which Jolt answers by scanning its
  ;; argument for each element: quadratic in the children compared (581, 1,890 and 2,066
  ;; ticks an entry; 271, 251 and 206 with Clojure sets).
  (let [cfg (shaped-config (file-store) 32 0)
        _ (d/create-database cfg)
        conn (d/connect cfg)
        n 20000
        rnd (java.util.Random. 7)]
    (doseq [part (partition-all 2000 (range n))]
      (d/transact conn (vec (for [i part] {:k/n (long i) :k/tags [(str "t" i)]}))))
    (let [es (mapv :e (d/datoms @conn {:index :aevt :components [:k/n]}))
          per-entry (vec (for [changes [10 100 1000]]
                           (let [a @conn
                                 b (:db-after (d/transact conn (vec (for [_ (range changes)]
                                                                      [:db/add (nth es (.nextInt rnd (int n))) :k/n (.nextLong rnd)]))))
                                 f #(d/datoms-diff a b :eavt [])]
                             (f)
                             (/ (double (ticks f)) (entries-compared (:eavt a) (:eavt b))))))]
      (is (< (peek per-entry) (* 2 (first per-entry))) (str "ticks per entry compared at 20, 200, 2,000 datoms: " per-entry))
      (is (< (second per-entry) (* 2 (first per-entry))) (str "ticks per entry compared at 20, 200, 2,000 datoms: " per-entry)))
    (d/release conn)))

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

(deftest an-attribute-only-one-side-declares-diffs-as-that-sides-datoms
  ;; seon's merge diffs every attribute either branch declares; a base cut before an attribute existed threw 'not defined in current schema'
  (let [cfg (config) _ (d/create-database cfg) main (d/connect cfg)]
    (d/transact main [{:k/n 1}])
    (d/branch! main :db :chain)
    (let [chain (d/connect (assoc cfg :branch :chain))]
      (d/transact chain [{:db/ident :k/new :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
      (d/transact chain [{:k/n 2 :k/new 20} {:k/new 30}])
      (let [added (mapv (juxt :e :a :v) (d/datoms @chain {:index :aevt :components [:k/new]}))]
        (is (= 2 (count added)))
        (is (= [[] added] (mapv #(mapv (juxt :e :a :v) %) (d/datoms-diff @main @chain :aevt [:k/new]))))
        (is (= [added []] (mapv #(mapv (juxt :e :a :v) %) (d/datoms-diff @chain @main :aevt [:k/new]))))))))

(deftest an-attribute-a-db-predates-has-no-datoms-there
  ;; a past db value asked for an attribute declared after it (seon's agent reading its merge base,
  ;; cut before :form/ops): it holds none of its datoms, as Datascript answers, where it threw 'not
  ;; defined in current schema'; a transaction naming it is still refused
  (let [cfg (config) _ (d/create-database cfg) main (d/connect cfg)]
    (d/transact main [{:k/n 1}])
    (let [before @main]
      (d/transact main [{:db/ident :k/new :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/index true}])
      (d/transact main [{:k/n 2 :k/new 20}])
      (doseq [[index cs] [[:aevt [:k/new]] [:eavt [1 :k/new]] [:avet [:k/new 20]]]]
        (is (empty? (d/datoms before {:index index :components cs})) (str index " on the db before the attribute"))
        (is (= (case index :eavt 0 1) (count (d/datoms @main {:index index :components cs}))) (str index " on the db after it")))
      (is (thrown-with-msg? Throwable #"not defined in current schema" (d/db-with before [{:k/new 1}]))
          "a write naming it is still refused"))))

(deftest a-diff-of-the-whole-index-tests-no-datom-against-its-bounds
  ;; Bounds of every entity and no attribute or value hold every datom, so the walk asks
  ;; nothing of them; bounds that do cut the index are still asked, and still answer exactly.
  (let [cfg (shaped-config (file-store) 8 0)
        _ (d/create-database cfg)
        conn (d/connect cfg)
        _ (d/transact conn (vec (for [i (range 300)] {:k/n (long i) :k/tags [(str "t" i)]})))
        a @conn
        b (:db-after (d/transact conn [{:k/n 1000 :k/tags ["new"]} [:db/add (:e (first (d/datoms a :eavt))) :k/tags "x"]]))
        asked (atom 0)
        counted (fn [f]
                  (reset! asked 0)
                  (with-redefs [psi/slice-comparator-constructor
                                (let [make psi/slice-comparator-constructor]
                                  (fn [& bounds]
                                    (let [in-range (apply make bounds)]
                                      (fn [x y] (swap! asked inc) (in-range x y)))))]
                    (f))
                  @asked)]
    (doseq [index [:eavt :aevt :avet]]
      (is (zero? (counted #(d/datoms-diff a b index []))) (str index " over the whole index"))
      (is (exact? a b index)))
    (is (pos? (counted #(d/datoms-diff a b :aevt [:k/tags]))) "an attribute's range is asked")
    (is (= (plain a b :k/tags) (answered a b :k/tags)))
    (d/release conn)))

;; ---------------------------------------------------------------------------
;; A temporal or filtered value holds no index of its own: its datoms are merged, not tree-diffed.

(defn- shown
  "The whole datoms `db` shows for the index and components, in index order, as tuples."
  [db index components]
  (mapv (juxt :e :a :v :tx) (d/datoms db {:index index :components components})))

(defn- view-diff-is-exact?
  "datoms-diff answers each side's datoms the other does not show, in the order the side shows them."
  [a b index components]
  (let [ta (shown a index components) tb (shown b index components)
        sa (set ta) sb (set tb)
        [only-a only-b] (d/datoms-diff a b index components)]
    (and (= (filterv (complement sb) ta) (mapv (juxt :e :a :v :tx) only-a))
         (= (filterv (complement sa) tb) (mapv (juxt :e :a :v :tx) only-b)))))

(defn- history-of-changes
  "A history-keeping db whose five transactions add, update, retract a value and retract an entity.
  Returns the connection and the :max-tx after each transaction."
  []
  (let [cfg (config) _ (d/create-database cfg) conn (d/connect cfg)
        tx! (fn [data] (:max-tx (:db-after (d/transact conn data))))
        t1 (tx! (vec (for [i (range 1 7)] {:db/id (- i) :k/n (long i) :k/tags [(str "t" i) "shared"]})))
        e (:e (first (d/datoms @conn {:index :aevt :components [:k/n]})))
        t2 (tx! [[:db/add e :k/n 100]])
        t3 (tx! [[:db/retract e :k/tags "shared"]])
        t4 (tx! [[:db/retractEntity (:e (second (d/datoms @conn {:index :aevt :components [:k/n]})))]])
        t5 (tx! [{:k/n 7 :k/tags ["late"]}])]
    {:conn conn :txs [t1 t2 t3 t4 t5] :e e}))

(deftest the-diff-of-a-temporal-value-is-the-difference-of-the-datoms-it-shows
  (let [{:keys [conn txs e]} (history-of-changes) db @conn [t1 t2 t3 t4 _] txs
        a (d/as-of db t2)]
    (testing "as-of against the current value: nothing thrown, the exact difference"
      (let [[only-a only-b] (d/datoms-diff a db :eavt [])]
        (is (seq only-a) "the update and the retractions took datoms away")
        (is (seq only-b) "the later transactions added datoms")
        (is (view-diff-is-exact? a db :eavt []))
        (is (view-diff-is-exact? db a :eavt []))
        (is (= (set (shown db :eavt [])) (set (shown (d/as-of db (:max-tx db)) :eavt [])))
            "the sets agree when the view is the whole present")
        (is (= [[] []] (d/datoms-diff db (d/as-of db (:max-tx db)) :eavt [])) "an as-of the present differs in nothing")))
    (testing "with components: one entity, one attribute on :aevt"
      (is (view-diff-is-exact? a db :eavt [e]))
      (is (view-diff-is-exact? db a :eavt [e]))
      (is (view-diff-is-exact? a db :aevt [:k/n]))
      (is (view-diff-is-exact? a db :aevt [:k/tags]))
      (is (seq (first (d/datoms-diff (d/as-of db t1) (d/as-of db t2) :eavt [e])))
          "the updated value is only in the earlier value"))
    (testing "as-of against as-of, two times"
      (doseq [[x y] [[t1 t2] [t2 t3] [t1 t4] [t4 t1]] index [:eavt :aevt :avet]]
        (is (view-diff-is-exact? (d/as-of db x) (d/as-of db y) index []))))
    (testing "since and history values"
      (doseq [index [:eavt :aevt]]
        (is (view-diff-is-exact? (d/since db t2) db index []))
        (is (view-diff-is-exact? (d/since db t2) (d/since db t3) index []))
        (is (view-diff-is-exact? (d/history db) db index []))
        (is (view-diff-is-exact? (d/history db) (d/as-of db t2) index []))
        (is (view-diff-is-exact? (d/history db) (d/since db t3) index [:k/tags]))))
    (testing "a filtered value"
      (let [f (d/filter db (fn [_ datom] (not= (:a datom) :k/tags)))]
        (is (view-diff-is-exact? f db :aevt [:k/tags]))
        (is (view-diff-is-exact? f db :eavt []))))
    (d/release conn)))

(deftest a-diff-of-a-temporal-value-records-its-range-as-a-read
  (let [{:keys [conn txs]} (history-of-changes) db @conn
        [_ reads] (d/with-reads #(d/datoms-diff (d/as-of db (second txs)) db :aevt [:k/tags]))
        idx (d/reads-index reads)]
    (is (contains? idx [:a :k/tags]) "the diffed attribute is a read")
    (d/release conn)))
