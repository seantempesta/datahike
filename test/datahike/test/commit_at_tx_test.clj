(ns datahike.test.commit-at-tx-test
  "`datahike.versioning/commit-at-tx`: the point {:branch :commit :t} of a
   branch at a transaction, the commit of its lineage whose last transaction is
   the greatest at or before it, derived from the commit records (nothing
   stored) and held in the process's memory, so each commit is read once.
   `commit-tx-range`: the transactions a commit holds, over its base."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.versioning :as v]
            [datahike.writer :as writer]
            [konserve.core :as k]))

(def ^:private run-dir (str (System/getProperty "java.io.tmpdir") "/dh-commit-at-tx-" (random-uuid)))

(defn- with-conn [label f & {:keys [writer backend history?] :or {writer {:backend :sync} backend :memory history? true}}]
  (let [c {:store (cond-> {:backend backend :id (random-uuid)} (= :file backend) (assoc :path (str run-dir "-" label)))
           :keep-history? history? :schema-flexibility :read :writer writer}]
    (d/delete-database c)
    (d/create-database c)
    (let [conn (d/connect c)]
      (try (v/forget-commit-points!) (f conn)
           (finally (d/release conn) (d/delete-database c))))))

(defn- tx! [conn n] (d/transact conn [{:name (str n)}]) [(d/commit-id @conn) (:max-tx @conn)])

(defn- refusal [f] (try (f) nil (catch Exception e (select-keys (ex-data e) [:type :branch :tx]))))

(defn- reads
  "[answer n], n the reads of commit keys f made of the store (gets and exists?)."
  [f]
  (let [n (atom 0) get k/get exists? k/exists?]
    [(with-redefs [k/get (fn [& args] (when (uuid? (second args)) (swap! n inc)) (apply get args))
                   k/exists? (fn [& args] (when (uuid? (second args)) (swap! n inc)) (apply exists? args))]
       (f))
     @n]))

(deftest a-commit-holds-the-transactions-after-its-base
  (with-conn "range"
    (fn [conn]
      (let [root (d/commit-id @conn)
            root-tx (:max-tx @conn)
            [c1 t1] (tx! conn 1)]
        (is (= [(inc root-tx) root-tx] (v/commit-tx-range conn root)) "the root holds none")
        (is (= [t1 t1] (v/commit-tx-range conn c1)) "a one-transaction commit, its own")
        (writer/transact-batch! conn (mapv (fn [n] {:tx-data [{:name (str "b" n)}]}) (range 3)))
        (is (= [(inc t1) (+ t1 3)] (v/commit-tx-range conn (d/commit-id @conn))) "a batch, its three")
        (dotimes [n 4] (writer/apply! conn {:tx-data [{:name (str "r" n)}]}))
        (let [{:keys [commit-id]} (writer/commit-pending! conn)]
          (is (= [(+ t1 4) (+ t1 7)] (v/commit-tx-range conn commit-id)) "a run-ahead commit of four applies"))
        (d/branch! conn :db :b)
        (let [b (d/connect (assoc (:config @conn) :branch :b))]
          (try
            (let [fork-tx (:max-tx @conn)
                  [b1] (tx! b "b1")
                  [mh main-tx] (tx! conn "m")
                  [b2] (tx! b "b2")]
              (is (= (inc fork-tx) (first (v/commit-tx-range conn b1)))
                  "a branch's first commit holds what it added over the commit the branch was made from")
              (is (< (second (v/commit-tx-range conn b1)) main-tx) "ids are the store's: main's next one is past b's")
              (v/merge! conn #{:b} [{:name "merged"}])
              (is (= #{mh b2} (d/parent-commit-ids @conn)) "the merge's parents are main's head and b's")
              (is (= [(inc main-tx) (:max-tx @conn)] (v/commit-tx-range conn (d/commit-id @conn)))
                  "a merge holds its own transaction over the head it merged into, never the merged branch's"))
            (finally (d/release b))))))))

(deftest the-point-at-a-transaction-is-at-or-before-it
  (with-conn "point"
    (fn [conn]
      (let [root (d/commit-id @conn)
            root-tx (:max-tx @conn)
            [c1 t1] (tx! conn 1)
            _ (writer/transact-batch! conn (mapv (fn [n] {:tx-data [{:name (str "b" n)}]}) (range 3)))
            batch (d/commit-id @conn)
            [c3 t3] (tx! conn 3)]
        (is (= {:branch :db :commit c1 :t t1} (v/commit-at-tx @conn t1)) "exact")
        (is (= {:branch :db :commit c1 :t t1} (v/commit-at-tx conn :db (inc t1)))
            "inside a batch: the commit before it, never the batch, which holds later transactions too")
        (is (= {:branch :db :commit batch :t (+ t1 3)} (v/commit-holding-tx @conn (inc t1))) "the commit holding it is the batch")
        (is (= {:branch :db :commit root :t root-tx} (v/commit-at-tx @conn root-tx)))
        (is (= {:type :tx-before-first-commit :branch :db :tx (dec root-tx)} (refusal #(v/commit-at-tx @conn (dec root-tx)))))
        (is (= {:type :tx-after-head :branch :db :tx (inc t3)} (refusal #(v/commit-at-tx @conn (inc t3)))))
        (is (= {:branch :db :commit c3 :t t3} (v/commit-at-tx @conn t3)) "the head")))))

(deftest a-branch-reads-the-lineage-it-was-made-from
  (with-conn "lineage"
    (fn [conn]
      (let [[c1 t1] (tx! conn 1)
            [c2 t2] (tx! conn 2)]
        (d/branch! conn :db :b)
        (let [b (d/connect (assoc (:config @conn) :branch :b))]
          (try
            (let [[m3 t3] (tx! conn 3)
                  [b3 bt3] (tx! b "b3")]
              (is (< t2 t3 bt3) "ids are the store's: b's transaction after main's")
              (is (= {:branch :b :commit c2 :t t2} (v/commit-at-tx @b t3)) "main's transaction is not b's: b stood at c2")
              (is (= {:branch :b :commit b3 :t bt3} (v/commit-at-tx @b bt3)))
              (is (= {:branch :db :commit m3 :t t3} (v/commit-at-tx @conn t3)))
              (is (= {:type :tx-after-head :branch :db :tx bt3} (refusal #(v/commit-at-tx @conn bt3))) "b's is not main's")
              (is (= {:branch :b :commit c1 :t t1} (v/commit-at-tx conn :b t1)) "main's commits below the fork")
              (d/branch! conn c2 :from-id)
              (is (= {:branch :from-id :commit c2 :t t2} (v/commit-at-tx conn :from-id t2))
                  "a branch made from a commit id stands at it")
              (is (= {:type :tx-after-head :branch :from-id :tx t3} (refusal #(v/commit-at-tx conn :from-id t3))))
              (is (= {:branch :from-id :commit c1 :t t1} (v/commit-at-tx conn :from-id t1)))
              (testing "a branch whose first commit merges main's newer head (a forward merge) stands on the commit it was made from"
                (d/branch! conn c2 :fwd)
                (let [fwd (d/connect (assoc (:config @conn) :branch :fwd))]
                  (try
                    (v/merge! fwd #{m3} [{:name "fwd"}])
                    (is (= #{c2 m3} (d/parent-commit-ids @fwd)))
                    (is (= [(inc t2) (:max-tx @fwd)] (v/commit-tx-range fwd (d/commit-id @fwd))))
                    (is (= {:branch :fwd :commit c2 :t t2} (v/commit-at-tx @fwd t3)) "main's transaction is not the merge's")
                    (finally (d/release fwd))))))
            (finally (d/release b))))))))

(deftest a-deleted-and-re-made-branch-answers-its-own-lineage
  (with-conn "remade"
    (fn [conn]
      (tx! conn 1)
      (d/branch! conn :db :b)
      (let [b (d/connect (assoc (:config @conn) :branch :b))
            [x1 t1] (tx! b "x1")
            [x2 t2] (tx! b "x2")]
        (d/branch! conn :b :keep)
        (d/release b)
        (d/delete-branch! conn :b)
        (d/branch! conn x1 :b)
        (let [b (d/connect (assoc (:config @conn) :branch :b))
              [y ty] (tx! b "y")]
          (is (< t2 ty))
          (is (= {:branch :b :commit x1 :t t1} (v/commit-at-tx @b t2)) "x2 is not on b's lineage now")
          (is (= {:branch :b :commit y :t ty} (v/commit-at-tx @b ty)))
          (is (= {:branch :keep :commit x2 :t t2} (v/commit-at-tx conn :keep t2)) "the kept branch still answers x2")
          (is (= {:branch :b :commit x1 :t t1} (v/commit-at-tx @b t1)))
          (d/release b))))))

(deftest a-point-collection-removed-is-expired
  (with-conn "gc"
    (fn [conn]
      (let [[_ old-tx] (tx! conn 1)]
        (tx! conn "boundary")
        (Thread/sleep 5)
        (let [cutoff (java.util.Date.)]
          (Thread/sleep 5)
          (tx! conn 2)
          (tx! conn 3)
          (is (= old-tx (:t (v/commit-at-tx @conn old-tx))) "known before the collection")
          @(d/gc-storage conn cutoff)
          (is (= {:type :commit-expired :branch :db :tx old-tx} (refusal #(v/commit-at-tx @conn old-tx)))
              "a commit this process collected, though its walk is in memory"))))
    :writer {:backend :self} :backend :file))

(deftest a-point-another-process-removed-is-expired
  (with-conn "removed"
    (fn [conn]
      (let [[c1 t1] (tx! conn 1)
            _ (tx! conn 2)
            db @conn]
        (is (= {:branch :db :commit c1 :t t1} (v/commit-at-tx db t1)))
        ;; what another process's collection leaves: the key gone behind this process's memory
        (k/dissoc (:store db) c1 {:sync? true})
        (is (= {:type :commit-expired :branch :db :tx t1} (refusal #(v/commit-at-tx db t1))))
        (is (= {:type :commit-expired :branch :db :tx t1} (refusal #(v/commit-at-tx db t1))) "and again")))))

(deftest a-walk-that-meets-a-removed-commit-is-expired
  (with-conn "gap"
    (fn [conn]
      (let [[_ t1] (tx! conn 1)
            [c2] (tx! conn 2)
            _ (tx! conn 3)
            db @conn]
        (k/dissoc (:store db) c2 {:sync? true})
        (v/forget-commit-points!)
        (is (= {:type :commit-expired :branch :db :tx t1} (refusal #(v/commit-at-tx db t1)))
            "a parent record gone on the way is expired, never the start of the lineage")))))

(deftest a-merges-base-is-the-head-its-db-was-built-on
  (with-conn "merge-base"
    (fn [conn]
      (let [[c1 t1] (tx! conn 1)
            [h th] (tx! conn 2)]
        (testing "a branch standing at a newer commit that merges an older one: its base is the newer"
          (d/branch! conn :db :p)
          (let [p (d/connect (assoc (:config @conn) :branch :p))]
            (try
              (v/merge! p #{c1} [{:name "p"}])
              (is (= #{c1 h} (d/parent-commit-ids @p)))
              (is (= [(inc th) (:max-tx @p)] (v/commit-tx-range p (d/commit-id @p))))
              (is (= {:branch :p :commit h :t th} (v/commit-at-tx @p th)))
              (finally (d/release p)))))
        (testing "a re-made branch that merges a commit of the branch it was before: its base is the commit it was made from"
          (d/branch! conn :db :b)
          (let [b (d/connect (assoc (:config @conn) :branch :b))
                [x1 tx1] (tx! b "x1")
                [x2 tx2] (tx! b "x2")]
            (d/release b)
            (let [[m3 t3] (tx! conn 3)]
              (d/delete-branch! conn :b)
              (d/branch! conn :db :b)
              (let [b (d/connect (assoc (:config @conn) :branch :b))]
                (try
                  (v/merge! b #{x2} [{:name "back"}])
                  (is (= #{m3 x2} (d/parent-commit-ids @b)))
                  (is (= [(inc t3) (:max-tx @b)] (v/commit-tx-range b (d/commit-id @b))))
                  (is (= {:branch :b :commit h :t th} (v/commit-at-tx @b tx2)) "x2 is not on b's lineage")
                  (finally (d/release b)))))))))))

(deftest a-merge-whose-merged-parent-is-gone-keeps-its-base
  (with-conn "merged-parent-gone"
    (fn [conn]
      (let [[c1 t1] (tx! conn 1)]
        (d/branch! conn :db :b)
        (let [b (d/connect (assoc (:config @conn) :branch :b))
              [x1 _] (tx! b "x1")]
          (d/release b)
          (let [[h th] (tx! conn 2)]
            (v/merge! conn #{x1} [{:name "merged"}])
            (tx! conn 3)
            (d/delete-branch! conn :b)
            ;; what a collection leaves once the merged branch is past its cut-off: the
            ;; merge and its base stay, the merged-in parent is gone
            (k/dissoc (:store @conn) x1 {:sync? true})
            (v/forget-commit-points!)
            (is (= {:branch :db :commit h :t th} (v/commit-at-tx @conn th))
                "the walk passes the merge by its base")
            (is (= {:branch :db :commit c1 :t t1} (v/commit-at-tx @conn t1)))))))))

(deftest a-lookup-reads-only-the-commits-it-has-not-met
  (with-conn "reads"
    (fn [conn]
      (let [points (mapv #(tx! conn %) (range 200))
            [c150 t150] (nth points 150)]
        (v/forget-commit-points!)
        (let [db @conn]
          (is (= [{:branch :db :commit c150 :t t150} 51] (reads #(v/commit-at-tx db t150)))
              "a first lookup reads the commits from the head back to the answer, not the branch, and asks the answer exists")
          (is (= [{:branch :db :commit c150 :t t150} 1] (reads #(v/commit-at-tx db t150))) "again: only that it exists")
          (is (= 1 (second (reads #(v/commit-at-tx db (second (nth points 170))))))
              "and at any commit already met"))
        (dotimes [n 5] (tx! conn (str "new" n)))
        (let [db @conn]
          (is (= [{:branch :db :commit c150 :t t150} 6] (reads #(v/commit-at-tx db t150)))
              "after five commits, the five and the answer's"))))))

(deftest without-history-a-merges-base-is-ambiguous
  ;; a db keeps no transaction entity without history, so a merge's db cannot say which parent it was built on
  (with-conn "no-history"
    (fn [conn]
      (let [[c1 t1] (tx! conn 1)]
        (d/branch! conn :db :b)
        (let [b (d/connect (assoc (:config @conn) :branch :b))]
          (try
            (tx! b "b1")
            (v/merge! conn #{:b} [{:name "m"}])
            (is (= :commit-base-ambiguous (:type (refusal #(v/commit-at-tx @conn t1)))))
            (finally (d/release b))))))
    :history? false))
