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

(defn- with-conn [label f & {:keys [writer backend] :or {writer {:backend :sync} backend :memory}}]
  (let [c {:store (cond-> {:backend backend :id (random-uuid)} (= :file backend) (assoc :path (str run-dir "-" label)))
           :keep-history? false :schema-flexibility :read :writer writer}]
    (d/delete-database c)
    (d/create-database c)
    (let [conn (d/connect c)]
      (try (v/forget-commit-points!) (f conn)
           (finally (d/release conn) (d/delete-database c))))))

(defn- tx! [conn n] (d/transact conn [{:name (str n)}]) [(d/commit-id @conn) (:max-tx @conn)])

(defn- refusal [f] (try (f) nil (catch Exception e (select-keys (ex-data e) [:type :branch :tx]))))

(defn- reads
  "[answer n], n the commit records f read from the store."
  [f]
  (let [n (atom 0) get k/get]
    [(with-redefs [k/get (fn [& args] (when (uuid? (second args)) (swap! n inc)) (apply get args))] (f)) @n]))

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
          @(d/gc-storage conn cutoff)
          (v/forget-commit-points!)
          (is (= {:type :commit-expired :branch :db :tx old-tx} (refusal #(v/commit-at-tx @conn old-tx)))))))
    :writer {:backend :self} :backend :file))

(deftest a-lookup-reads-only-the-commits-it-has-not-met
  (with-conn "reads"
    (fn [conn]
      (let [points (mapv #(tx! conn %) (range 200))
            [c150 t150] (nth points 150)]
        (v/forget-commit-points!)
        (let [db @conn]
          (is (= [{:branch :db :commit c150 :t t150} 50] (reads #(v/commit-at-tx db t150)))
              "a first lookup reads the commits from the head back to the answer, not the branch")
          (is (= [{:branch :db :commit c150 :t t150} 0] (reads #(v/commit-at-tx db t150))) "again: none")
          (is (= 0 (second (reads #(v/commit-at-tx db (second (nth points 170))))))
              "nor at any commit already met"))
        (dotimes [n 5] (tx! conn (str "new" n)))
        (let [db @conn]
          (is (= [{:branch :db :commit c150 :t t150} 5] (reads #(v/commit-at-tx db t150)))
              "after five commits, the five"))))))
