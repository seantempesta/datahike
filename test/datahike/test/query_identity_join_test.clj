(ns datahike.test.query-identity-join-test
  "A unique lookup bound by an input drives the joins hanging off it.

   Shape: `[?run :turn/id ?id]` binds ?run from an input, then two entity groups
   join through the ref `[?x :eval/run ?run]`. Over a store whose groups estimate
   only a few output rows (pass rates multiplied over many unrelated datoms), the
   planner used to start with a group and scan the whole ref attribute; and every
   execution re-counted attributes while re-planning. Both made one lookup cost
   in proportion to the store."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.lru :as lru]
   [datahike.query :as q]
   [datahike.query.analyze :as analyze]
   [datahike.query.estimate :as estimate]))

(def ^:private runs 2000)

(defn- store-config [path]
  {:store {:backend :file :path path :id (random-uuid)}
   :schema-flexibility :write
   :keep-history? false
   ;; A small cache makes storage-read regressions observable.
   :store-cache-size 1})

(def ^:private schema
  [{:db/ident :turn/id :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :eval/run :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :eval/ordinal :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :eval/source :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :eval/shown :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :junk/a :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :junk/b :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :junk/c :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(defn- run-tx [r]
  (cons {:db/id (str "run-" r) :turn/id (str "run-" r)}
        (for [o (range 3)]
          (cond-> {:eval/run (str "run-" r) :eval/ordinal o :eval/source (str "(+ " o ")")}
            (even? r) (assoc :eval/shown (str o))))))

(def ^:private config (atom nil))

(defn- with-store [f]
  (let [path (str (System/getProperty "java.io.tmpdir") "/dh-identity-join-" (random-uuid))
        cfg (store-config path)]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn schema)
        (doseq [part (partition-all 500 (mapcat run-tx (range runs)))]
          (d/transact conn (vec part)))
        (doseq [part (partition-all 5000 (for [j (range 30000)] {:junk/a j :junk/b j :junk/c j}))]
          (d/transact conn (vec part)))
        (finally (d/release conn))))
    (reset! config cfg)
    (try (f)
         (finally
           (reset! config nil)
           (d/delete-database cfg)
           (doseq [file (reverse (file-seq (io/file path)))] (io/delete-file file true))))))

(use-fixtures :once with-store)

(defn- with-conn [f]
  (let [conn (d/connect @config)]
    (try (f conn) (finally (d/release conn)))))

(defn- clear-plans! []
  (vreset! @#'datahike.query/plan-cache (lru/lru 100)))

(def ^:private receipts
  '[:find ?run-id ?ordinal ?source ?result
    :in $ [?run-id ...]
    :where
    [?run :turn/id ?run-id]
    [?form :eval/run ?run]
    [?form :eval/ordinal ?ordinal]
    [?form :eval/source ?source]
    [?receipt :eval/run ?run]
    [?receipt :eval/ordinal ?ordinal]
    [(get-else $ ?receipt :eval/shown "absent") ?result]])

(def ^:private shapes
  "[label query args] — identity joins the planner and the base engine must answer alike."
  [[:collection receipts [["run-5" "run-6"]]]
   [:missing receipts [["run-none"]]]
   [:scalar '[:find ?x ?o :in $ ?id :where [?run :turn/id ?id] [?x :eval/run ?run] [?x :eval/ordinal ?o]]
    ["run-7"]]
   [:literal '[:find ?x ?o :where [?run :turn/id "run-8"] [?x :eval/run ?run] [?x :eval/ordinal ?o]] []]
   [:lookup-ref '[:find ?x ?s :in $ ?run :where [?x :eval/run ?run] [?x :eval/source ?s]]
    [[:turn/id "run-9"]]]
   [:join-last '[:find ?x ?s :in $ [?id ...] :where [?x :eval/run ?run] [?x :eval/source ?s] [?run :turn/id ?id]]
    [["run-10" "run-11"]]]
   [:with-predicate '[:find ?x ?o :in $ ?id
                      :where [?run :turn/id ?id] [?x :eval/run ?run] [?x :eval/ordinal ?o] [(> ?o 0)]]
    ["run-13"]]
   [:wide-group-with-predicate '[:find ?x ?o :in $ ?id
                                 :where [?run :turn/id ?id] [?x :eval/run ?run]
                                 [?x :eval/ordinal ?o] [?x :eval/source ?source]
                                 [?x :eval/shown ?shown] [(> ?o 0)]]
    ["run-12"]]
   [:two-groups '[:find ?x ?y :in $ ?id
                  :where [?run :turn/id ?id] [?x :eval/run ?run] [?x :eval/ordinal 0]
                  [?y :eval/run ?run] [?y :eval/ordinal 2]]
    ["run-12"]]])

(deftest an-identity-join-answers-like-the-base-engine
  (with-conn
    (fn [conn]
      (doseq [[label query args] shapes]
        (testing label
          (clear-plans!)
          (let [planned (binding [q/*query-result-cache?* false] (set (apply d/q query @conn args)))
                base (binding [q/*disable-planner* true q/*query-result-cache?* false]
                       (set (apply d/q query @conn args)))]
            (is (= base planned))))))))

(deftest an-identity-join-has-answers
  (with-conn
    (fn [conn]
      (is (= #{["run-6" 0 "(+ 0)" "0"] ["run-6" 1 "(+ 1)" "1"] ["run-6" 2 "(+ 2)" "2"]
               ["run-5" 0 "(+ 0)" "absent"] ["run-5" 1 "(+ 1)" "absent"] ["run-5" 2 "(+ 2)" "absent"]}
             (set (d/q receipts @conn ["run-5" "run-6"])))))))

(deftest the-bound-unique-lookup-leads
  (with-conn
    (fn [conn]
      (clear-plans!)
      (let [plan (d/explain {:query receipts :args [@conn ["run-5"]]})
            ops (->> (str/split-lines plan) (drop-while #(not= "---" %)) rest
                     (remove #(str/starts-with? % " ")))]
        (is (re-find #"^SCAN .*:turn/id" (first ops)) plan)
        (is (every? #(re-find #"scan: \[\?\w+ :eval/run \?run\]" %)
                    (filter #(str/includes? % "scan:") (str/split-lines plan)))
            plan)))))

(defn- execution-reads
  "Store reads of one execution of `query` on a fresh connection, its plan cached."
  [query & args]
  (clear-plans!)
  (with-conn (fn [conn]
               (binding [q/*query-result-cache?* false]
                 (apply d/q query @conn args))))
  (with-conn
    (fn [conn]
      (let [stats (-> @conn :store :storage :stats)
            before (:reads @stats)]
        (binding [q/*query-result-cache?* false] (apply d/q query @conn args))
        (- (:reads @stats) before)))))

(deftest an-identity-join-reads-the-nodes-of-its-answer
  ;; The lookup alone reads its root-to-leaf paths; the joins add the paths to
  ;; three forms and three receipts. Starting with a group, or re-counting the
  ;; ref attribute while re-planning, read every leaf of :eval/run (139 reads).
  (let [lookup (execution-reads '[:find ?run :in $ ?id :where [?run :turn/id ?id]] "run-5")
        joined (execution-reads receipts ["run-5"])]
    (is (<= lookup 4))
    (is (<= joined 16)))
  (with-conn
    (fn [conn]
      (doseq [part (partition-all 500 (mapcat run-tx (range runs (* 4 runs))))]
        (d/transact conn (vec part)))
      (is (= (* 4 runs)
             (d/q '[:find (count ?run) . :where [?run :turn/id]] @conn)))))
  (is (<= (execution-reads receipts ["run-5"]) 16)))

(deftest a-bound-unique-value-names-at-most-one-entity-per-value
  (with-conn
    (fn [conn]
      (let [db @conn
            pattern (analyze/classify-clause '[?run :turn/id ?id])
            info (analyze/pattern-schema-info db pattern)]
        ;; A heuristic base (no subtree counts) has no per-value meaning;
        ;; the unique law alone bounds the estimate.
        (is (= 100 (estimate/estimate-pattern-with-bindings db pattern info '{?id 100} 38461)))
        (is (= 1 (estimate/estimate-pattern-with-bindings db pattern info '{?id 1} 38461)))
        (is (= 7 (estimate/estimate-pattern-with-bindings db pattern info '{?id 100} 7)))))))

(deftest a-plan-made-on-a-small-database-is-not-reused-on-a-large-one
  ;; Plans are cached by shape and schema. A plan made on a database of a few
  ;; entities scans the ref attribute, and ran unchanged on every larger
  ;; database with the same schema.
  (let [cfg {:store {:backend :memory :id (random-uuid)} :schema-flexibility :write :keep-history? false}]
    (d/create-database cfg)
    (let [small (d/connect cfg)]
      (try
        (d/transact small schema)
        (d/transact small (vec (run-tx 5)))
        (clear-plans!)
        (binding [q/*query-result-cache?* false] (d/q receipts @small ["run-5"]))
        ;; The first execution on the large database may plan; the second is measured.
        (with-conn (fn [conn] (binding [q/*query-result-cache?* false] (d/q receipts @conn ["run-5"]))))
        (with-conn
          (fn [conn]
            (let [stats (-> @conn :store :storage :stats)
                  before (:reads @stats)]
              (is (= 3 (count (binding [q/*query-result-cache?* false] (d/q receipts @conn ["run-5"])))))
              (is (<= (- (:reads @stats) before) 16)))))
        (finally (d/release small) (d/delete-database cfg))))))

(deftest a-join-after-an-or-reads-the-nodes-of-its-answer
  ;; A union whose branches look up bound unique values was estimated at the
  ;; extent of its branches' attributes, so the pattern joined to it ran first
  ;; and read its whole attribute, whatever the number of inputs.
  (let [q-form '[:find ?x ?o :in $ [?id ...]
                 :where (or [?run :turn/id ?id] (and [?run :turn/id ?id] [(= ?id "none")]))
                 [?x :eval/run ?run] [?x :eval/ordinal ?o]]]
    (with-conn
      (fn [conn]
        (binding [q/*query-result-cache?* false]
          (is (= (binding [q/*disable-planner* true] (set (d/q q-form @conn ["run-5"])))
                 (set (d/q q-form @conn ["run-5"]))))
          (is (= 3 (count (d/q q-form @conn ["run-5"])))))))
    (is (<= (execution-reads q-form ["run-5"]) 16))))

(deftest a-union-after-its-binding-pattern-waits-for-it
  ;; A union's bound-aware estimate assumes the vars it was planned under and
  ;; holds only once they are bound. Costed by it from the start, the union ran
  ;; first with ?e unbound and scanned both attributes (60,000 entities: 7.2 ->
  ;; 43.7 MB, 4.8 -> 80 ms).
  (let [database (d/db-with (db/empty-db {:p/team {:db/index true}
                                          :p/tags {:db/cardinality :db.cardinality/many}
                                          :p/nums {:db/cardinality :db.cardinality/many}})
                            (vec (for [i (range 2000)]
                                   {:db/id (inc i) :p/team (if (zero? (mod i 500)) :rare :common)
                                    :p/tags [(str "t" (mod i 7))] :p/nums [(mod i 3)]})))
        clauses '[[?e :p/team :rare] (or [?e :p/tags ?t] [?e :p/nums ?t])]
        plan (#'q/create-plan-via-ir database clauses #{} nil nil)]
    (is (not= :or (:op (first (:ops plan)))))
    (binding [q/*query-result-cache?* false]
      (is (= (binding [q/*disable-planner* true] (set (d/q (into '[:find ?e ?t :where] clauses) database)))
             (set (d/q (into '[:find ?e ?t :where] clauses) database)))))))
