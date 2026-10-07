(ns datahike.test.query-planner-temporal-test
  "Regression tests for planner bugs surfaced by the jobtech-taxonomy-api
   test suite. Each test is a minimal repro that compares the planner
   path against the legacy engine on a HistoricalDB in :attribute-refs?
   mode — the combination that exposed each bug in production. The tests
   should pass on both engines; the assertion form is `(= legacy planner)`
   so a future regression in either path is caught."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query :as q])
  (:import [java.util Date UUID]))

(defn- tx-instant [tx-report]
  (:v (first (filter #(= :db/txInstant (:a %))
                     (:tx-data tx-report)))))

(defn- fresh-cfg
  "In-memory DB with history enabled and attr-refs on — the mode where
   datoms are stored as `(e attr-eid v tx)` rather than `(e :keyword v tx)`,
   so any keyword-attribute pattern that reaches the executor without
   keyword→eid resolution silently slices an empty range. `:writer
   {:backend :self}` makes `transact` return its TxReport synchronously
   so we don't have to deref a Future."
  []
  {:store {:backend :memory :id (UUID/randomUUID)}
   :writer {:backend :self}
   :schema-flexibility :write
   :keep-history? true
   :attribute-refs? true})

(defn- run-both
  "Execute `q-form` once with the planner enabled and once forced to legacy.
   Return both result sets so tests can assert equality, capture both into
   the failure message, and check the planner result independently."
  [q-form & inputs]
  {:legacy  (binding [q/*disable-planner* true]
              (apply d/q q-form inputs))
   :planner (binding [q/*disable-planner* false]
              (apply d/q q-form inputs))})

;; ---------------------------------------------------------------------------
;; Bug 1 — rule body with nested or-join + :attribute-refs? mode
;;
;; A rule body that consists of a single compound form (or-join / or / and)
;; was not having its INNER data patterns' attribute keywords rewritten to
;; eids. The single-level resolve-clause inside rename-branch-vars only
;; matched top-level data patterns:
;;   (and (vector? clause) (not (sequential? (first clause))) ...)
;; — so for a body like `(or-join [...] (and [?e :attr ?v] ...))` the
;; clause itself is a list, vector? returns false, the rewrite is skipped,
;; every nested pattern keeps its keyword, and lookup-batch-search slices
;; AEVT for the keyword (no match in attr-refs storage), returning 0 rows.
;;
;; Surface symptom in jobtech: 3 changelog GraphQL tests on HistoricalDB
;; returned 0 events instead of 2-5. Fixed in
;; src/datahike/query/plan.cljc rename-branch-vars by replacing the
;; one-level resolve-clause with resolve-recursive that descends into
;; or / or-join / and / not / not-join compound forms.

(deftest test-rule-body-or-join-attr-refs
  (testing "rule body containing (or-join ...) with data patterns resolves
            attribute keywords for both engines on HistoricalDB"
    (let [cfg (fresh-cfg)
          conn* (atom nil)]
      (try
        (d/create-database cfg)
        (let [conn (d/connect cfg)
              _ (reset! conn* conn)]
          (d/transact conn
                      [{:db/ident :version/id
                        :db/cardinality :db.cardinality/one
                        :db/valueType :db.type/long
                        :db/unique :db.unique/identity
                        :db/index true}
                       {:db/ident :version/tx
                        :db/cardinality :db.cardinality/one
                        :db/valueType :db.type/ref}])
          ;; Two version markers, each linked to its own creation tx
          (d/transact conn [{:version/id 1 :version/tx "datomic.tx"}])
          (d/transact conn [{:version/id 2 :version/tx "datomic.tx"}])

          (let [hdb     (d/history (d/db conn))
                ;; Rule body is a single (or-join …) compound. Without
                ;; recursive attr-resolution the planner returns ∅.
                ;; Legacy can only execute this rule when the input
                ;; is concretely bound BEFORE the rule call (otherwise
                ;; legacy sum-rel chokes on heterogeneous branch attrs);
                ;; the production callers all bind via
                ;; `[(identity ?in-input) ?in]` first, so we mirror that
                ;; here to keep the test exercising the bug we fixed
                ;; rather than an orthogonal legacy edge case.
                rules   '[[(->inst ?in ?ret)
                           (or-join [?in ?ret]
                                    (and [(int? ?in)]
                                         [?v :version/id ?in]
                                         [?v :version/tx ?tx]
                                         [?tx :db/txInstant ?ret])
                                    (and [(inst? ?in)]
                                         [(identity ?in) ?ret]))]]
                q-form  '[:find ?ret
                          :in $ % ?in-input
                          :where
                          [(identity ?in-input) ?in]
                          (->inst ?in ?ret)]
                {:keys [legacy planner]} (run-both q-form hdb rules 1)]
            (is (= 1 (count legacy))
                "legacy resolves the rule and returns one tx-instant")
            (is (= legacy planner)
                "planner must match legacy under attribute-refs+history")
            (is (instance? java.util.Date (ffirst planner))
                "planner returns a real instant, not nil/empty")))
        (finally
          (when-let [conn @conn*] (d/release conn))
          (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Bug 2 — pushed-down predicate dropped in entity-group temporal fallback
;;
;; The planner pushes range predicates like `[(<= ?from ?inst)]` onto the
;; relevant scan-op's :pushdown-preds and consumes the original clause
;; (so it never reappears as a standalone :predicate op). On regular DBs
;; the fused PSS scan path applies the predicate via slice bounds and a
;; strict-filter; on HistoricalDB / AsOfDB / SinceDB the fallback
;; delegates to legacy/lookup-batch-search, which has no concept of
;; :pushdown-preds — the predicate is silently dropped and rows that
;; would have been filtered out leak through.
;;
;; Surface symptom in jobtech: 3 daynotes-test cases returned more rows
;; than legacy because their `[(<= ?from-version ?inst)]` predicate was
;; pushed onto the [?tx :db/txInstant ?inst] scan but never evaluated.
;; Fixed in src/datahike/query/execute.cljc :entity-group temporal
;; branch by re-applying scan-op + merge-op :pushdown-preds via
;; legacy/filter-by-pred after the scan/merge/optional/anti phases.

(deftest test-temporal-entity-group-pushdown-pred
  (testing "range predicate pushed onto entity-group scan-op is applied
            on HistoricalDB"
    (let [cfg (fresh-cfg)
          conn* (atom nil)]
      (try
        (d/create-database cfg)
        (let [conn (d/connect cfg)
              _ (reset! conn* conn)]
          (d/transact conn
                      [{:db/ident :event/marker
                        :db/cardinality :db.cardinality/one
                        :db/valueType :db.type/string
                        :db/index true}
                       {:db/ident :event/category
                        :db/cardinality :db.cardinality/one
                        :db/valueType :db.type/string}])
          ;; Three transactions with distinct txInstants. A predicate
          ;; bounded between tx2 and tx3 must keep ONLY the middle event.
          (d/transact conn [{:event/marker "e1" :event/category "alpha"}])
          (Thread/sleep 5)
          (d/transact conn [{:event/marker "e2" :event/category "beta"}])
          (Thread/sleep 5)
          (d/transact conn [{:event/marker "e3" :event/category "alpha"}])

          (let [hdb (d/history (d/db conn))
                ;; Three event-bearing txs in chronological order. We
                ;; pick a window that excludes the FIRST event but
                ;; includes the other two — the predicate must shrink
                ;; the result set from 3 → 2.
                event-tx-instants (sort
                                   (mapv first
                                         (d/q '[:find ?inst
                                                :where
                                                [_ :event/marker ?m ?tx true]
                                                [?tx :db/txInstant ?inst]]
                                              hdb)))
                e1-inst (nth event-tx-instants 0)
                e3-inst (nth event-tx-instants 2)
                ;; Range: > e1, ≤ e3 — should keep e2 and e3.
                from-inst (java.util.Date/from
                           (.plus (.toInstant ^java.util.Date e1-inst)
                                  1 java.time.temporal.ChronoUnit/MILLIS))
                to-inst   e3-inst
                q-form '[:find ?m
                         :in $ ?from-inst ?to-inst
                         :where
                         [?e :event/marker ?m ?tx true]
                         [?tx :db/txInstant ?inst]
                         [(<= ?from-inst ?inst)]
                         [(<= ?inst ?to-inst)]]
                {:keys [legacy planner]} (run-both q-form hdb from-inst to-inst)]
            (is (= 2 (count legacy))
                "legacy filters out e1, keeps e2 and e3")
            (is (= legacy planner)
                "planner must match legacy — pushed-down predicate
                  must be re-applied in the temporal fallback path")
            (is (= #{["e2"] ["e3"]} (set planner))
                "planner result must drop e1 (whose ?inst < ?from-inst)")))
        (finally
          (when-let [conn @conn*] (d/release conn))
          (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Bug 2b — same shape as Bug 2 but for the STANDALONE pattern-scan path
;;
;; Bug 2 fixed the entity-group temporal branch. A separate code path —
;; `execute-plan`'s `:pattern-scan` case (single pattern, no merges to
;; fuse) — also delegates to `legacy/lookup-batch-search` on a temporal
;; DB and was symmetrically missing the post-filter step for
;; :pushdown-preds. So a query whose WHERE has a SINGLE pattern plus a
;; range predicate that the planner pushes onto it surfaces the bug all
;; over again on HistoricalDB / AsOfDB / SinceDB.
;;
;; Surface symptom: any temporal range query of the shape
;;   [?tx :db/txInstant ?inst] [(<= ?from ?inst)]
;; would over-return — the pushed-down `?from ≤ ?inst` predicate would
;; be consumed at plan time and silently dropped at execute time.
;;
;; Fixed in src/datahike/query/execute.cljc `:pattern-scan` temporal
;; branch by re-applying op-level :pushdown-preds via
;; legacy/filter-by-pred after lookup-batch-search.

(deftest test-temporal-standalone-pattern-scan-pushdown-pred
  (testing "range predicate pushed onto a STANDALONE pattern-scan is
            applied on HistoricalDB"
    (let [cfg (fresh-cfg)
          conn* (atom nil)]
      (try
        (d/create-database cfg)
        (let [conn (d/connect cfg)
              _ (reset! conn* conn)]
          (d/transact conn
                      [{:db/ident :event/marker
                        :db/cardinality :db.cardinality/one
                        :db/valueType :db.type/string}])
          ;; Three txs producing three distinct txInstants. The query
          ;; below has the txInstant pattern as a standalone
          ;; :pattern-scan op (not part of an entity-group). The
          ;; `[?m ...]` collection binding forces non-empty :rels in
          ;; the context, which makes execute-plan-direct ineligible
          ;; and the plan is executed through `execute-plan` — that's
          ;; the path with the temporal-pattern-scan pushdown-pred
          ;; drop bug.
          (d/transact conn [{:event/marker "e1"}])
          (Thread/sleep 5)
          (d/transact conn [{:event/marker "e2"}])
          (Thread/sleep 5)
          (d/transact conn [{:event/marker "e3"}])

          (let [hdb (d/history (d/db conn))
                tx-instants (sort
                             (mapv first
                                   (d/q '[:find ?inst
                                          :where
                                          [_ :event/marker _ ?tx true]
                                          [?tx :db/txInstant ?inst]]
                                        hdb)))
                e1-inst (nth tx-instants 0)
                from-inst (java.util.Date/from
                           (.plus (.toInstant ^java.util.Date e1-inst)
                                  1 java.time.temporal.ChronoUnit/MILLIS))
                q-form '[:find ?inst
                         :in $ ?from-inst [?m ...]
                         :where
                         [?e :event/marker ?m]
                         [_ _ _ ?tx true]
                         [?tx :db/txInstant ?inst]
                         [(<= ?from-inst ?inst)]]
                {:keys [legacy planner]} (run-both q-form hdb from-inst ["e1" "e2" "e3"])]
            (is (= (set legacy) (set planner))
                "planner must match legacy — pushed-down predicate
                  must be re-applied in the standalone pattern-scan
                  temporal fallback path")
            (is (not (some #(= e1-inst (first %)) planner))
                "planner result must drop the e1 tx (whose ?inst < ?from-inst)")))
        (finally
          (when-let [conn @conn*] (d/release conn))
          (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Bug 3 — LOptionalScan reordered before its entity-var binders
;;
;; The planner classifies LOptionalScan ops (synthesized from a
;; `[(get-else $ ?e :attr default) ?v]` clause) as :pattern-scan in the
;; physical plan. order-plan-ops puts every :pattern-scan into the
;; "groups" partition and feeds those to dp-order-groups, which only
;; considers shared-var connectivity for the join graph — it has no
;; awareness of the optional scan's runtime constraint that ?e MUST be
;; bound BEFORE the scan executes. At runtime, the :optional? branch in
;; execute-plan routes through `legacy/bind-by-fn` with a synthetic
;; `(get-else $ ?e :attr default)` fn-clause; bind-by-fn evaluates the
;; fn per row in ctx.rels — and if ?e is still free, the fn gets a nil
;; entity, returns the default value, and binds the bind-var on a row
;; where ?e itself is also nil. That nil-?e tuple then propagates
;; through downstream ops as if it were a real binding, producing
;; all-nil-tuple find results.
;;
;; Surface symptom in jobtech: 2 daynotes test-relation-changes tests
;; got `event-concept-1 = nil` because fetch-relation-txs (an outer
;; query with `[?c :concept/id ?input-concept-id]` followed by two
;; `(or ...)` clauses, three `(get-else …)` clauses, and a range
;; predicate) had the optional scan for
;; `:relation/substitutability-percentage` placed BEFORE the OR that
;; binds ?r. Fixed in src/datahike/query/plan.cljc order-plan-ops by
;; routing optional pattern-scans through the non-groups partition,
;; which uses op-cost+bound-vars to wait until the e-var binder has
;; run.

(deftest test-optional-scan-after-binder
  (testing "LOptionalScan (get-else with default) is ordered after its
            entity-var binder so its e-var isn't nil at execute time"
    (let [cfg (fresh-cfg)
          conn* (atom nil)]
      (try
        (d/create-database cfg)
        (let [conn (d/connect cfg)
              _ (reset! conn* conn)]
          (d/transact conn
                      [{:db/ident :concept/id
                        :db/cardinality :db.cardinality/one
                        :db/valueType :db.type/string
                        :db/unique :db.unique/identity
                        :db/index true}
                       {:db/ident :relation/concept-1
                        :db/cardinality :db.cardinality/one
                        :db/valueType :db.type/ref}
                       {:db/ident :relation/concept-2
                        :db/cardinality :db.cardinality/one
                        :db/valueType :db.type/ref}
                       {:db/ident :relation/percentage
                        :db/cardinality :db.cardinality/one
                        :db/valueType :db.type/long}])
          (d/transact conn [{:concept/id "C1"} {:concept/id "C2"}])
          (let [c1 (ffirst (d/q '[:find ?e :where [?e :concept/id "C1"]] (d/db conn)))
                c2 (ffirst (d/q '[:find ?e :where [?e :concept/id "C2"]] (d/db conn)))]
            (d/transact conn [{:relation/concept-1 c1
                               :relation/concept-2 c2
                               :relation/percentage 50}]))

          (let [hdb (d/history (d/db conn))
                ;; The query that triggered the bug: ?r is introduced by
                ;; the (or ...) clause, BEFORE the get-else expects it.
                ;; The cost-based reorderer must NOT lift the optional
                ;; scan above the OR.
                q-form '[:find ?r ?pct
                         :in $
                         :where
                         [?c :concept/id ?cid]
                         (or [?r :relation/concept-1 ?c]
                             [?r :relation/concept-2 ?c])
                         [(get-else $ ?r :relation/percentage 0) ?pct]]
                {:keys [legacy planner]} (run-both q-form hdb)]
            (is (= 1 (count legacy))
                "legacy returns one row with ?r and ?pct bound")
            (is (= legacy planner)
                "planner must match legacy — the optional scan must run
                  AFTER ?r is bound, not before")
            (is (every? some? (first planner))
                "planner row must have non-nil values — a pre-fix run
                  produces a tuple of all-nils because the optional scan
                  ran with ?r still free")))
        (finally
          (when-let [conn @conn*] (d/release conn))
          (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Note 179 §4 Gap 1 — zero-cost overlay invariant
;;
;; The substrate's framing in schema.cljc:71-74 — "valid-time is an
;; overlay on tx-time" — implies that a query that does NOT reference
;; `:db.valid/*` must produce IDENTICAL plans on a vt-equipped DB and
;; on a plain DB. Per note 179 §6 Q1, this property has no test
;; anywhere in the suite. The deftest below locks it in:
;;
;;   1. Plan the same EAV query on both a baseline DB and a DB that
;;      contains vt-bearing transactions.
;;   2. Assert the plans are structurally identical.
;;
;; A future refactor that, say, wires a vt filter "just to be safe"
;; into the planner's default path would silently regress this
;; invariant — and the rest of the suite would stay green because it
;; only asserts behavioural correctness, not plan shape.
;;
;; The `(q/explain ...)` string-rendering is the canonical
;; introspection hook (it folds the same plan tree the executor
;; consumes through `format-plan-ops`). We strip the noisy `rules:`
;; header line that lists the built-in interval rules — those are
;; loaded into every Context regardless of whether the query uses them
;; and aren't a per-DB property — and assert the remaining plan body
;; matches byte-for-byte.

(defn- strip-rules-line
  "Remove the `rules: [...]` header line from a (q/explain ...) string.
   Built-in interval rules are loaded uniformly; they are not part of the
   per-DB plan shape we want to compare."
  [^String s]
  (->> (str/split-lines s)
       (remove #(str/starts-with? % "rules:"))
       (str/join "\n")))

(deftest test-vt-schema-zero-cost-for-non-vt-query
  (testing "a query that does NOT touch :db.valid/* must produce the
            identical plan whether the DB has vt-bearing data or not.
            This is the maintainer's headline overlay invariant —
            non-vt queries pay zero plan-shape cost."
    (let [baseline-cfg {:store {:backend :memory :id (UUID/randomUUID)}
                        :writer {:backend :self}
                        :schema-flexibility :write
                        :keep-history? true}
          vt-cfg {:store {:backend :memory :id (UUID/randomUUID)}
                  :writer {:backend :self}
                  :schema-flexibility :write
                  :keep-history? true}
          baseline* (atom nil)
          vt* (atom nil)]
      (try
        (d/create-database baseline-cfg)
        (d/create-database vt-cfg)
        (let [baseline (d/connect baseline-cfg)
              vt (d/connect vt-cfg)
              _ (reset! baseline* baseline)
              _ (reset! vt* vt)
              schema [{:db/ident :user/name
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one
                       :db/unique :db.unique/identity}
                      {:db/ident :user/age
                       :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one
                       :db/index true}]]
          (d/transact baseline schema)
          (d/transact vt schema)
          ;; Baseline: write with no vt-meta.
          (d/transact baseline [{:user/name "Alice" :user/age 30}
                                {:user/name "Bob"   :user/age 40}])
          ;; vt: write the SAME data but every tx carries a vt-meta
          ;; window. The vt-bearing data must NOT perturb the plan for
          ;; a query that doesn't reference :db.valid/*.
          (d/transact vt {:tx-data [{:user/name "Alice" :user/age 30}]
                          :tx-meta {:db.valid/from #inst "2024-01-01"
                                    :db.valid/to   #inst "2024-07-01"}})
          (d/transact vt {:tx-data [{:user/name "Bob" :user/age 40}]
                          :tx-meta {:db.valid/from #inst "2024-01-01"}})

          (testing "simple EAV scan — `[?e :user/name \"Alice\"]`"
            (let [q '[:find ?e :where [?e :user/name "Alice"]]
                  baseline-plan (strip-rules-line (q/explain q (d/db baseline)))
                  vt-plan       (strip-rules-line (q/explain q (d/db vt)))]
              (is (= baseline-plan vt-plan)
                  (str "plans must be identical for a non-vt query.\n"
                       "baseline:\n" baseline-plan "\n"
                       "vt:\n" vt-plan))))

          (testing "multi-clause join — entity-group with predicate"
            (let [q '[:find ?n ?a
                      :where
                      [?e :user/name ?n]
                      [?e :user/age ?a]
                      [(> ?a 35)]]
                  baseline-plan (strip-rules-line (q/explain q (d/db baseline)))
                  vt-plan       (strip-rules-line (q/explain q (d/db vt)))]
              (is (= baseline-plan vt-plan)
                  (str "plans must be identical for a multi-clause
                        non-vt query.\nbaseline:\n" baseline-plan
                       "\nvt:\n" vt-plan))))

          (testing "behavioural correctness — vt data answers the query"
            ;; Sanity: the vt-equipped DB still resolves the same data
            ;; the baseline does. Without this check, a "plans equal"
            ;; assertion could be vacuously true if vt-data simply
            ;; failed to land.
            (let [q '[:find ?n :where [_ :user/name ?n]]
                  baseline-result (set (d/q q (d/db baseline)))
                  vt-result       (set (d/q q (d/db vt)))]
              (is (= #{["Alice"] ["Bob"]} baseline-result))
              (is (= baseline-result vt-result)
                  "vt-DB answers the non-vt query identically"))))
        (finally
          (when-let [baseline @baseline*] (d/release baseline))
          (when-let [vt @vt*] (d/release vt))
          (d/delete-database baseline-cfg)
          (d/delete-database vt-cfg))))))

;; ---------------------------------------------------------------------------
;; Bug — get-else enumerated every historical version instead of one value
;;
;; `[(get-else $ ?e :attr default) ?v]` is single-valued: the legacy
;; `-get-else` returns `(first (search …))`, i.e. exactly one value (or the
;; supplied default) per entity. On a HistoricalDB, though, the planner forced
;; every merge card-many so all versions surface — so get-else emitted one row
;; per historical version (e.g. `[e 40] [e 25]` after an update), and one row
;; per value for a card-many attribute, diverging from the legacy engine. The
;; fix marks the optional (get-else) merge single-valued regardless of temporal
;; type or attribute cardinality.

(deftest test-get-else-on-history-is-single-valued
  (testing "get-else over d/history yields one value per entity, matching legacy"
    (let [cfg (fresh-cfg)
          conn* (atom nil)]
      (try
        (d/create-database cfg)
        (let [conn (d/connect cfg)
              _ (reset! conn* conn)]
          (d/transact conn [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                            {:db/ident :age  :db/valueType :db.type/long   :db/cardinality :db.cardinality/one}
                            {:db/ident :tag  :db/valueType :db.type/string :db/cardinality :db.cardinality/many}])
          (d/transact conn [{:db/id -1 :name "Alice" :age 40 :tag ["x" "y"]}
                            {:db/id -2 :name "Bob"}])          ; Bob: no age/tag → default
          (let [e1 (ffirst (d/q '[:find ?e :where [?e :name "Alice"]] (d/db conn)))]
            (d/transact conn [{:db/id e1 :age 25}])            ; card-one update  40 → 25
            (d/transact conn [[:db/retract e1 :tag "x"]])      ; card-many retract
            (d/transact conn [{:db/id e1 :tag "z"}])           ; card-many add
            (let [hdb (d/history (d/db conn))
                  q-card-one '[:find ?e ?a :where [?e :name ?n] [(get-else $ ?e :age 0) ?a]]
                  q-card-many '[:find ?e ?t :where [?e :name ?n] [(get-else $ ?e :tag "none") ?t]]]
              (doseq [[label q] [["card-one :age"  q-card-one]
                                 ["card-many :tag" q-card-many]]]
                (let [{:keys [legacy planner]} (run-both q hdb)]
                  (is (= legacy planner)
                      (str label ": planner must match legacy get-else on history"))
                  ;; each entity appears at most once (single-valued get-else)
                  (is (= (count planner) (count (into #{} (map first) planner)))
                      (str label ": at most one row per entity")))))))
          (finally
            (when-let [conn @conn*] (d/release conn))
            (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Bug — date as-of card-one merge skipped the older visible value
;;
;;
;; Minimal repro:
;;
;;   (d/transact conn [{:name "Alice" :age 20}
;;                     {:name "Bob" :age 30}
;;                     {:name "Charlie" :age 40}])
;;   (let [timestamp (Date.)]
;;     (d/transact conn {:tx-data [{:db/id 3 :age 25}]})
;;     (d/q '[:find ?e ?n ?a
;;            :where
;;            [?e :name ?n]
;;            [?e :age ?a]]
;;          (d/as-of (d/db conn) timestamp)))
;;
;;  as-of timestamp
;;   expected: #{[1 "Alice" 20] [2 "Bob" 30] [3 "Charlie" 40]}
;;     actual: #{[1 "Alice" 20] [2 "Bob" 30]}
;;
;;
;; From native-pod tests but was reproducible on JVM and JS. The failure
;; only appeared when the Date used for `as-of` landed in the millisecond
;; gap after the initial txInstant and before the update txInstant. In that
;; window the planner's card-one merge found Charlie's current age datom,
;; rejected it as too new, and stopped instead of looking back through
;; temporal history for the older visible age.

(deftest test-as-of-card-one-merge-uses-older-visible-value-when-current-value-is-too-new
  (testing "date-based as-of keeps the old card-one value between tx instants"
    (let [cfg {:keep-history? true
               :search-cache-size 10000
               :index :datahike.index/persistent-set
               :store {:id (UUID/randomUUID)
                       :backend :memory
                       :scope "query-planner-temporal-test"}
               :store-cache-size 1000
               :attribute-refs? false
               :writer {:backend :self}
               :crypto-hash? false
               :schema-flexibility :read
               :branch :db}
          q '[:find ?e ?n ?a
              :where
              [?e :name ?n]
              [?e :age ?a]]
          conn* (atom nil)]
      (try
        (d/create-database cfg)
        (let [conn (d/connect cfg)
              _ (reset! conn* conn)
              tx1 (d/transact conn [{:name "Alice" :age 20}
                                    {:name "Bob" :age 30}
                                    {:name "Charlie" :age 40}])
              tx1-ms (.getTime ^Date (tx-instant tx1))]
          (Thread/sleep 20)
          (let [tx2 (d/transact conn {:tx-data [{:db/id 3 :age 25}]})
                tx2-ms (.getTime ^Date (tx-instant tx2))
                before-tx1 (Date. (dec tx1-ms))
                at-tx1 (Date. tx1-ms)
                before-tx2 (Date. (dec tx2-ms))
                at-tx2 (Date. tx2-ms)
                before-update #{[1 "Alice" 20] [2 "Bob" 30] [3 "Charlie" 40]}
                after-update #{[1 "Alice" 20] [2 "Bob" 30] [3 "Charlie" 25]}]
            (is (< tx1-ms (.getTime before-tx2) tx2-ms))
            (doseq [[label as-of-date expected]
                    [["before first tx" before-tx1 #{}]
                     ["at first tx" at-tx1 before-update]
                     ["one ms before second tx" before-tx2 before-update]
                     ["at second tx" at-tx2 after-update]]]
              (let [result (run-both q (d/as-of (d/db conn) as-of-date))]
                (is (= (:legacy result) (:planner result)))
                (is (= expected (:planner result)) label)))))
        (finally
          (when-let [conn @conn*] (d/release conn))
          (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; A history merge over a scan in VALUE order
;;
;; The history merge walks a [current temporal] EAVT cursor pair that only
;; moves forward, which serves a scan emitting entities in ascending order
;; (EAVT/AEVT, or entity seeks). An AVET scan — a range predicate pushed onto
;; an indexed attribute, or per-value seeks for a bound value — emits entities
;; in VALUE order, so the cursors skipped every entity below the last one
;; merged and the planner silently dropped rows. Names here sort opposite to
;; entity ids, so value order is descending entity order.

(deftest test-history-merge-over-value-ordered-scan
  (let [cfg (fresh-cfg)
        conn* (atom nil)]
    (try
      (d/create-database cfg)
      (let [conn (d/connect cfg)
            _ (reset! conn* conn)]
        (d/transact conn [{:db/ident :item/name :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db/index true}
                          {:db/ident :item/team :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn (vec (for [i (range 10)] {:item/name (str "n" (- 9 i)) :item/team i})))
        (let [hdb (d/history (d/db conn))]
          (doseq [[label q-form args]
                  [["range predicate on an indexed value"
                    '[:find ?n ?t :where [?e :item/name ?n] [(> ?n "n3")] [?e :item/team ?t]] []]
                   ["bound value collection"
                    '[:find ?n ?t :in $ [?n ...] :where [?e :item/name ?n] [?e :item/team ?t]]
                    [["n8" "n2" "n5"]]]]]
            (testing label
              (let [{:keys [legacy planner]} (apply run-both q-form hdb args)]
                (is (= (set legacy) (set planner))))))
          (is (= #{["n8" 1] ["n2" 7] ["n5" 4]}
                 (set (d/q '[:find ?n ?t :in $ [?n ...] :where [?e :item/name ?n] [?e :item/team ?t]]
                           hdb ["n8" "n2" "n5"]))))))
      (finally
        (when-let [conn @conn*] (d/release conn))
        (d/delete-database cfg)))))

;; ---------------------------------------------------------------------------
;; Recursive rules answer like the base engine in every world
;;
;; A recursive rule's head vars kept their own spelling inside the fixpoint,
;; and the fixpoint keeps every outer relation sharing a var with the branch
;; plans: a head var spelled like an outer query var (`?a` below) held every
;; recursive step to the caller's values, so the closure stopped at depth one.
;; A lookup ref passed to a rule call (scalar input or literal) was never
;; resolved, so the rule answered for every entity.

(defn- chain-db
  "Twenty people, each the friend of the previous one, then p4 retracted."
  []
  (let [base (d/db-with (db/empty-db {:p/id {:db/unique :db.unique/identity}
                                      :p/friend {:db/valueType :db.type/ref}}
                                     {:keep-history? true})
                        (vec (for [i (range 20)]
                               (cond-> {:db/id (- (inc i)) :p/id (str "p" i)}
                                 (pos? i) (assoc :p/friend (- i))))))]
    {:before base
     :after (d/db-with base [[:db/retractEntity [:p/id "p4"]]])}))

(defn- ring-db
  "Eighty entities, each referring to the next and to the one three on, plus
   one move of a unique value and one retracted ref."
  []
  (let [before (d/db-with (db/empty-db {:u/id {:db/unique :db.unique/identity}
                                        :u/ref {:db/valueType :db.type/ref
                                                :db/cardinality :db.cardinality/many}
                                        :u/a {:db/index true}}
                                       {:keep-history? true})
                          (vec (for [i (range 1 81)]
                                 {:db/id i :u/id i :u/a (mod i 7)
                                  :u/ref (vec (distinct [(inc (mod i 80)) (inc (mod (+ i 3) 80))]))})))]
    {:before before
     :after (d/db-with before [[:db/retract 1 :u/id 1] [:db/add 81 :u/id 1] [:db/add 81 :u/a 3]
                               [:db/add 81 :u/ref 3] [:db/retract 2 :u/ref 3]])}))

(defn- worlds [{:keys [before after]}]
  {:current after :history (d/history after) :as-of (d/as-of after (:max-tx before))
   :unretracted before})

(defn- outcome [q-form database args]
  (try (set (apply d/q q-form database args))
       (catch Exception e {:error (ex-message e)})))

(deftest recursive-rules-answer-like-the-base-engine
  (let [reach '[[(reach ?a ?b) [?a :p/friend ?b]] [(reach ?a ?b) [?a :p/friend ?x] (reach ?x ?b)]]
        path '[[(path ?e ?x) [?e :u/ref ?x]]
               [(path ?e ?x) [?e :u/ref ?y] [(< ?e ?y)] [(< ?y 8)] (path ?y ?x)]]
        friend '[[(friend ?a ?b) [?a :p/friend ?b]]]
        cases {(worlds (chain-db))
               [["identity input" '[:find ?b :in $ % ?id :where [?a :p/id ?id] (reach ?a ?b)] [reach "p6"]]
                ["lookup-ref input" '[:find ?b :in $ % ?a :where (reach ?a ?b)] [reach [:p/id "p6"]]]
                ["lookup-ref input in find" '[:find ?a ?b :in $ % ?a :where (reach ?a ?b)] [reach [:p/id "p6"]]]
                ["lookup-ref input, right" '[:find ?a :in $ % ?b :where (reach ?a ?b)] [reach [:p/id "p2"]]]
                ["literal lookup ref" '[:find ?b :in $ % :where (reach [:p/id "p6"] ?b)] [reach]]
                ["missing lookup ref" '[:find ?b :in $ % ?a :where (reach ?a ?b)] [reach [:p/id "none"]]]
                ["caller-bound pass-through head var" '[:find ?a ?b :in $ % [?b ...] :where (reach ?a ?b)] [reach [3 5]]]
                ["caller-bound pass-through var, other spelling" '[:find ?x ?y :in $ % [?y ...] :where (reach ?x ?y)] [reach [3 5]]]
                ["lookup-ref input, plain rule" '[:find ?b :in $ % ?a :where (friend ?a ?b)] [friend [:p/id "p6"]]]]
               (worlds (ring-db))
               [["head vars spelled like outer vars"
                 '[:find ?e ?x :in $ % [?id ...] :where [?e :u/id ?id] (path ?e ?x) [?x :u/a ?a]] [path [1 2]]]]}]
    (doseq [[ws rows] cases
            [world database] ws
            [label q-form args] rows]
      (testing [world label]
        (binding [q/*query-result-cache?* false]
          (is (= (binding [q/*disable-planner* true] (outcome q-form database args))
                 (outcome q-form database args))))))
    (is (= #{[1] [2] [3] [4] [5] [6]}
           (binding [q/*query-result-cache?* false]
             (set (d/q '[:find ?b :in $ % ?a :where (reach ?a ?b)]
                       (d/history (:after (chain-db))) reach [:p/id "p6"])))))))

;; ---------------------------------------------------------------------------
;; History merge versions belong to the scan datom they join
;;
;; The history cursor merge buffered an entity's matching versions once, checked
;; against the FIRST scan datom's v and tx, and replayed them for every later
;; scan datom of that entity: a shared v or tx var then joined versions that
;; matched a different scan datom.

(deftest history-merge-matches-belong-to-the-scan-version
  (let [before (d/db-with (db/empty-db {:item/a {:db/cardinality :db.cardinality/many}
                                        :item/b {:db/cardinality :db.cardinality/many}}
                                       {:keep-history? true})
                          [{:db/id 1 :item/a [1 2] :item/b [1 4 5]}])
        after (d/db-with before [[:db/retract 1 :item/b 1] [:db/add 1 :item/a 4]])]
    (doseq [[label q-form database]
            [["shared value" '[:find ?v :where [?e :item/a ?v] [?e :item/b ?v]] (d/history before)]
             ["shared value and tx" '[:find ?v ?tx :where [?e :item/a ?v ?tx] [?e :item/b ?v]] (d/history after)]
             ["shared tx" '[:find ?v ?w :where [?e :item/a ?v ?tx] [?e :item/b ?w ?tx]] (d/history after)]]]
      (testing label
        (binding [q/*query-result-cache?* false]
          (let [{:keys [legacy planner]} (run-both q-form database)]
            (is (= (set legacy) (set planner)))))))
    (is (= #{[1]} (binding [q/*query-result-cache?* false]
                    (set (d/q '[:find ?v :where [?e :item/a ?v] [?e :item/b ?v]] (d/history before))))))))

;; ---------------------------------------------------------------------------
;; Lookup-shaped data passed to a rule or an input stays data
;;
;; A two-vector headed by a keyword names an entity only where it is used as one:
;; a pattern's entity or tx position, or a ref attribute's value. Rule arguments
;; and input relations were resolved as lookup refs whatever their use, so a tuple
;; argument threw and a lookup ref compared as a value answered its entity id.

(deftest rule-arguments-preserve-lookup-shaped-data
  (let [schema {:p/id {:db/unique :db.unique/identity}
                :p/friend {:db/valueType :db.type/ref}
                :p/pair {:db/valueType :db.type/tuple :db/tupleTypes [:db.type/keyword :db.type/long]}}
        before (d/db-with (db/empty-db schema {:keep-history? true})
                          [{:db/id 1 :p/id "a" :p/pair [:k1 1]} {:db/id 2 :p/id "b" :p/pair [:k2 2] :p/friend 1}
                           {:db/id 3 :p/id "c" :p/friend 2}])
        after (d/db-with before [[:db/retract 2 :p/id "b"] [:db/add 4 :p/id "b"] [:db/add 4 :p/friend 1]])
        rules '[[(has-pair ?e ?p) [?e :p/pair ?p]] [(same ?x ?y) [(= ?x ?y)]] [(idv ?v ?x) [(identity ?v) ?x]]
                [(friend ?e ?x) [?e :p/friend ?x]] [(friend-of ?e ?x) [?x :p/friend ?e]]]
        rows [["tuple literal" '[:find ?e :in $ % :where (has-pair ?e [:k1 1])] [rules]]
              ["tuple input" '[:find ?e :in $ % ?p :where (has-pair ?e ?p)] [rules [:k1 1]]]
              ["tuple collection to a rule" '[:find ?e :in $ % [?p ...] :where (has-pair ?e ?p)] [rules [[:k1 1] [:k2 2]]]]
              ["tuple collection" '[:find ?e :in $ [?p ...] :where [?e :p/pair ?p]] [[[:k1 1]]]]
              ["lookup ref compared as a value" '[:find ?e :in $ % :where [?e :p/id "a"] (same ?e [:p/id "a"])] [rules]]
              ["lookup ref bound as a value" '[:find ?x :in $ % ?v :where (idv ?v ?x)] [rules [:p/id "b"]]]
              ["lookup-shaped data naming no attribute" '[:find ?x :in $ % ?v :where (idv ?v ?x)] [rules [:not/attr 2]]]
              ["lookup refs bound as values" '[:find ?x :in $ % [?v ...] :where (idv ?v ?x)] [rules [[:p/id "b"]]]]
              ["lookup ref at an entity position" '[:find ?x :in $ % ?e :where (friend ?e ?x)] [rules [:p/id "c"]]]
              ["literal lookup ref at an entity position" '[:find ?x :in $ % :where (friend [:p/id "c"] ?x)] [rules]]
              ["lookup refs at an entity position" '[:find ?e ?x :in $ % [?e ...] :where (friend ?e ?x)] [rules [[:p/id "c"] [:p/id "b"]]]]
              ["lookup ref at a ref value" '[:find ?x :in $ % ?e :where (friend-of ?e ?x)] [rules [:p/id "a"]]]]]
    (doseq [[world database] {:current after :history (d/history after) :as-of (d/as-of after (:max-tx before))}
            [label q-form args] rows]
      (testing [world label]
        (binding [q/*query-result-cache?* false]
          (is (= (binding [q/*disable-planner* true] (outcome q-form database args))
                 (outcome q-form database args))))))
    (is (= #{[[:p/id "b"]]} (binding [q/*query-result-cache?* false]
                               (set (d/q '[:find ?x :in $ % ?v :where (idv ?v ?x)] after rules [:p/id "b"])))))))

;; ---------------------------------------------------------------------------
;; A var shared inside an entity group is a join
;;
;; A fused merge compared only the scan's value and tx vars at the same position:
;; a shared added var, or a var two merges share, joined every pair of datoms.

(deftest shared-vars-in-an-entity-group-are-joined
  (let [before (d/db-with (db/empty-db {:item/a {:db/cardinality :db.cardinality/many}
                                        :item/b {:db/cardinality :db.cardinality/many}
                                        :item/c {:db/cardinality :db.cardinality/many}
                                        :item/x {}}
                                       {:keep-history? true})
                          [{:db/id 1 :item/a [1 2] :item/b [1 4 5] :item/c [2 5 9] :item/x 0}])
        after (d/db-with before [[:db/retract 1 :item/b 1] [:db/add 1 :item/a 4]
                                 [:db/retract 1 :item/c 5] [:db/add 1 :item/x 3]])
        added '[:find ?v ?w ?added :where [?e :item/a ?v ?tx ?added] [?e :item/b ?w ?tx2 ?added]]]
    (doseq [[world database] {:current after :history (d/history after) :as-of (d/as-of after (:max-tx before))}
            q-form [added
                    '[:find ?v ?added :where [?e :item/a ?v ?tx ?added] [?e :item/b ?v ?tx2 ?added]]
                    '[:find ?w ?added :where [?e :item/a ?v ?tx] [?e :item/b ?w ?tx2 ?added] [?e :item/c ?w ?tx3 ?added]]
                    '[:find ?x ?v :where [?e :item/x ?x] [?e :item/b ?v] [?e :item/c ?v]]
                    '[:find ?x ?v ?a :where [?e :item/x ?x ?t0 ?a0] [?e :item/b ?v ?t1 ?a] [?e :item/c ?v ?t2 ?a2]]]]
      (testing [world q-form]
        (binding [q/*query-result-cache?* false]
          (let [{:keys [legacy planner]} (run-both q-form database)]
            (is (= (set legacy) (set planner)))))))
    (is (= (set (for [v [1 2 4] w [1 4 5]] [v w true]))
           (binding [q/*query-result-cache?* false] (set (d/q added (d/history after))))))))

;; ---------------------------------------------------------------------------
;; A ground retraction flag matches nothing on a current db
;;
;; Every current datom is an assertion, but a fused merge on a current db ignored
;; its ground added flag.

(deftest a-ground-retraction-flag-matches-nothing-on-a-current-db
  (let [before (d/db-with (db/empty-db {:t/tags {:db/cardinality :db.cardinality/many} :t/n {}} {:keep-history? true})
                          [{:db/id 1 :t/tags ["tag1" "x"] :t/n 0} {:db/id 2 :t/tags ["x"] :t/n 7}])
        after (d/db-with before [[:db/retract 1 :t/tags "x"]])]
    (doseq [[world database] {:current after :history (d/history after) :as-of (d/as-of after (:max-tx before))}
            q-form ['[:find ?e ?t :where [?e :t/tags ?t ?tx false] [?e :t/n 0]]
                    '[:find ?e ?t :where [?e :t/n 0] [?e :t/tags ?t ?tx false]]
                    '[:find ?e :where [?e :t/n ?n] (not [?e :t/tags "x" ?tx false])]]]
      (testing [world q-form]
        (binding [q/*query-result-cache?* false]
          (let [{:keys [legacy planner]} (run-both q-form database)]
            (is (= (set legacy) (set planner)))))))
    (is (= #{} (binding [q/*query-result-cache?* false]
                 (set (d/q '[:find ?e ?t :where [?e :t/n 0] [?e :t/tags ?t ?tx false]] after)))))))
