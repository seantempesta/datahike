(ns datahike.test.cljs-recursive-rule-test
  "Regression tests for the planner's recursive-rule defects (2026-07-11).

   `execute-recursive-rule`'s semi-naive fixpoint is cross-platform; three
   defects were found in one zone:

   1) CLJS gating — `delta-driven-expand` and `magic-base-scan` are
      `#?(:cljs nil)`, and the magic-set demand machinery used
      `java.util.HashSet` interop, but neither shortcut excluded CLJS:
      any simple binary transitive-closure rule whose delta dropped below
      16 tuples took the shortcut, got `nil` → empty rec-rel, and the
      fixpoint TERMINATED (a 19-edge chain returned exactly depth 5); a
      ground call-arg THREW (`demand_set.size is not a function`).
      Fix: both shortcuts platform-gated to :clj.

   2) Direction-blindness (BOTH platforms wherever the shortcuts run) —
      the shortcuts assumed the reverse recursion form
      `[?x :attr ?t] (rule ?t ?y)` (link var at the pattern VALUE). The
      equally-valid forward form `[?m :attr ?a] (rule ?m ?d)` (link at the
      pattern ENTITY — e.g. `descendant` over a parent ref) got a reverse
      expansion that only re-emitted already-seen pairs → the dedup emptied
      the delta → same truncation arithmetic; the magic base scan seeded
      the wrong side of the closure → `#{}` for ground queries — in BOTH
      ground positions (even the reverse form broke with the ground arg at
      position 1). Fix: lowering computes each clause version's join
      topology structurally (`rec-expand-info` / `base-scan-info` in
      lower.cljc); the shortcuts follow the clause's ACTUAL direction and
      the magic path only fires for topologies its demand scheme is sound
      for (`magic-direction-ok?`).

   3) Multiplicity/cross-product blowup — `hash-join` with zero common
      attrs silently degenerated to a constant-key Cartesian product, and
      rule/OR unions carried duplicate tuples that MULTIPLY through later
      joins (a 13-node plan graph manufactured 15.4M tuples from 783
      distinct pairs → pod OOM). Fix: explicit deduped `prod-rel` for the
      no-common-attrs case, `distinct-rel` at rule/OR-branch boundaries,
      and the fixpoint scopes the outer context's rels to those sharing
      vars with the rule's plans.

   On CLJ the planner is opt-in (`*force-legacy*` defaults true), so these
   tests bind it false on CLJ to exercise the same engine both platforms.
   `*fixpoint-shortcuts?*` distinguishes the JVM shortcut paths from the
   plain fixpoint (the path CLJS always runs)."
  (:require
   #?(:clj  [clojure.test :refer [is deftest testing]]
      :cljs [cljs.test :refer-macros [is deftest testing]])
   [datahike.api :as d]
   [datahike.db]
   [datahike.query]
   [datahike.query.execute :as execute]
   #?(:clj [datahike.query.relation :as rel])))

(def ^:private schema
  {:node/parent {:db/valueType :db.type/ref}
   :node/name   {:db/unique :db.unique/identity}
   :edge/to     {:db/valueType :db.type/ref}})

(def ^:private descendant-rules
  '[[(descendant ?a ?d) [?d :node/parent ?a]]
    [(descendant ?a ?d) [?m :node/parent ?a] (descendant ?m ?d)]])

(defn- chain-db
  "A db holding a single parent chain n1 <- n2 <- ... <- n<len>."
  [len]
  (-> (datahike.db/empty-db schema)
      (d/db-with (vec (for [i (range 1 (inc len))]
                        (cond-> {:db/id (- i) :node/name (str "n" i)}
                          (> i 1) (assoc :node/parent (- (dec i)))))))))

(defn- edge-db
  "A db holding a single edge chain n1 -> n2 -> ... -> n<len>."
  [len]
  (-> (datahike.db/empty-db schema)
      (d/db-with (vec (for [i (range 1 (inc len))]
                        (cond-> {:db/id (- i) :node/name (str "n" i)}
                          (< i len) (assoc :edge/to (- (inc i)))))))))

(defn- run-q
  "Planner engine, shortcuts as shipped (JVM shortcuts eligible on :clj).
   Result cache off so every run actually executes."
  [query & args]
  (binding [datahike.query/*query-result-cache?* false]
    #?(:clj  (binding [datahike.query/*force-legacy* false]
               (apply d/q query args))
       :cljs (apply d/q query args))))

(defn- run-q-plain
  "Planner engine, plain semi-naive fixpoint (shortcuts bound off).
   On CLJS this is the only path; the binding is a harmless no-op there."
  [query & args]
  (binding [execute/*fixpoint-shortcuts?* false
            datahike.query/*query-result-cache?* false]
    #?(:clj  (binding [datahike.query/*force-legacy* false]
               (apply d/q query args))
       :cljs (apply d/q query args))))

(defn- run-both
  "Run `query` through the planner with shortcuts on AND off; assert the two
   paths agree and return the (shortcuts-on) result."
  [query & args]
  (let [on (apply run-q query args)
        off (apply run-q-plain query args)]
    (is (= (set on) (set off))
        "shortcut and plain fixpoint paths must agree")
    on))

;; ---------------------------------------------------------------------------
;; Free-var closure over a short chain. Delta is < 16 from the first
;; iteration, so pre-fix CLJS took the delta-driven shortcut immediately:
;; only the 3 depth-1 pairs came back.

(deftest short-chain-full-closure
  (let [db (chain-db 4)
        pairs (run-q '[:find ?pa ?da :in $ %
                       :where
                       (descendant ?p ?d)
                       [?p :node/name ?pa]
                       [?d :node/name ?da]]
                     db descendant-rules)]
    (is (= #{["n1" "n2"] ["n1" "n3"] ["n1" "n4"]
             ["n2" "n3"] ["n2" "n4"]
             ["n3" "n4"]}
           pairs)
        "4-node chain closure must contain all 6 ancestor/descendant pairs (pre-fix CLJS: only the 3 depth-1 pairs)")))

;; ---------------------------------------------------------------------------
;; Ground call-arg — the magic-set path. Pre-fix CLJS threw
;; `demand_set.size is not a function` before returning anything.

(deftest ground-arg-descendants
  (let [db (chain-db 4)
        root (:db/id (d/entity db [:node/name "n1"]))
        names (run-q '[:find [?n ...] :in $ % ?r
                       :where (descendant ?r ?x) [?x :node/name ?n]]
                     db descendant-rules root)]
    (is (= #{"n2" "n3" "n4"} (set names))
        "ground-root descendants must reach full depth (pre-fix CLJS: TypeError in the magic-set demand machinery)")))

;; ---------------------------------------------------------------------------
;; Longer chain: the plain fixpoint runs while delta >= 16, then pre-fix
;; CLJS switched to the broken shortcut mid-closure — a 20-node chain
;; returned exactly 85 pairs (depth capped at 5) instead of 190.

(deftest long-chain-full-closure
  (let [db (chain-db 20)
        pairs (run-q '[:find ?p ?d :in $ %
                       :where (descendant ?p ?d)]
                     db descendant-rules)]
    (is (= 190 (count pairs))
        (str "20-node chain closure must contain n*(n-1)/2 = 190 pairs (pre-fix CLJS: 85 — truncated at depth 5); got "
             (count pairs)))))

;; ---------------------------------------------------------------------------
;; Direction coverage — the REVERSE form the shortcuts were written for
;; (`[?x :edge ?z] (reach ?z ?y)`, link var at the pattern VALUE). Pins the
;; optimization's own correctness: closure (short + a 20-node chain whose
;; delta crosses the <16 shortcut threshold mid-run) and BOTH ground
;; positions. Ground position 1 was broken pre-fix even for this form —
;; the magic base scan seeded the demanded node's OUT-edges instead of its
;; in-edges, returning #{} — and is now sound via the pass-through case of
;; `magic-direction-ok?`.

(def ^:private reach-rules
  '[[(reach ?x ?y) [?x :edge/to ?y]]
    [(reach ?x ?y) [?x :edge/to ?z] (reach ?z ?y)]])

(deftest reverse-form-closure-and-ground
  (let [db4 (edge-db 4)
        db20 (edge-db 20)
        root (:db/id (d/entity db20 [:node/name "n1"]))
        leaf (:db/id (d/entity db20 [:node/name "n20"]))]
    (testing "short-chain closure (delta < 16 from the first iteration)"
      (is (= 6 (count (run-both '[:find ?x ?y :in $ % :where (reach ?x ?y)]
                                db4 reach-rules)))))
    (testing "20-node chain closure (delta crosses the <16 threshold mid-run)"
      (is (= 190 (count (run-both '[:find ?x ?y :in $ % :where (reach ?x ?y)]
                                  db20 reach-rules)))))
    (testing "ground arg at position 0 — everything reachable FROM root"
      (is (= 19 (count (run-both '[:find [?y ...] :in $ % ?r :where (reach ?r ?y)]
                                 db20 reach-rules root)))))
    (testing "ground arg at position 1 — everything that reaches the leaf (pre-fix: #{})"
      (is (= 19 (count (run-both '[:find [?x ...] :in $ % ?l :where (reach ?x ?l)]
                                 db20 reach-rules leaf)))))))

;; ---------------------------------------------------------------------------
;; Direction coverage — the FORWARD form (`[?m :parent ?a] (descendant ?m ?d)`,
;; link var at the pattern ENTITY): the shape the shortcuts silently broke.
;; The free-var closure + ground-position-0 cases are the inherited tests
;; above; this adds ground position 1 (ancestors of the leaf, pre-fix: #{}).

(deftest forward-form-ground-pos1
  (let [db (chain-db 20)
        leaf (:db/id (d/entity db [:node/name "n20"]))]
    (is (= 19 (count (run-both '[:find [?a ...] :in $ % ?l :where (descendant ?a ?l)]
                               db descendant-rules leaf)))
        "ancestors of the leaf must reach full depth (pre-fix planner: #{})")))

;; ---------------------------------------------------------------------------
;; The my.plan `ready` shape (copied from the seon consumer that surfaced the
;; bugs — deeply nested OR / NOT / not-join over the forward-form `descendant`
;; closure). Pre-fix, the JVM planner with shortcuts on returned root as ready
;; despite open work under it; the CLJS executor manufactured 15.4M duplicate
;; tuples from 783 distinct pairs evaluating it on a grown store (pod OOM).

(def ^:private plan-schema
  {:my.plan/parent {:db/valueType :db.type/ref}
   :my.plan/agent  {:db/valueType :db.type/ref}
   :my.plan/needs  {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :my.plan/id     {:db/unique :db.unique/identity}
   :my.plan/status {}
   :my.plan/title  {}})

(def ^:private plan-rules
  '[[(descendant ?a ?n) [?n :my.plan/parent ?a]]
    [(descendant ?a ?n) [?m :my.plan/parent ?a] (descendant ?m ?n)]
    [(leaf ?t) (not-join [?t] [?c :my.plan/parent ?t])]
    [(unfinished ?t) [?t :my.plan/status :open]]
    [(unfinished ?t) [?t :my.plan/status :active]]
    [(unfinished ?t) [?t :my.plan/status :blocked]]
    [(open-work ?t) (unfinished ?t) (leaf ?t)]
    [(open-work ?t) (descendant ?t ?l) (unfinished ?l) (leaf ?l)]
    [(blocked ?t) [?t :my.plan/status :blocked]]
    [(blocked ?t) [?t :my.plan/needs ?d] (open-work ?d)]
    [(ready ?t) [?t :my.plan/status :open] (leaf ?t) (not (blocked ?t))]
    [(ready ?t) [?t :my.plan/status :open] (not (leaf ?t))
     (not (open-work ?t)) (not (blocked ?t))]])

(def ^:private ready-query
  '[:find [?id ...]
    :in $ % ?a
    :where
    [?t :my.plan/agent ?a]
    (ready ?t)
    [?t :my.plan/id ?id]])

(defn- plan-node [id status parent]
  (cond-> {:db/id -2 :my.plan/id id :my.plan/title id
           :my.plan/agent [:my.plan/id "agent"]}
    status (assoc :my.plan/status status)
    parent (assoc :my.plan/parent [:my.plan/id parent])))

(defn- plan-db
  "In-memory db with an agent entity + the given [id status parent-id] rows
   (parents transacted first — rows reference them by ident)."
  [rows]
  (reduce (fn [db [id status parent]]
            (d/db-with db [(plan-node id status parent)]))
          (-> (datahike.db/empty-db plan-schema)
              (d/db-with [{:db/id -1 :my.plan/id "agent" :my.plan/title "agent"}]))
          rows))

(deftest ready-planner-correct
  ;; 5-node repro tree: root -> (a b), a -> (a1 a2); only a1 is ready.
  ;; Pre-fix the JVM planner with shortcuts on returned #{"a1" "root"}.
  (let [db (plan-db [["root" :open nil]
                     ["a" :open "root"] ["b" :done "root"]
                     ["a1" :open "a"] ["a2" :done "a"]])
        agent (:db/id (d/entity db [:my.plan/id "agent"]))]
    (is (= #{"a1"} (set (run-both ready-query db plan-rules agent)))
        "root has open work under it and must NOT be ready")))

(deftest ready-planner-richer-tree
  ;; 13-node tree exercising every rule: drained non-leaf (b), blocked leaf
  ;; (c1), :active steps (d, a3), open leaves (a1, d1).
  (let [db (plan-db [["root" :open nil]
                     ["a" :open "root"] ["b" :open "root"]
                     ["c" :open "root"] ["d" :active "root"]
                     ["a1" :open "a"] ["a2" :done "a"] ["a3" :active "a"]
                     ["b1" :done "b"] ["b2" :done "b"]
                     ["c1" :blocked "c"]
                     ["d1" :open "d"] ["d2" :done "d"]])
        agent (:db/id (d/entity db [:my.plan/id "agent"]))]
    (is (= #{"a1" "d1" "b"} (set (run-both ready-query db plan-rules agent)))
        "open unblocked leaves + the drained non-leaf, nothing else")))

;; ---------------------------------------------------------------------------
;; Bounded intermediates — the OOM regression. The 13-node tree above, with
;; every relation flowing through `rel/hash-join` measured. Pre-fix the
;; multiplicity leak manufactured tuple counts orders of magnitude beyond
;; the data (15.4M from 783 distinct pairs on the live store); post-fix
;; nothing may exceed a few hundred tuples for a 13-node graph. JVM-only
;; instrumentation (the CLJS suite compiles :advanced — no with-redefs),
;; run over the PLAIN fixpoint path, which is exactly the path CLJS executes
;; (the .cljc executor is shared; CLJS never takes the shortcut paths).

#?(:clj
   (deftest ready-bounded-intermediates
     (let [db (plan-db [["root" :open nil]
                        ["a" :open "root"] ["b" :open "root"]
                        ["c" :open "root"] ["d" :active "root"]
                        ["a1" :open "a"] ["a2" :done "a"] ["a3" :active "a"]
                        ["b1" :done "b"] ["b2" :done "b"]
                        ["c1" :blocked "c"]
                        ["d1" :open "d"] ["d2" :done "d"]])
           agent (:db/id (d/entity db [:my.plan/id "agent"]))
           max-tuples (atom 0)
           orig-hash-join rel/hash-join]
       (with-redefs [rel/hash-join (fn [r1 r2]
                                     (let [out (orig-hash-join r1 r2)]
                                       (swap! max-tuples max
                                              (count (:tuples r1))
                                              (count (:tuples r2))
                                              (count (:tuples out)))
                                       out))]
         (is (= #{"a1" "d1" "b"} (set (run-q-plain ready-query db plan-rules agent))))
         (is (< @max-tuples 500)
             (str "intermediate relations must stay bounded by the data; "
                  "largest seen: " @max-tuples " tuples"))))))

(comment
  ;; CLJ REPL:
  (clojure.test/run-tests 'datahike.test.cljs-recursive-rule-test)
  ;; CLJS: wired into datahike.test.nodejs-test → `bb node-cljs-test`
  )
