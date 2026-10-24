(ns datahike.reads
  "What a read of a database scanned, and whether a transaction touched it. Every q, pull,
   datoms and entity read reaches an index through one protocol (datahike.index.interface),
   which records each range it is asked for while with-reads runs: [from to], two datoms
   whose nil components (and the e0/emax entity sentinels) are open. A range's bound prefix
   is its key ([:e e] [:ea e a] [:a a] [:av a v], or :any), and a datom touches a read when
   one of its own keys is among the read's (datom-keys). Matching a transaction costs its
   datoms, whatever was read."
  (:require [datahike.index.interface :as di]
            [datahike.constants :refer [e0 emax]]
            [datahike.query :as q]))

(defn with-reads
  "Call f (no arguments) and answer [result reads]: reads, the ranges f's reads scanned. While
   recording, queries run on the relational engine (the planner's cursors read past the index
   protocol) with no result cache, and the search memo is bypassed, so every read reaches an
   index. f's result should be realized: a lazy read after it returns is not recorded."
  [f]
  (let [r (atom [])
        result (binding [di/*reads* r
                         q/*disable-planner* true
                         q/*query-result-cache?* false]
                 (f))]
    [result @r]))

(defn- bound
  "A range component's bound value: x when from and to agree on it and it is not open."
  [x y open?]
  (when (and (some? x) (= x y) (not (open? x))) x))

(defn- range-key
  "Range [from to]'s key: its most specific bound prefix."
  [[from to]]
  (if (or (nil? from) (nil? to))
    :any
    (let [e (bound (:e from) (:e to) #(or (= e0 %) (= emax %)))
          a (bound (:a from) (:a to) (constantly false))
          v (bound (:v from) (:v to) (constantly false))]
      (cond (and e a) [:ea e a]
            e [:e e]
            (and a (some? v)) [:av a v]
            a [:a a]
            :else :any))))

(defn reads-index
  "The set of keys reads touch (range-key each): built once a read, looked up per datom."
  [reads]
  (into #{} (map range-key) reads))

(def ^:private schema-attrs
  "Attributes whose datoms change the schema: a read is read through the schema, so they touch every read."
  #{:db/ident :db/valueType :db/cardinality :db/unique :db/index :db/isComponent :db/noHistory
    :db/tupleAttrs :db/tupleType :db/tupleTypes})

(defn datom-keys
  "The keys datom d touches: its entity, its entity and attribute, its attribute, its attribute
   and value, and :any; a schema datom touches every read (:all)."
  [d]
  (let [e (:e d) a (:a d)]
    (if (schema-attrs a)
      [:all]
      [[:e e] [:ea e a] [:a a] [:av a (:v d)] :any])))

(defn reads-touched?
  "Does a datom of tx-data (added or retracted: both change what a read answers) fall in a read
   of idx (reads-index)? One lookup a key a datom."
  [idx tx-data]
  (boolean (some (fn [d] (some #(or (= :all %) (contains? idx %)) (datom-keys d))) tx-data)))
