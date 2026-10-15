(ns datahike.index.node-cache
  "Byte-bounded retention of decoded index nodes. Weights estimate heap bytes,
   including datom payloads, without following children into other nodes."
  (:require [datahike.datom])
  (:import [com.github.benmanes.caffeine.cache Cache Caffeine Weigher]
           [datahike.datom Datom]
           [java.lang.reflect Field]
           [org.replikativ.persistent_sorted_set ANode Branch Slot]))

(def ^:private ^:const cache-entry-bytes 200)
(def ^:private ^:const margin 1.05)

(defn- pad8 ^long [^long n] (bit-and (+ n 7) (bit-not 7)))

(defn- latin1? [^String s]
  (let [n (.length s)]
    (loop [i 0] (cond (== i n) true (> (int (.charAt s i)) 255) false :else (recur (inc i))))))

(defn- string-bytes
  "A String object plus its byte[] (one byte per char when Latin-1, else two)."
  ^long [^String s]
  (let [n (.length s)] (+ 40 (pad8 (if (latin1? s) n (* 2 n))))))

(declare value-bytes)

(defn- sum-bytes ^long [coll]
  (reduce (fn [^long n v] (+ n (value-bytes v))) 0 coll))

(defn- value-bytes
  "Heap bytes a decoded datom value retains: calibrated on measured object graphs
   (JOL, JDK 26, compressed oops). Interned keywords are shared and cost nothing."
  ^long [value]
  (cond
    (nil? value) 0
    (string? value) (string-bytes value)
    (keyword? value) 0
    (instance? Boolean value) 0
    (instance? Number value) (if (or (instance? Long value) (instance? Double value) (instance? Integer value)) 24 96)
    (symbol? value) (let [n (namespace value)]
                      (+ 32 (string-bytes (name value))
                         (if n (+ (string-bytes n) (string-bytes (str n "/" (name value)))) 0)))
    (instance? java.util.UUID value) 32
    (instance? java.util.Date value) 24
    (instance? Datom value) (+ 40 (value-bytes (:a value)) (value-bytes (:v value)))
    (vector? value) (+ 256 (* 8 (count value)) (sum-bytes value))
    (map? value) (reduce-kv (fn [^long n k v] (+ n 64 (value-bytes k) (value-bytes v))) 128 value)
    (coll? value) (+ 128 (* 64 (count value)) (sum-bytes value))
    (bytes? value) (+ 16 (pad8 (alength ^bytes value)))
    :else 128))

(defn- field [^Class cls ^String name]
  (doto (.getDeclaredField cls name) (.setAccessible true)))

(def ^:private ^Field state-field (field Branch "_state"))
(def ^:private ^Field buf-field (field (Class/forName "org.replikativ.persistent_sorted_set.Branch$NodeState") "buf"))
(def ^:private ^Field slots-field (field (Class/forName "org.replikativ.persistent_sorted_set.Branch$BufState") "slots"))
(def ^:private ^Field diff-field (field Slot "diff"))

(defn- keys-bytes
  "The key array and what its elements retain."
  ^long [^ANode node]
  (let [^objects ks (.-_keys node)
        n (.-_len node)]
    (loop [i 0 total (+ 16 (pad8 (* 4 (alength ks))))]
      (if (< i n) (recur (inc i) (+ total (value-bytes (aget ks i)))) total))))

(defn- branch-bytes
  "Own fields, keys, one address and one weak child reference per child, and the diff-buf slots."
  ^long [^Branch node]
  (let [n (.-_len node)
        state (.get state-field node)
        buf (when state (.get buf-field state))
        ^objects slots (when buf (.get slots-field buf))
        slot-bytes (if slots
                     (areduce slots i total 16
                              (let [s (aget slots i)]
                                (if s (+ total 56 (value-bytes (.get diff-field s))) total)))
                     0)]
    (+ 96 (keys-bytes node)
       (* n (+ 32 4 48 4))
       slot-bytes)))

(defn node-bytes
  "Estimated heap bytes a cached node retains, from its keys and values without
   following children into other nodes. Over-estimates by design: `margin` covers
   what the per-type constants cannot see."
  [node]
  (long (* margin
           (+ cache-entry-bytes
              (if (instance? Branch node)
                (branch-bytes node)
                (+ 32 (keys-bytes node)))))))

(defn cache? [value] (instance? Cache value))

(defn create [max-bytes]
  (-> (Caffeine/newBuilder)
      (.maximumWeight (long max-bytes))
      (.weigher (reify Weigher
                  (weigh [_ _ node] (int (min Integer/MAX_VALUE (node-bytes node))))))
      (.recordStats)
      (.build)))

(defn lookup [^Cache cache address] (.getIfPresent cache address))
(defn put! [^Cache cache address node] (.put cache address node))
(defn evict! [^Cache cache address] (.invalidate cache address))

(defn stats [^Cache cache]
  (.cleanUp cache)
  (let [eviction (.get (.eviction (.policy cache)))
        counters (.stats cache)]
    {:nodes (.estimatedSize cache)
     :max-bytes (.getMaximum eviction)
     :weighted-bytes (.orElse (.weightedSize eviction) 0)
     :hits (.hitCount counters) :misses (.missCount counters)
     :evictions (.evictionCount counters)}))
