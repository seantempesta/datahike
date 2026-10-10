(ns datahike.index.node-cache
  "Byte-bounded retention of decoded index nodes. Weights estimate heap bytes,
   including datom payloads, without following children into other nodes."
  (:require [datahike.datom]
            [org.replikativ.persistent-sorted-set.fressian :as fressian])
  (:import [com.github.benmanes.caffeine.cache Cache Caffeine Weigher]
           [datahike.datom Datom]))

(defn- value-bytes [value]
  (cond
    (nil? value) 0
    (string? value) (+ 40 (* 2 (count value)))
    (or (symbol? value) (keyword? value)) (+ 80 (* 2 (+ (count (name value)) (count (namespace value)))))
    (instance? Datom value) (+ 48 (value-bytes (:a value)) (value-bytes (:v value)))
    (map? value) (reduce-kv (fn [n k v] (+ n 48 (value-bytes k) (value-bytes v))) 48 value)
    (coll? value) (reduce (fn [n v] (+ n 16 (value-bytes v))) 32 value)
    (bytes? value) (+ 24 (alength ^bytes value))
    :else 32))

(defn node-bytes
  "Conservative decoded-content estimate, counting shared payloads per node.
   The content projection excludes child references, storage and comparators."
  [node]
  (long (+ 256 (value-bytes (fressian/node->map node)))))

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
