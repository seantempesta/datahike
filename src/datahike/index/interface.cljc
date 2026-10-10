(ns datahike.index.interface
  "All the functions in this namespace must be implemented for each index type"
  #?(:cljs (:refer-clojure :exclude [-seq -count -persistent! -flush -lookup]))
  (:require #?(:clj [clojure.core.cache :as cache]
               :cljs [cljs.cache :as cache])
            #?(:clj [datahike.index.node-cache :as nodes])))

(def node-cache-key
  "Key under which a connection hands its storage the node cache it shares with
   every other connection to the same physical store (upstream d2b9e525). Read
   by the index implementation when it builds storage; absent on paths with no
   connection behind them, which get a private cache."
  ::node-cache)

(defn make-node-cache
  "A shared node cache; a byte budget takes precedence over the legacy node count."
  [configuration]
  (let [config (if (map? configuration) configuration {:store-cache-size configuration})]
    (if-let [max-bytes (:store-cache-bytes config)]
      #?(:clj (nodes/create max-bytes)
         :cljs (throw (ex-info "Byte-bounded node caching requires the JVM." {:store-cache-bytes max-bytes})))
      (atom (cache/lru-cache-factory {} :threshold (:store-cache-size config))))))

(defn cache-get [held address]
  #?(:clj (if (nodes/cache? held) (nodes/lookup held address)
              (cache/lookup @held address))
     :cljs (cache/lookup @held address)))

(defn cache-put! [held address node]
  #?(:clj (if (nodes/cache? held) (nodes/put! held address node)
              (swap! held cache/miss address node))
     :cljs (swap! held cache/miss address node))
  nil)

(defn cache-hit! [held address]
  #?(:clj (if (nodes/cache? held) (nodes/lookup held address) (swap! held cache/hit address))
     :cljs (swap! held cache/hit address))
  nil)

(defn cache-evict! [held address]
  #?(:clj (if (nodes/cache? held) (nodes/evict! held address)
              (swap! held cache/evict address))
     :cljs (swap! held cache/evict address))
  nil)

(defn node-cache-stats [held]
  #?(:clj (if (nodes/cache? held) (nodes/stats held) {:nodes (count @held)})
     :cljs {:nodes (count @held)}))

(defprotocol IIndex
  (-all [index] "Returns a sequence of all datoms in the index")
  (-seq [index] "Returns a sequence of all datoms in the index")
  (-count [index] "Returns the number of datoms in the index")
  (-insert [index datom index-type op-count] "Inserts a datom into the index")
  (-temporal-insert [index datom index-type op-count] "Inserts a datom in a history index")
  (-upsert [index datom index-type op-count old-datom] "Inserts or updates a datom into the index")
  (-temporal-upsert [index datom index-type op-count old-datom] "Inserts or updates a datom in a history index")
  (-remove [index datom index-type op-count] "Removes a datom from the index")
  (-slice [index from to index-type] "Returns a slice of the index")
  (-rslice [index from to index-type] "Returns a REVERSE slice of the index: a lazy backwards iterator over datoms d with to <= d <= from, starting at `from` and descending. Mirrors persistent-sorted-set's rslice argument order (from = upper bound).")
  (-lookup [index key cmp] "Look up a single key with custom comparator. Returns the stored element or nil.")
  (-count-slice [index from to cmp] "O(log n) count of elements in [from, to] range using the given comparator.")
  (-has-subtree-counts? [index] "Returns true if count-slice is O(log n). False means counts are missing and count-slice would degrade to O(n).")
  (-flush [index backend] "Saves the changes to the index to the given konserve backend")
  (-transient [index] "Returns a transient version of the index")
  (-persistent! [index] "Returns a persistent version of the index")
  (-mark [index] [index seen] "Return konserve addresses that should be whitelisted for mark and sweep gc. With `seen`, an atom of addresses already marked, return only the unseen ones and add them: a seen node's subtree is not walked again.")
  (-root-node [index] "Returns the in-memory root node of a flushed index, for root fusion (inlining the root into the db-record).")
  (-seed-root! [index root-node] "Seeds the in-memory root node after restoring a db-record that inlined it (root fusion). MUTATES the index — call it only on an OWNED, unpublished copy (e.g. the with-storage copy made at attach), never on a stored record's index: records may be shared through the store's cache by every reader of that key. Returns the index."))

(defmulti empty-index
  "Creates an empty index"
  (fn [index-name _store _index-type _index-config]
    index-name))

(defmulti init-index
  "Creates an index with datoms"
  (fn [index-name _store _datoms _index-type _op-count _index-config]
    index-name))

(defmulti add-konserve-handlers
  "Adds read and write handlers for the index data types."
  (fn [config _store] (:index config)))

(defmulti konserve-backend
  "Returns a konserve store capable of handling the index. Used for flushing."
  (fn [index-name _store] index-name))

(defmulti default-index-config
  "Returns the default index configuration."
  (fn [index-name] index-name))

(defmulti with-storage
  "Return `index` bound to `storage` as a shallow copy sharing the
   (immutable) node tree. Storage is connection-scoped context, not part
   of the index value: bind an index to the live connection's storage
   when materializing it from a store, and detach it (storage nil) before
   writing it into a store, so a stored value never carries a foreign
   storage handle — even through identity-preserving stores that skip
   serialization (e.g. a tiered memory frontend). Never mutates the
   input; returns the index unchanged for index types without embedded
   storage and for nil."
  (fn [index-name _index _storage] index-name))

(defmethod with-storage :default [_index-name index _storage] index)

;; Default handlers for missing index implementations

(defn- hitchhiker-tree-missing-error []
  (ex-info
   "Hitchhiker-tree index requires explicit setup:
   1. Add io.replikativ/hitchhiker-tree to your deps.edn
   2. Require datahike.index.hitchhiker-tree in your namespace
   Or use the default :datahike.index/persistent-set index."
   {:type :missing-index-implementation
    :index :datahike.index/hitchhiker-tree
    :available-indexes (disj (set (keys (methods empty-index))) :default)}))

(defmethod empty-index :default [index-name _ _ _]
  (if (= index-name :datahike.index/hitchhiker-tree)
    (throw (hitchhiker-tree-missing-error))
    (throw (ex-info (str "Unknown index type: " index-name)
                    {:type :unknown-index-type
                     :index index-name
                     :available-indexes (disj (set (keys (methods empty-index))) :default)}))))

(defmethod init-index :default [index-name _ _ _ _ _]
  (if (= index-name :datahike.index/hitchhiker-tree)
    (throw (hitchhiker-tree-missing-error))
    (throw (ex-info (str "Unknown index type: " index-name)
                    {:type :unknown-index-type
                     :index index-name
                     :available-indexes (disj (set (keys (methods init-index))) :default)}))))

(defmethod add-konserve-handlers :default [config _]
  (let [index-name (:index config)]
    (if (= index-name :datahike.index/hitchhiker-tree)
      (throw (hitchhiker-tree-missing-error))
      (throw (ex-info (str "Unknown index type: " index-name)
                      {:type :unknown-index-type
                       :index index-name
                       :available-indexes (disj (set (keys (methods add-konserve-handlers))) :default)})))))

(defmethod konserve-backend :default [index-name _]
  (if (= index-name :datahike.index/hitchhiker-tree)
    (throw (hitchhiker-tree-missing-error))
    (throw (ex-info (str "Unknown index type: " index-name)
                    {:type :unknown-index-type
                     :index index-name
                     :available-indexes (disj (set (keys (methods konserve-backend))) :default)}))))

(defmethod default-index-config :default [index-name]
  (if (= index-name :datahike.index/hitchhiker-tree)
    (throw (hitchhiker-tree-missing-error))
    (throw (ex-info (str "Unknown index type: " index-name)
                    {:type :unknown-index-type
                     :index index-name
                     :available-indexes (disj (set (keys (methods default-index-config))) :default)}))))
