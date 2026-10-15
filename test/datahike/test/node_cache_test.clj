(ns datahike.test.node-cache-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.index.interface :as index]
            [datahike.index.node-cache]
            [datahike.index.persistent-set]
            [datahike.datom :as datom]
            [datahike.api :as d]
            [konserve.memory :as memory]
            [org.replikativ.persistent-sorted-set :as pss])
  (:import [org.replikativ.persistent_sorted_set Leaf Settings RefType]))

(deftest a-byte-budget-keeps-small-nodes-and-refuses-an-oversize-node
  (let [cache (index/make-node-cache {:store-cache-size 1 :store-cache-bytes 100000})
        small (Leaf. [(datom/datom 1 :value "small" 1)] (Settings. 128 RefType/WEAK))
        large (Leaf. [(datom/datom 2 :value (apply str (repeat 100000 "x")) 1)] (Settings. 128 RefType/WEAK))]
    (doseq [k (range 10)] (index/cache-put! cache k small))
    (is (= 10 (count (filter #(index/cache-get cache %) (range 10)))))
    (index/cache-put! cache :large large)
    (index/node-cache-stats cache)
    (is (nil? (index/cache-get cache :large)))
    (is (<= (:weighted-bytes (index/node-cache-stats cache)) 100000))))

(deftest byte-cached-trees-use-weak-references
  (let [raw (memory/new-mem-store (atom {}) {:sync? true})
        cfg {:index :datahike.index/persistent-set :store-cache-size 10 :store-cache-bytes 100000
             :index-config {:branching-factor 128}}
        store (index/add-konserve-handlers cfg raw)
        tree (index/empty-index (:index cfg) store :eavt nil)]
    (is (= :weak (:ref-type (pss/settings tree))))))

(deftest a-byte-cache-is-shared-and-restored-nodes-are-weak
  (let [directory (.toFile (java.nio.file.Files/createTempDirectory "datahike-node-cache-" (make-array java.nio.file.attribute.FileAttribute 0)))
        cfg {:store {:backend :file :path (.getPath (java.io.File. directory "store")) :id (random-uuid)}
             :schema-flexibility :read :store-cache-bytes 1000000
             :index-config {:branching-factor 32}}
        _ (d/create-database cfg)]
    (try
      (let [conn (d/connect cfg)]
        (try
          (d/transact conn (mapv (fn [n] {:db/id (inc n) :value n}) (range 300)))
          (d/branch! conn :db :sibling)
          (let [sibling (d/connect (assoc cfg :branch :sibling))]
            (try
              (is (identical? (-> @conn :store :storage :cache)
                              (-> @sibling :store :storage :cache)))
              (finally (d/release sibling))))
          (finally (d/release conn))))
      (let [conn (d/connect cfg)]
        (try
          (is (= 300 (d/q '[:find (count ?e) . :where [?e :value]] @conn)))
          (is (= :weak (:ref-type (pss/settings (:eavt @conn)))))
          (let [storage (-> @conn :store :storage)
                cache (:cache storage)
                nodes (vals (.asMap ^com.github.benmanes.caffeine.cache.Cache cache))]
            (is (seq nodes))
            (is (every? #(= RefType/WEAK (.refType (.-_settings ^org.replikativ.persistent_sorted_set.ANode %))) nodes))
            (is (<= (:weighted-bytes (index/node-cache-stats cache)) 1000000)))
          (finally (d/release conn))))
      (finally
        (d/delete-database cfg)
        (doseq [file (reverse (file-seq directory))] (.delete file))))))

(defn- live-bytes []
  (dotimes [_ 4] (System/gc) (Thread/sleep 100))
  (let [r (Runtime/getRuntime)] (- (.totalMemory r) (.freeMemory r))))

(defn- generated-leaf [^java.util.Random rnd settings value-kind]
  (Leaf. (vec (for [e (range 100)]
                (datom/datom e :value
                             (let [n (.nextInt rnd 1000000)]
                               (case value-kind
                                 :string (apply str (repeat (+ 1 (.nextInt rnd 300)) (char (+ 97 (.nextInt rnd 26)))))
                                 :wide-string (apply str (repeat (+ 1 (.nextInt rnd 300)) (char (+ 1000 (.nextInt rnd 26)))))
                                 :symbol (symbol (str "ns" (.nextInt rnd 1000)) (str "name-" n))
                                 :long (+ n 1000000000)
                                 :uuid (java.util.UUID. (.nextLong rnd) (.nextLong rnd))
                                 :date (java.util.Date. (.nextLong rnd))
                                 :vector [(str "s" n) (+ n 1000000000) (symbol (str "v" n))]))
                             1)))
         settings))

(defn- held-bytes
  "Heap bytes retained by exactly the nodes `make` builds: live heap with them held minus live
   heap right after releasing them, so unrelated garbage of earlier tests cancels. Median of 5."
  [make]
  (let [holder (atom nil)
        one (fn []
              (reset! holder (make))
              (let [held (live-bytes)]
                (reset! holder nil)
                (- held (live-bytes))))
        xs (sort (repeatedly 5 one))]
    (nth xs 2)))

(deftest weighed-bytes-cover-real-retained-bytes-and-track-them
  (let [settings (Settings. 128 RefType/WEAK)]
    (doseq [kind [:string :wide-string :symbol :long :uuid :date :vector]]
      (let [weighed (atom 0)
            real (held-bytes (fn []
                               (let [rnd (java.util.Random. 7)
                                     nodes (vec (repeatedly 300 #(generated-leaf rnd settings kind)))]
                                 (reset! weighed (reduce + (map @#'datahike.index.node-cache/node-bytes nodes)))
                                 nodes)))]
        (is (<= real @weighed) (str kind " weighed under real"))
        (is (<= @weighed (* 3 real)) (str kind " weighed loosely"))))))
