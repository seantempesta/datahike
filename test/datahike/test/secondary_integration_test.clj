(ns datahike.test.secondary-integration-test
  "Integration tests for Proximum and Scriptum secondary index implementations."
  (:require
   [clojure.test :refer [deftest testing is]]
   [clojure.core.async :as async]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [datahike.index.secondary.scriptum]
   [datahike.index.secondary.stratum]
   [datahike.versioning :as dv]
   [konserve.core :as k]))

;; Proximum requires Java 22+ (class file version 66.0).
;; Load lazily so the test file compiles on older JVMs.
(def ^:private proximum-available?
  (try
    (require 'datahike.index.secondary.proximum)
    true
    (catch Throwable _ false)))

(def ^:private detached-branch-side-effects (atom []))

(defn- delete-tree!
  [path]
  (let [root (java.io.File. path)]
    (when (.exists root)
      (doseq [file (reverse (file-seq root))]
        (java.nio.file.Files/deleteIfExists (.toPath file))))))

(defn- prox-call
  [sym & args]
  (apply (requiring-resolve sym) args))

(defn- await-prox
  [channel]
  (let [result (async/<!! channel)]
    (if (instance? Throwable result)
      (throw result)
      result)))

(defn- nearest-proximum-eids
  [db vector]
  (let [index (get-in db [:secondary-indices :idx/vectors])]
    (set (es/entity-bitset-seq
          (sec/-search index {:vector (float-array vector) :k 1} nil)))))

(defmethod sec/branch-from-key-map ::side-effect
  [key-map _store _from-branch new-branch]
  (swap! detached-branch-side-effects conj new-branch)
  (assoc key-map :branch new-branch))

;; ---------------------------------------------------------------------------
;; Proximum (Vector Search) Tests

(deftest test-proximum-lifecycle
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (testing "create, insert, search, delete"
      (let [idx (sec/create-index :proximum
                                  {:attrs #{:person/embedding}
                                   :dim 4 :distance :cosine
                                   :store-config {:backend :memory :id (random-uuid)}}
                                  nil)]
        (is (= #{:person/embedding} (sec/-indexed-attrs idx)))

      ;; Insert 3 vectors via -transact
        (let [d1 (datahike.datom/datom 1 :person/embedding (float-array [1.0 0.0 0.0 0.0]))
              d2 (datahike.datom/datom 2 :person/embedding (float-array [0.0 1.0 0.0 0.0]))
              d3 (datahike.datom/datom 3 :person/embedding (float-array [0.7 0.7 0.0 0.0]))
              idx (-> idx
                      (sec/-transact {:datom d1 :added? true})
                      (sec/-transact {:datom d2 :added? true})
                      (sec/-transact {:datom d3 :added? true}))]

        ;; Estimate
          (is (= 2 (sec/-estimate idx {:k 2})))
          (is (= 3 (sec/-estimate idx {:k 10})))

        ;; Search: all 3 entities returned
          (let [results (sec/-search idx {:vector (float-array [1.0 0.0 0.0 0.0]) :k 3} nil)]
            (is (= 3 (es/entity-bitset-cardinality results)))
            (is (= #{1 2 3} (set (es/entity-bitset-seq results)))))

        ;; Search with entity filter
          (let [filter-bs (es/entity-bitset-from-longs [1 3])
                results (sec/-search idx {:vector (float-array [1.0 0.0 0.0 0.0]) :k 3} filter-bs)]
            (is (= #{1 3} (set (es/entity-bitset-seq results)))))

        ;; Ordered results (by distance ascending)
          (is (sec/-can-order? idx :person/embedding :asc))
          (is (not (sec/-can-order? idx :person/embedding :desc)))
          (let [ordered (sec/-slice-ordered idx {:vector (float-array [1.0 0.0 0.0 0.0]) :k 3}
                                            nil nil :asc nil)]
            (is (= 3 (count ordered)))
            (is (= 1 (:entity-id (first ordered)))) ;; closest
            (is (< (:distance (first ordered)) (:distance (second ordered)))))

        ;; Delete entity 2
          (let [idx-del (sec/-transact idx {:datom d2 :added? false})
                results (sec/-search idx-del {:vector (float-array [0.0 1.0 0.0 0.0]) :k 3} nil)]
            (is (not (es/entity-bitset-contains? results 2)))
            (is (= 2 (es/entity-bitset-cardinality results))))

        ;; Non-vector value is silently skipped
          (let [d-str (datahike.datom/datom 4 :person/embedding "not-a-vector")
                idx2 (sec/-transact idx {:datom d-str :added? true})
                results (sec/-search idx2 {:vector (float-array [1.0 0.0 0.0 0.0]) :k 10} nil)]
            (is (= 3 (es/entity-bitset-cardinality results)))))))))

(deftest historical-branch-restores-selected-proximum-commit
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (let [prox-path (str (System/getProperty "java.io.tmpdir")
                         "/datahike-proximum-branch-" (random-uuid))
          prox-store {:backend :file :path prox-path :id (random-uuid)}
          cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :read
               :keep-history? true}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn
                    [{:db/ident :idx/vectors
                      :db.secondary/type :proximum
                      :db.secondary/attrs [:person/embedding]
                      :db.secondary/config {:dim 4
                                            :distance :cosine
                                            :store-config prox-store}
                      :db.secondary/status :ready}])
        (d/transact conn [{:db/id 1
                           :person/embedding [1.0 0.0 0.0 0.0]}])
        (let [historical-cid (dv/commit-id @conn)]
          ;; Move entity 1 away from the query and put entity 2 at its old
          ;; position. A historical branch that accidentally reuses the live
          ;; HNSW head will now return the wrong nearest entity, not merely an
          ;; extra member hidden by an unordered set assertion.
          (d/transact conn [{:db/id 1
                             :person/embedding [0.0 1.0 0.0 0.0]}
                            {:db/id 2
                             :person/embedding [1.0 0.0 0.0 0.0]}])
          (let [head-index (get-in @conn [:secondary-indices :idx/vectors])]
            (is (= #{2}
                   (set (es/entity-bitset-seq
                         (sec/-search head-index
                                      {:vector (float-array [1.0 0.0 0.0 0.0])
                                       :k 1}
                                      nil))))))
          ;; Compatibility proof for commits written before Datahike persisted
          ;; :mmap-dir in the key map: the file store path deterministically
          ;; recovers the same store-local mmap directory.
          (k/update (:store @conn) historical-cid
                    #(update-in % [:secondary-index-keys :idx/vectors]
                                dissoc :mmap-dir)
                    {:sync? true})
          (dv/branch! conn historical-cid :historical)
          (let [historical (d/connect (assoc cfg :branch :historical))]
            (try
              (testing "primary and Proximum both stop at the selected commit"
                (is (= 1
                       (d/q '[:find (count ?e) .
                              :where [?e :person/embedding _]]
                            @historical)))
                (let [historical-index (get-in @historical
                                               [:secondary-indices :idx/vectors])]
                  (is (= #{1}
                         (set (es/entity-bitset-seq
                               (sec/-search historical-index
                                            {:vector (float-array [1.0 0.0 0.0 0.0])
                                             :k 1}
                                            nil)))))))
              (testing "branching the stored commit does not mutate the attached head"
                (let [head-index (get-in @conn [:secondary-indices :idx/vectors])]
                  (is (= #{2}
                         (set (es/entity-bitset-seq
                               (sec/-search head-index
                                            {:vector (float-array [1.0 0.0 0.0 0.0])
                                             :k 1}
                                            nil)))))))
              (finally
                (d/release historical)))))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(deftest guarded-force-replaces-the-exact-proximum-generation
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (let [prox-path (str (System/getProperty "java.io.tmpdir")
                         "/datahike-proximum-force-" (random-uuid))
          prox-store {:backend :file :path prox-path :id (random-uuid)}
          cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :read
               :keep-history? true}
          _ (d/create-database cfg)
          main (d/connect cfg)
          main-released? (atom false)]
      (try
        (d/transact main
                    [{:db/ident :idx/vectors
                      :db.secondary/type :proximum
                      :db.secondary/attrs [:person/embedding]
                      :db.secondary/config {:dim 4
                                            :capacity 64
                                            :distance :cosine
                                            :store-config prox-store}
                      :db.secondary/status :ready}])
        (d/transact main [{:db/id 1
                           :person/embedding [1.0 0.0 0.0 0.0]}])
        (dv/branch! main :db :prepared)
        (let [prepared (d/connect (assoc cfg :branch :prepared))
              prepared-released? (atom false)]
          (try
            (d/transact prepared [{:db/id 3
                                   :person/embedding [0.0 0.0 1.0 0.0]}])
            (d/transact main [{:db/id 1
                               :person/embedding [0.0 1.0 0.0 0.0]}
                              {:db/id 2
                               :person/embedding [1.0 0.0 0.0 0.0]}])
            (is (= #{1} (nearest-proximum-eids @prepared [1.0 0.0 0.0 0.0])))
            (is (= #{2} (nearest-proximum-eids @main [1.0 0.0 0.0 0.0])))
            (let [store (:store @main)
                  expected-main (dv/commit-id @main)
                  prepared-key-map (get-in (k/get store :prepared nil {:sync? true})
                                           [:secondary-index-keys :idx/vectors])
                  current-key-map (get-in (k/get store :db nil {:sync? true})
                                          [:secondary-index-keys :idx/vectors])
                  force-secondary sec/force-from-key-map]
              (testing "mismatched native storage fails preflight"
                (is (= :secondary-force/storage-mismatch
                       (try
                         (sec/check-force-from-key-map
                          prepared-key-map
                          (assoc current-key-map
                                 :store-config {:backend :memory :id (random-uuid)})
                          store :db)
                         (catch Exception error (:type (ex-data error))))))
                (is (= :secondary-force/storage-mismatch
                       (try
                         (sec/check-force-from-key-map
                          prepared-key-map
                          (assoc current-key-map
                                 :mmap-dir (str prox-path "-other-mmap"))
                          store :db)
                         (catch Exception error (:type (ex-data error)))))))
              (testing "a crash after native force leaves primary unchanged"
                (is (= :injected-secondary-force-crash
                       (try
                         (with-redefs [sec/force-from-key-map
                                       (fn [& args]
                                         (apply force-secondary args)
                                         (throw (ex-info "injected crash after native force"
                                                         {:type :injected-secondary-force-crash})))]
                           (dv/force-branch! @prepared :db #{(dv/commit-id @prepared)}
                                             {:expected-current-commit expected-main}))
                         (catch Exception error (:type (ex-data error))))))
                (is (= expected-main
                       (dv/commit-id (dv/branch-as-db main :db)))))

              (testing "response-loss retry adopts the native generation"
                (dv/force-branch! @prepared :db #{(dv/commit-id @prepared)}
                                  {:expected-current-commit expected-main})
                (let [forced-db (dv/branch-as-db main :db)
                      forced-key-map (get-in (k/get store :db nil {:sync? true})
                                             [:secondary-index-keys :idx/vectors])]
                  (is (= (:commit-id prepared-key-map)
                         (:commit-id forced-key-map))
                      "primary and secondary records retain the selected Proximum root")
                  (is (= "main" (:branch forced-key-map))
                      "the key map names the existing native destination branch")
                  (is (= #{1}
                         (nearest-proximum-eids forced-db [1.0 0.0 0.0 0.0])))))

              (testing "a stale native destination fails before primary mutation"
                (let [forced-primary (dv/commit-id (dv/branch-as-db main :db))
                      forced-key-map (get-in (k/get store :db nil {:sync? true})
                                             [:secondary-index-keys :idx/vectors])
                      destination-owner
                      (prox-call 'proximum.writing/load-commit
                                 (:store-config forced-key-map)
                                 (:commit-id forced-key-map)
                                 {:branch (keyword (:branch forced-key-map))
                                  :mmap-dir (:mmap-dir forced-key-map)})
                      dirty-owner (prox-call 'proximum.core/insert destination-owner
                                             (float-array [0.0 0.0 0.0 1.0]) 99)
                      advanced-owner (await-prox
                                      (prox-call 'proximum.protocols/sync! dirty-owner))]
                  (try
                    (is (= :force-branch/stale-destination
                           (try
                             (dv/force-branch! @prepared :db
                                               #{(dv/commit-id @prepared)}
                                               {:expected-current-commit forced-primary})
                             (catch Exception error (:type (ex-data error))))))
                    (is (= forced-primary
                           (dv/commit-id (dv/branch-as-db main :db))))
                    (finally
                      (await-prox (prox-call 'proximum.protocols/close!
                                             advanced-owner))))))

              (testing "release, cold reopen, and later writes stay isolated"
                (d/release main)
                (reset! main-released? true)
                (let [reopened-main (d/connect cfg)]
                  (try
                    (is (= #{1}
                           (nearest-proximum-eids @reopened-main
                                                  [1.0 0.0 0.0 0.0]))
                        "cold restore follows Datahike's exact stored root, not a stray native head")
                    (is (= #{1}
                           (nearest-proximum-eids @prepared
                                                  [1.0 0.0 0.0 0.0])))
                    (d/transact reopened-main [{:db/id 4
                                                :person/embedding [0.0 0.0 0.0 1.0]}])
                    (is (= #{4}
                           (nearest-proximum-eids @reopened-main
                                                  [0.0 0.0 0.0 1.0])))
                    (is (= #{1}
                           (nearest-proximum-eids @prepared
                                                  [1.0 0.0 0.0 0.0])))
                    (d/release prepared)
                    (reset! prepared-released? true)
                    (let [cold-prepared (d/connect (assoc cfg :branch :prepared))]
                      (try
                        (is (= #{1}
                               (nearest-proximum-eids @cold-prepared
                                                      [1.0 0.0 0.0 0.0]))
                            "a destination write cannot retarget the prepared branch")
                        (is (= 2
                               (sec/-estimate
                                (get-in @cold-prepared
                                        [:secondary-indices :idx/vectors])
                                {:k 100}))
                            "the prepared branch reopens its own two-vector generation")
                        (finally
                          (d/release cold-prepared))))
                    (finally
                      (d/release reopened-main))))))
            (finally
              (when-not @prepared-released?
                (d/release prepared)))))
        (finally
          (when-not @main-released?
            (d/release main))
          (d/delete-database cfg)
          (delete-tree! prox-path)
          (delete-tree! (str prox-path "-mmap")))))))

(deftest legacy-main-branch-key-map-migrates-without-an-embedding-write
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (let [prox-path (str (System/getProperty "java.io.tmpdir")
                         "/datahike-proximum-legacy-force-" (random-uuid))
          prox-store {:backend :file :path prox-path :id (random-uuid)}
          cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :read
               :keep-history? true}
          _ (d/create-database cfg)
          initial (d/connect cfg)]
      (try
        (d/transact initial
                    [{:db/ident :idx/vectors
                      :db.secondary/type :proximum
                      :db.secondary/attrs [:person/embedding]
                      :db.secondary/config {:dim 4
                                            :capacity 64
                                            :distance :cosine
                                            :store-config prox-store}
                      :db.secondary/status :ready}])
        (d/transact initial [{:db/id 1
                              :person/embedding [1.0 0.0 0.0 0.0]}])
        (let [datahike-store (:store @initial)
              key-map (get-in (k/get datahike-store :db nil {:sync? true})
                              [:secondary-index-keys :idx/vectors])
              proximum-store (prox-call 'proximum.writing/connect-store-sync prox-store)
              legacy-snapshot #(dissoc % :mmap-generation
                                       :mmap-generation-bytes
                                       :mmap-generation-sha256)]
          ;; Exact pre-fb6572c/old-bridge shape: native :main snapshot without
          ;; generation evidence, while Datahike's opaque key map claims "db".
          (k/update proximum-store (:commit-id key-map) legacy-snapshot {:sync? true})
          (k/update proximum-store :main legacy-snapshot {:sync? true})
          (k/update datahike-store :db
                    #(assoc-in % [:secondary-index-keys :idx/vectors :branch] "db")
                    {:sync? true}))
        (d/release initial)
        (let [main (d/connect cfg)]
          (try
            (is (= #{1} (nearest-proximum-eids @main [1.0 0.0 0.0 0.0])))
            (dv/branch! main :db :legacy-prepared)
            (let [prepared (d/connect (assoc cfg :branch :legacy-prepared))]
              (try
                (testing "branching alone upgrades the selected legacy source"
                  (is (= #{1}
                         (nearest-proximum-eids @prepared [1.0 0.0 0.0 0.0])))
                  (let [prepared-key-map
                        (get-in (k/get (:store @main) :legacy-prepared nil {:sync? true})
                                [:secondary-index-keys :idx/vectors])
                        snapshot (k/get (prox-call 'proximum.writing/connect-store-sync
                                                   prox-store)
                                        (:commit-id prepared-key-map) nil {:sync? true})]
                    (is (= "legacy-prepared" (:branch prepared-key-map)))
                    (is (uuid? (:mmap-generation snapshot)))))
                (d/transact main [{:db/id 1
                                   :person/embedding [0.0 1.0 0.0 0.0]}
                                  {:db/id 2
                                   :person/embedding [1.0 0.0 0.0 0.0]}])
                (let [expected-main (dv/commit-id @main)
                      source-root (get-in (k/get (:store @main) :legacy-prepared nil
                                                 {:sync? true})
                                          [:secondary-index-keys :idx/vectors :commit-id])]
                  (dv/force-branch! @prepared :db #{(dv/commit-id @prepared)}
                                    {:expected-current-commit expected-main})
                  (let [forced-key-map
                        (get-in (k/get (:store @main) :db nil {:sync? true})
                                [:secondary-index-keys :idx/vectors])]
                    (is (= source-root (:commit-id forced-key-map)))
                    (is (= "main" (:branch forced-key-map)))))
                (d/release main)
                (let [reopened (d/connect cfg)]
                  (try
                    (is (= #{1}
                           (nearest-proximum-eids @reopened [1.0 0.0 0.0 0.0])))
                    (finally
                      (d/release reopened))))
                (finally
                  (d/release prepared))))
            (finally
              (try (d/release main) (catch Exception _ nil)))))
        (finally
          (try (d/release initial) (catch Exception _ nil))
          (d/delete-database cfg)
          (delete-tree! prox-path)
          (delete-tree! (str prox-path "-mmap")))))))

;; ---------------------------------------------------------------------------
;; Scriptum (Full-Text Search) Tests

(deftest test-scriptum-lifecycle
  (testing "create, index documents, search, delete"
    (let [idx (sec/create-index :scriptum
                                {:attrs #{:person/name :person/bio}
                                 :path (str "/tmp/scriptum-test-" (random-uuid))}
                                nil)]
      (is (= #{:person/name :person/bio} (sec/-indexed-attrs idx)))

      ;; Index documents
      (let [d1 (datahike.datom/datom 1 :person/name "Alice Johnson")
            d2 (datahike.datom/datom 1 :person/bio "Expert in machine learning and NLP")
            d3 (datahike.datom/datom 2 :person/name "Bob Smith")
            d4 (datahike.datom/datom 2 :person/bio "Database engineer")
            d5 (datahike.datom/datom 3 :person/name "Charlie Brown")
            d6 (datahike.datom/datom 3 :person/bio "Machine learning researcher")]
        ;; Scriptum writer is mutable, so -transact returns `this`
        (sec/-transact idx {:datom d1 :added? true})
        (sec/-transact idx {:datom d2 :added? true})
        (sec/-transact idx {:datom d3 :added? true})
        (sec/-transact idx {:datom d4 :added? true})
        (sec/-transact idx {:datom d5 :added? true})
        (sec/-transact idx {:datom d6 :added? true})

        ;; Search for "machine learning"
        (let [results (sec/-search idx {:query "machine learning" :field :value :limit 10} nil)]
          (is (= #{1 3} (set (es/entity-bitset-seq results)))))

        ;; Search for "database"
        (let [results (sec/-search idx {:query "database" :field :value :limit 10} nil)]
          (is (= #{2} (set (es/entity-bitset-seq results)))))

        ;; Filtered search
        (let [filter-bs (es/entity-bitset-from-longs [3])
              results (sec/-search idx {:query "machine learning" :field :value} filter-bs)]
          (is (= #{3} (set (es/entity-bitset-seq results)))))

        ;; Ordered results
        (is (sec/-can-order? idx :person/bio :desc))
        (is (not (sec/-can-order? idx :person/bio :asc)))
        (let [ordered (sec/-slice-ordered idx {:query "machine learning" :field :value}
                                          nil nil :desc 10)]
          (is (= 2 (count ordered)))
          (is (every? #(contains? % :score) ordered))
          (is (every? #(contains? #{1 3} (:entity-id %)) ordered)))

        ;; Delete entity 1
        (sec/-transact idx {:datom d1 :added? false})
        (let [results (sec/-search idx {:query "Alice" :field :value :limit 10} nil)]
          (is (zero? (es/entity-bitset-cardinality results))))))))

(deftest historical-scriptum-branch-fails-before-forking-current-head
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :read
             :keep-history? true}
        scriptum-path (str "/tmp/scriptum-historical-branch-" (random-uuid))
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      (d/transact conn [{:db/ident :idx/fulltext
                         :db.secondary/type :scriptum
                         :db.secondary/attrs [:person/bio]
                         :db.secondary/config {:path scriptum-path}
                         :db.secondary/status :ready}])
      (d/transact conn [{:db/id 1 :person/bio "historical document"}])
      (let [historical-cid (dv/commit-id @conn)]
        (d/transact conn [{:db/id 2 :person/bio "head only document"}])
        ;; Put a mutating detached adapter before Scriptum in the selected
        ;; key-map. The capability preflight must reject Scriptum before the
        ;; first adapter is allowed to fork anything.
        (reset! detached-branch-side-effects [])
        (k/update (:store @conn) historical-cid
                  (fn [stored]
                    (let [scriptum-key (get-in stored
                                               [:secondary-index-keys :idx/fulltext])]
                      (assoc stored :secondary-index-keys
                             (array-map :idx/side-effect
                                        {:type ::side-effect :branch :main}
                                        :idx/fulltext scriptum-key))))
                  {:sync? true})
        (is (= :historical-scriptum-branch-unsupported
               (try
                 (dv/branch! conn historical-cid :historical-scriptum)
                 (catch Exception e (:type (ex-data e))))))
        (is (empty? @detached-branch-side-effects)
            "all secondary capabilities are checked before the first fork")
        (is (not (contains? (dv/branches conn) :historical-scriptum))
            "a rejected historical secondary fork must not publish a primary branch"))
      (finally
        (d/release conn)
        (d/delete-database cfg)))))

;; ---------------------------------------------------------------------------
;; Cross-Index Composition Tests

(deftest test-cross-index-bitmap-composition
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (testing "RoaringBitmap flows between Proximum and Scriptum"
      (let [;; Create both indices
            vec-idx (sec/create-index :proximum
                                      {:attrs #{:person/embedding}
                                       :dim 4 :distance :cosine
                                       :store-config {:backend :memory :id (random-uuid)}}
                                      nil)
            ft-idx (sec/create-index :scriptum
                                     {:attrs #{:person/bio}
                                      :path (str "/tmp/scriptum-cross-" (random-uuid))}
                                     nil)
          ;; Transact vectors
            vec-idx (-> vec-idx
                        (sec/-transact {:datom (datahike.datom/datom 1 :person/embedding
                                                                     (float-array [1.0 0.0 0.0 0.0]))
                                        :added? true})
                        (sec/-transact {:datom (datahike.datom/datom 2 :person/embedding
                                                                     (float-array [0.0 1.0 0.0 0.0]))
                                        :added? true})
                        (sec/-transact {:datom (datahike.datom/datom 3 :person/embedding
                                                                     (float-array [0.9 0.1 0.0 0.0]))
                                        :added? true}))]
      ;; Transact text
        (sec/-transact ft-idx {:datom (datahike.datom/datom 1 :person/bio "ML researcher")
                               :added? true})
        (sec/-transact ft-idx {:datom (datahike.datom/datom 2 :person/bio "Database admin")
                               :added? true})
        (sec/-transact ft-idx {:datom (datahike.datom/datom 3 :person/bio "ML engineer")
                               :added? true})

      ;; Fulltext "ML" → entities {1, 3}
        (let [ml-bits (sec/-search ft-idx {:query "ML" :field :value} nil)]
          (is (= #{1 3} (set (es/entity-bitset-seq ml-bits))))

        ;; Use as pre-filter for KNN
          (let [knn-filtered (sec/-search vec-idx
                                          {:vector (float-array [1.0 0.0 0.0 0.0]) :k 3}
                                          ml-bits)]
            (is (= #{1 3} (set (es/entity-bitset-seq knn-filtered))))
          ;; Entity 2 excluded by fulltext filter
            (is (not (es/entity-bitset-contains? knn-filtered 2))))

        ;; AND composition
          (let [knn-all (sec/-search vec-idx
                                     {:vector (float-array [1.0 0.0 0.0 0.0]) :k 2} nil)
                combined (es/entity-bitset-and knn-all ml-bits)]
          ;; KNN top-2 = {1, 3}, ML = {1, 3}, AND = {1, 3}
            (is (= #{1 3} (set (es/entity-bitset-seq combined))))))))))

;; ---------------------------------------------------------------------------
;; In-Transaction Maintenance via d/db-with

(deftest test-in-transaction-maintenance
  (testing "secondary indices updated during d/db-with"
    (let [schema {:person/name {:db/index true}
                  :person/bio {}
                  :idx/fulltext {:db.secondary/type :scriptum
                                 :db.secondary/attrs [:person/name :person/bio]
                                 :db.secondary/config {:path (str "/tmp/scriptum-tx-" (random-uuid))}}}
          empty-db (db/empty-db schema)
          ft-idx (sec/create-index :scriptum
                                   {:attrs [:person/name :person/bio]
                                    :path (str "/tmp/scriptum-tx-" (random-uuid))}
                                   empty-db)
          db (assoc empty-db :secondary-indices {:idx/fulltext ft-idx})
          db2 (d/db-with db [{:db/id 1 :person/name "Alice" :person/bio "ML researcher"}
                             {:db/id 2 :person/name "Bob" :person/bio "Database engineer"}])]

      ;; The fulltext index should have been updated in-transaction
      (let [ft (get-in db2 [:secondary-indices :idx/fulltext])
            results (sec/-search ft {:query "ML" :field :value} nil)]
        (is (= #{1} (set (es/entity-bitset-seq results)))))

      ;; Search for name
      (let [ft (get-in db2 [:secondary-indices :idx/fulltext])
            results (sec/-search ft {:query "Alice" :field :value} nil)]
        (is (= #{1} (set (es/entity-bitset-seq results))))))))

(deftest test-in-transaction-proximum
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (testing "vector index updated during d/db-with"
      (let [schema {:person/embedding {}
                    :idx/vectors {:db.secondary/type :proximum
                                  :db.secondary/attrs [:person/embedding]
                                  :db.secondary/config {:dim 4 :distance :cosine
                                                        :store-config {:backend :memory
                                                                       :id (random-uuid)}}}}
            empty-db (db/empty-db schema)
            vec-idx (sec/create-index :proximum
                                      {:attrs [:person/embedding]
                                       :dim 4 :distance :cosine
                                       :store-config {:backend :memory :id (random-uuid)}}
                                      empty-db)
            db (assoc empty-db :secondary-indices {:idx/vectors vec-idx})
            db2 (d/db-with db [{:db/id 1 :person/embedding (float-array [1.0 0.0 0.0 0.0])}
                               {:db/id 2 :person/embedding (float-array [0.0 1.0 0.0 0.0])}])]

        (let [vt (get-in db2 [:secondary-indices :idx/vectors])
              results (sec/-search vt {:vector (float-array [1.0 0.0 0.0 0.0]) :k 2} nil)]
          (is (= 2 (es/entity-bitset-cardinality results)))
          (is (es/entity-bitset-contains? results 1))
          (is (es/entity-bitset-contains? results 2)))))))

;; ---------------------------------------------------------------------------
;; Stratum Entity-Filter Aggregate Tests

(deftest test-stratum-entity-filter-aggregate
  (testing "IColumnarAggregate with entity-filter mask injection"
    (let [idx (sec/create-index :stratum
                                {:attrs #{:person/salary :person/dept}}
                                nil)
          datoms [(datahike.datom/datom 1 :person/salary 50000)
                  (datahike.datom/datom 1 :person/dept "eng")
                  (datahike.datom/datom 2 :person/salary 60000)
                  (datahike.datom/datom 2 :person/dept "eng")
                  (datahike.datom/datom 3 :person/salary 70000)
                  (datahike.datom/datom 3 :person/dept "sales")
                  (datahike.datom/datom 4 :person/salary 80000)
                  (datahike.datom/datom 4 :person/dept "sales")
                  (datahike.datom/datom 5 :person/salary 90000)
                  (datahike.datom/datom 5 :person/dept "eng")]
          t (sec/-as-transient idx)
          _ (doseq [d datoms] (sec/-transact! t {:datom d :added? true}))
          idx (sec/-persistent! t)]

      ;; Full aggregate (no filter)
      (let [result (sec/-columnar-aggregate idx
                                            {:agg [[:avg :salary]] :group [:dept]})]
        (is (= 2 (count result)))
        ;; eng: (50+60+90)/3 = 66666.67, sales: (70+80)/2 = 75000
        (let [eng (first (filter #(= "eng" (:dept %)) result))
              sales (first (filter #(= "sales" (:dept %)) result))]
          (is (< (abs (- (:avg eng) 66666.67)) 1.0))
          (is (== 75000.0 (:avg sales)))))

      ;; Filtered aggregate — only entities {1, 2, 3}
      (let [filter-bs (es/entity-bitset-from-longs [1 2 3])
            result (sec/-columnar-aggregate idx
                                            {:agg [[:avg :salary]] :group [:dept]}
                                            filter-bs)]
        (is (= 2 (count result)))
        ;; eng: (50+60)/2 = 55000, sales: 70/1 = 70000
        (let [eng (first (filter #(= "eng" (:dept %)) result))
              sales (first (filter #(= "sales" (:dept %)) result))]
          (is (== 55000.0 (:avg eng)))
          (is (== 70000.0 (:avg sales)))))

      ;; Filtered aggregate — only entity {5}
      (let [filter-bs (es/entity-bitset-from-longs [5])
            result (sec/-columnar-aggregate idx
                                            {:agg [[:sum :salary]]}
                                            filter-bs)]
        (is (= 1 (count result)))
        (is (== 90000 (:sum (first result))))))))

(deftest test-stratum-partial-coverage-aggregate
  (testing "aggregate with partial coverage: filter via PSS, aggregate via stratum"
    (let [schema {:person/name {:db/index true}
                  :person/salary {}
                  :person/dept {}
                  :idx/analytics {:db.secondary/type :stratum
                                  :db.secondary/attrs [:person/salary :person/dept]}}
          empty-db (db/empty-db schema)
          stratum-idx (sec/create-index :stratum
                                        {:attrs #{:person/salary :person/dept}}
                                        empty-db)
          db (assoc empty-db :secondary-indices {:idx/analytics stratum-idx})
          ;; Add people: some named "Ivan", some not
          db (d/db-with db [{:db/id 1 :person/name "Ivan" :person/salary 50000 :person/dept "eng"}
                            {:db/id 2 :person/name "Ivan" :person/salary 80000 :person/dept "sales"}
                            {:db/id 3 :person/name "Petr" :person/salary 60000 :person/dept "eng"}
                            {:db/id 4 :person/name "Ivan" :person/salary 70000 :person/dept "eng"}
                            {:db/id 5 :person/name "Petr" :person/salary 90000 :person/dept "sales"}])]

      ;; :person/name is NOT in stratum, but :person/salary IS.
      ;; Query: avg salary of Ivans — partial coverage
      (let [result (binding [datahike.query/*disable-planner* false]
                     (d/q '[:find (avg ?s) .
                            :where [?e :person/name "Ivan"] [?e :person/salary ?s]]
                          db))]
        ;; Ivan salaries: 50000 + 80000 + 70000 = 200000 / 3 ≈ 66666.67
        (is (some? result))
        (is (< (abs (- result 66666.67)) 1.0)))

      ;; Verify legacy gives same result
      (let [result-legacy (binding [datahike.query/*disable-planner* true]
                            (d/q '[:find (avg ?s) .
                                   :where [?e :person/name "Ivan"] [?e :person/salary ?s]]
                                 db))]
        (is (< (abs (- result-legacy 66666.67)) 1.0))))))

(deftest test-stratum-predicate-pushdown
  (testing "predicates on covered columns translated to stratum WHERE"
    (let [schema {:person/salary {}
                  :person/dept {}
                  :idx/analytics {:db.secondary/type :stratum
                                  :db.secondary/attrs [:person/salary :person/dept]}}
          empty-db (db/empty-db schema)
          stratum-idx (sec/create-index :stratum
                                        {:attrs #{:person/salary :person/dept}}
                                        empty-db)
          db (assoc empty-db :secondary-indices {:idx/analytics stratum-idx})
          db (d/db-with db [{:db/id 1 :person/salary 50000 :person/dept "eng"}
                            {:db/id 2 :person/salary 80000 :person/dept "sales"}
                            {:db/id 3 :person/salary 60000 :person/dept "eng"}
                            {:db/id 4 :person/salary 70000 :person/dept "eng"}
                            {:db/id 5 :person/salary 90000 :person/dept "sales"}])]

      ;; Predicate filter: salary > 65000
      (let [result (binding [datahike.query/*disable-planner* false]
                     (d/q '[:find (avg ?s) .
                            :where [?e :person/salary ?s] [(> ?s 65000)]]
                          db))]
        ;; Salaries > 65000: 80000, 70000, 90000 → avg = 80000
        (is (some? result))
        (is (== 80000.0 result)))

      ;; Verify legacy gives same result
      (let [result-legacy (binding [datahike.query/*disable-planner* true]
                            (d/q '[:find (avg ?s) .
                                   :where [?e :person/salary ?s] [(> ?s 65000)]]
                                 db))]
        (is (== 80000.0 result-legacy))))))

;; ---------------------------------------------------------------------------
;; Cross-Index Composition: Scriptum → EntityBitSet → Stratum Aggregate

(deftest test-cross-index-scriptum-to-stratum-aggregate
  (testing "scriptum search produces bitmap → constrains stratum aggregate"
    (let [schema {:person/name {:db/index true}
                  :person/bio {}
                  :person/salary {}
                  :person/dept {}
                  :idx/fulltext {:db.secondary/type :scriptum
                                 :db.secondary/attrs [:person/bio]
                                 :db.secondary/config {:path (str "/tmp/scriptum-cross-strat-" (random-uuid))}}
                  :idx/analytics {:db.secondary/type :stratum
                                  :db.secondary/attrs [:person/salary :person/dept]}}
          empty-db (db/empty-db schema)
          ft-idx (sec/create-index :scriptum
                                   {:attrs #{:person/bio}
                                    :path (str "/tmp/scriptum-cross-strat-" (random-uuid))}
                                   empty-db)
          st-idx (sec/create-index :stratum
                                   {:attrs #{:person/salary :person/dept}}
                                   empty-db)
          db (assoc empty-db :secondary-indices
                    {:idx/fulltext ft-idx :idx/analytics st-idx})
          db (d/db-with db [{:db/id 1 :person/name "Alice" :person/bio "ML researcher" :person/salary 90000 :person/dept "eng"}
                            {:db/id 2 :person/name "Bob" :person/bio "Database admin" :person/salary 60000 :person/dept "ops"}
                            {:db/id 3 :person/name "Charlie" :person/bio "ML engineer" :person/salary 80000 :person/dept "eng"}
                            {:db/id 4 :person/name "Diana" :person/bio "PM" :person/salary 70000 :person/dept "eng"}
                            {:db/id 5 :person/name "Eve" :person/bio "ML ops" :person/salary 75000 :person/dept "ops"}])]

      ;; Direct protocol-level test: scriptum → bitmap → stratum aggregate
      (let [ft (get-in db [:secondary-indices :idx/fulltext])
            st (get-in db [:secondary-indices :idx/analytics])
            ;; Step 1: scriptum search for "ML" → EntityBitSet
            ml-entities (sec/-search ft {:query "ML" :field :value} nil)]
        ;; ML entities: {1, 3, 5}
        (is (= #{1 3 5} (set (es/entity-bitset-seq ml-entities))))

        ;; Step 2: pass bitmap as entity-filter to stratum aggregate
        (let [result (sec/-columnar-aggregate st
                                              {:agg [[:avg :salary]] :group [:dept]}
                                              ml-entities)]
          ;; eng: (90000 + 80000)/2 = 85000 (only entities 1,3 — not 4)
          ;; ops: 75000/1 = 75000 (only entity 5 — not 2)
          (is (= 2 (count result)))
          (let [eng (first (filter #(= "eng" (:dept %)) result))
                ops (first (filter #(= "ops" (:dept %)) result))]
            (is (== 85000.0 (:avg eng)))
            (is (== 75000.0 (:avg ops)))))

        ;; Step 3: chain scriptum → proximum → stratum (if proximum available)
        ;; Not tested here — but the bitmap algebra works the same way
        ))))

;; ---------------------------------------------------------------------------
;; Entity-Filter Constraining Fused Scan (General Non-Aggregate Path)

(deftest test-entity-filter-constrains-fused-scan
  (testing "secondary index search produces entity-filter that constrains PSS scan"
    (let [schema {:person/name {:db/index true}
                  :person/bio {}
                  :person/salary {}
                  :idx/fulltext {:db.secondary/type :scriptum
                                 :db.secondary/attrs [:person/bio]
                                 :db.secondary/config {:path (str "/tmp/scriptum-fused-" (random-uuid))}}}
          empty-db (db/empty-db schema)
          ft-idx (sec/create-index :scriptum
                                   {:attrs #{:person/bio}
                                    :path (str "/tmp/scriptum-fused-" (random-uuid))}
                                   empty-db)
          db (assoc empty-db :secondary-indices {:idx/fulltext ft-idx})
          db (d/db-with db [{:db/id 1 :person/name "Alice" :person/bio "ML researcher" :person/salary 90000}
                            {:db/id 2 :person/name "Bob" :person/bio "Database admin" :person/salary 60000}
                            {:db/id 3 :person/name "Charlie" :person/bio "ML engineer" :person/salary 80000}
                            {:db/id 4 :person/name "Diana" :person/bio "PM" :person/salary 70000}
                            {:db/id 5 :person/name "Eve" :person/bio "ML ops" :person/salary 75000}])]

      ;; Scriptum produces entity bitmap → used as entity-filter for PSS name lookup
      ;; Direct protocol test: filter PSS results using scriptum bitmap
      (let [ft (get-in db [:secondary-indices :idx/fulltext])
            ml-entities (sec/-search ft {:query "ML" :field :value} nil)]
        (is (= #{1 3 5} (set (es/entity-bitset-seq ml-entities))))

        ;; Now verify this can filter a PSS scan
        ;; Get all names, then filter by ML entity bitmap
        (let [all-names (d/q '[:find ?e ?n :where [?e :person/name ?n]] db)
              ml-names (filter (fn [[eid _]] (es/entity-bitset-contains? ml-entities eid)) all-names)]
          (is (= #{[1 "Alice"] [3 "Charlie"] [5 "Eve"]} (set ml-names))))))))

;; ---------------------------------------------------------------------------
;; Purge propagation — end-to-end
;;
;; Verifies the GDPR-relevant invariant against a real konserve-backed
;; secondary index: after :db.purge/entity, the purged entity must no
;; longer surface via the secondary index's own search path.

(deftest test-purge-removes-from-scriptum
  (testing "after purge the entity no longer surfaces via Scriptum fulltext"
    (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
               :keep-history? true
               :schema-flexibility :write}
          scriptum-path (str "/tmp/scriptum-purge-test-" (random-uuid))
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity
                           :db/index true}
                          {:db/ident :person/bio
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        ;; Install scriptum BEFORE adding data so the index sees live add events
        ;; (no async backfill to wait on).
        (d/transact conn [{:db/ident :idx/fulltext
                           :db.secondary/type :scriptum
                           :db.secondary/attrs [:person/name :person/bio]
                           :db.secondary/config {:path scriptum-path}}])
        (Thread/sleep 500)

        (d/transact conn [{:person/name "Alice" :person/bio "Machine learning researcher"}
                          {:person/name "Bob" :person/bio "Database engineer"}])
        (Thread/sleep 500)

        ;; Sanity: Alice and her bio surface via fulltext before purge
        (let [ft (get-in (d/db conn) [:secondary-indices :idx/fulltext])
              by-name (sec/-search ft {:query "Alice" :field :value} nil)
              by-bio  (sec/-search ft {:query "machine" :field :value} nil)]
          (is (pos? (es/entity-bitset-cardinality by-name))
              "Alice should be findable by name before purge")
          (is (pos? (es/entity-bitset-cardinality by-bio))
              "Alice's bio should be findable before purge"))

        ;; Purge Alice's entity (all her datoms across covered attributes)
        (d/transact conn [[:db.purge/entity [:person/name "Alice"]]])
        (Thread/sleep 500)

        ;; After purge: Alice no longer surfaces via either covered attribute
        (let [ft (get-in (d/db conn) [:secondary-indices :idx/fulltext])
              by-name (sec/-search ft {:query "Alice" :field :value} nil)
              by-bio  (sec/-search ft {:query "machine" :field :value} nil)]
          (is (zero? (es/entity-bitset-cardinality by-name))
              "Alice should not be findable by name after purge")
          (is (zero? (es/entity-bitset-cardinality by-bio))
              "Alice's bio should not be findable after purge"))

        ;; Bob is untouched
        (let [ft (get-in (d/db conn) [:secondary-indices :idx/fulltext])
              by-name (sec/-search ft {:query "Bob" :field :value} nil)]
          (is (pos? (es/entity-bitset-cardinality by-name))
              "Bob should still be findable after Alice's purge"))

        (finally
          (d/release conn)
          (d/delete-database cfg))))))
