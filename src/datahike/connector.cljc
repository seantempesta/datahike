(ns ^:no-doc datahike.connector
  (:require [datahike.connections :refer [delete-connection!
                                          release-connection-reference!
                                          reserve-connection-opening!
                                          complete-connection-opening!
                                          fail-connection-opening!
                                          *connections*]]
            [datahike.readers]
            [datahike.store :as ds]
            [datahike.writing :as dsi]
            [datahike.config :as dc]
            [datahike.tools :as dt #?(:clj :refer :cljs :refer-macros) [meta-data]]
            [datahike.writer :as w]
            [konserve.core :as k]
            [konserve.protocols :as kp]
            [konserve.store :as ks]
            [replikativ.logging :as log]
            [clojure.spec.alpha :as s]
            [clojure.data :refer [diff]]
            [konserve.utils :refer [#?(:clj async+sync) *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            [superv.async :refer [go-try- <?-]]
            [clojure.core.async :refer [go <!] :as async])
  #?(:clj (:import [clojure.lang IDeref IAtom IMeta ILookup IRef])))

;; connection

(declare deref-conn)

#?(:clj
   (defn- blocking-take
     "Take a runtime completion only from async+sync's translated sync branch."
     [channel]
     (async/<!! channel)))

(deftype Connection [wrapped-atom]
  IDeref
  (#?(:clj deref :cljs -deref) [conn] (deref-conn conn))
  ;; These interfaces should not be used from the outside, they are here to keep
  ;; the internal interfaces lean and working.
  ILookup
  (#?(:clj valAt :cljs -lookup) [c k] (if (= k :wrapped-atom) wrapped-atom nil))
  IMeta
  (#?(:clj meta :cljs -meta) [_] (meta wrapped-atom))
  #?@(:cljs
      [IAtom
       ISwap
       (-swap! [_ f] (swap! wrapped-atom f))
       (-swap! [_ f arg] (swap! wrapped-atom f arg))
       (-swap! [_ f arg1 arg2] (swap! wrapped-atom f arg1 arg2))
       (-swap! [_ f arg1 arg2 args] (apply swap! wrapped-atom f arg1 arg2 args))
       IReset
       (-reset! [_ newval] (reset! wrapped-atom newval))
       IWatchable ;; TODO This is unofficially supported, it triggers watches on each update, not on commits. For proper listeners use the API.
       (-add-watch [_ key f] (add-watch wrapped-atom key f))
       (-remove-watch [_ key] (remove-watch wrapped-atom key))
       (-notify-watches [_ old new] (-notify-watches wrapped-atom old new))])
  #?@(:clj
      [IAtom
       (swap [_ f] (swap! wrapped-atom f))
       (swap [_ f arg] (swap! wrapped-atom f arg))
       (swap [_ f arg1 arg2] (swap! wrapped-atom f arg1 arg2))
       (swap [_ f arg1 arg2 args] (apply swap! wrapped-atom f arg1 arg2 args))
       (compareAndSet [_ oldv newv] (compare-and-set! wrapped-atom oldv newv))
       (reset [_ newval] (reset! wrapped-atom newval))
       IRef ;; TODO This is unofficially supported, it triggers watches on each update, not on commits. For proper listeners use the API.
       (addWatch [_ key f] (add-watch wrapped-atom key f))
       (removeWatch [_ key] (remove-watch wrapped-atom key))]))

(defn connection? [x]
  (instance? Connection x))

#?(:clj
   (defmethod print-method Connection
     [^Connection conn ^java.io.Writer w]
     (let [config (:config @(:wrapped-atom conn))]
       (.write w "#datahike/Connection")
       (.write w (pr-str (ds/connection-id config))))))

(defn deref-conn [^Connection conn]
  (let [wrapped-atom (.-wrapped-atom conn)]
    (when (= @wrapped-atom :released)
      (throw (ex-info "Connection has been released."
                      {:type :connection-has-been-released})))
    (if (not (w/streaming? (get @wrapped-atom :writer)))
      (let [store  (:store @wrapped-atom)
            stored (k/get store (:branch (:config @wrapped-atom)) nil {:sync? true})]
        (log/trace :datahike/db-deref {:config (:config stored)})
        (dsi/stored->db stored store))
      @wrapped-atom)))

(defn conn-from-db
  "Creates a mutable reference to a given immutable database. See [[create-conn]]."
  [db]
  (Connection. (atom db :meta {:listeners (atom {})})))

(s/def ::connection #(and (instance? Connection %)
                          (not= @(:wrapped-atom %) :released)))

(defn version-check [{:keys [meta config] :as db}]
  (let [{dh-stored :datahike/version
         hh-stored :hitchhiker.tree/version
         pss-stored :persistent.set/version
         ksv-stored :konserve/version} meta
        {dh-now :datahike/version
         hh-now :hitchhiker.tree/version
         pss-now :persistent.set/version
         ksv-now :konserve/version} (meta-data)]
    (when-not (or (= dh-now "DEVELOPMENT")
                  (= dh-stored "DEVELOPMENT")
                  (>= (compare dh-now dh-stored) 0))
      (log/raise "Database was written with newer Datahike version."
                 {:type :db-was-written-with-newer-datahike-version
                  :stored dh-stored
                  :now dh-now
                  :config config}))
    (when (and hh-stored hh-now
               (not (>= (compare hh-now hh-stored) 0)))
      (log/raise "Database was written with newer hitchhiker-tree version."
                 {:type :db-was-written-with-newer-hht-version
                  :stored hh-stored
                  :now hh-now
                  :config config}))
    (when-not (>= (compare pss-now pss-stored) 0)
      (log/raise "Database was written with newer persistent-sorted-set version."
                 {:type :db-was-written-with-newer-pss-version
                  :stored pss-stored
                  :now pss-now
                  :config config}))
    (when-not (>= (compare ksv-now ksv-stored) 0)
      (log/raise "Database was written with newer konserve version."
                 {:type   :db-was-written-with-newer-konserve-version
                  :stored ksv-stored
                  :now    ksv-now
                  :config config}))))

(defn ensure-stored-config-consistency [config stored-config]
  (let [;; Remove runtime parameters and creation-time parameters
        config (dissoc config :name :search-cache-size :store-cache-size)
        stored-config (dissoc stored-config :initial-tx :name :search-cache-size :store-cache-size)
        stored-config (merge {:writer dc/self-writer} stored-config)
        stored-config (if (empty? (:index-config stored-config))
                        (dissoc stored-config :index-config)
                        stored-config)
        ;; if we connect to remote allow writer to be different
        [config stored-config] (if-not (= dc/self-writer config)
                                 [(dissoc config :writer)
                                  (dissoc stored-config :writer)]
                                 [config stored-config])

        ;; Validate store identities match (prevents connecting to wrong database)
        ;; Store configuration details (backend, path, credentials) can differ
        stored-store-id (get-in stored-config [:store :id])
        connect-store-id (get-in config [:store :id])
        _ (when (and stored-store-id connect-store-id
                     (not= stored-store-id connect-store-id))
            (log/raise "Store identity mismatch: connecting to wrong database."
                       {:type :store-identity-mismatch
                        :stored-id stored-store-id
                        :connect-id connect-store-id
                        :config config
                        :stored-config stored-config}))

        ;; Remove entire :store from comparison (backend, path, credentials can change)
        ;; Only the :id needs to match (checked above)
        config (dissoc config :store)
        stored-config (dissoc stored-config :store)]

    (when-not (= config stored-config)
      (log/raise "Configuration does not match stored configuration. In some cases this check is too restrictive. If you are sure you are loading the right database with the right configuration then you can disable this check by setting :allow-unsafe-config to true in your config."
                 {:type          :config-does-not-match-stored-db
                  :config        config
                  :stored-config stored-config
                  :diff          (diff config stored-config)}))))

(def create-time-fixed-index-keys
  "Sub-keys of :index-config that shape the on-disk index representation and are
   therefore fixed when the database is created. At connect they are adopted from
   the stored config, so a reconnect does not need to re-specify them."
  #{:branching-factor :diff-buf-size})

(def store-fixed-record-keys
  "Top-level config keys that describe how records in the store are laid out
   and are therefore fixed when the database is created: :fuse-index-roots?
   (index roots inlined into the db record) and :commit-graph? (whether each
   commit persists an immutable cid record). Adopted at connect like the
   create-time-fixed :index-config sub-keys."
  #{:fuse-index-roots? :commit-graph?})

(defn- adopt-create-time-fixed
  "Adopt store-fixed settings from the stored config into `config`: the
   create-time-fixed :index-config sub-keys and the store-fixed-record-keys
   (which describe how records in the store are laid out). A key the caller
   did not specify is taken from the store, so reconnects don't need to
   re-specify creation settings; an explicitly conflicting value raises unless
   :allow-unsafe-config is set (then the given value wins). Returns the
   possibly-updated config."
  [config stored-config]
  (let [unsafe?   (:allow-unsafe-config config)
        stored-ic (select-keys (:index-config stored-config) create-time-fixed-index-keys)
        given-ic  (:index-config config)
        conflicts (into (into {}
                              (keep (fn [[k stored-v]]
                                      (when (and (contains? given-ic k)
                                                 (not= (get given-ic k) stored-v))
                                        [k {:given (get given-ic k) :stored stored-v}])))
                              stored-ic)
                        (keep (fn [k]
                                (when (and (contains? stored-config k)
                                           (contains? config k)
                                           (not= (get config k) (get stored-config k)))
                                  [k {:given (get config k)
                                      :stored (get stored-config k)}])))
                        store-fixed-record-keys)]
    (when (and (seq conflicts) (not unsafe?))
      (log/raise "Create-time-fixed index settings differ from the stored configuration."
                 {:type      :create-time-fixed-index-config-mismatch
                  :conflicts conflicts
                  :config    config}))
    (let [ic (if unsafe? (merge stored-ic given-ic) (merge given-ic stored-ic))
          config (if (seq ic)
                   (assoc config :index-config ic)
                   (dissoc config :index-config))]
      (reduce (fn [config k]
                (if (and (contains? stored-config k)
                         (or (not (contains? config k)) (not unsafe?)))
                  (assoc config k (get stored-config k))
                  config))
              config
              store-fixed-record-keys))))

(defn connection-acquisition-key
  "Return the semantic config used to share one physical connection safely."
  [cfg]
  (let [writer-backend (get-in cfg [:writer :backend] :self)
        ;; Runtime handles such as Kabel's :local-peer are not comparable, but
        ;; the remote endpoint is: two callers targeting different peers/URLs
        ;; must never share one physical writer.
        writer-key (when-not (= :self writer-backend)
                     (select-keys (:writer cfg) [:backend :peer-id :url]))]
    (cond->
     (-> cfg
         ;; :index-config and the store-fixed-record-keys are store-fixed and
         ;; adopted on a fresh connect (adopt-create-time-fixed), so an existing
         ;; connection may carry adopted keys the caller's config omits;
         ;; conflicts are guarded on the fresh-connect path, not here.
         (dissoc :writer :store :store-cache-size :search-cache-size
                 :index-config :fuse-index-roots? :commit-graph?))
      writer-key (assoc :writer writer-key))))

(defn close-secondary-indices
  "Close every resource-owning secondary index and return cleanup failures."
  [db]
  #?(:clj
     (into []
           (keep (fn [[ident idx]]
                   (when (instance? java.io.Closeable idx)
                     (try
                       (.close ^java.io.Closeable idx)
                       nil
                       (catch Throwable e
                         (log/warn :datahike/secondary-index-close-failed
                                   {:ident ident :error (.getMessage e)})
                         e)))))
           (:secondary-indices db))
     :cljs []))

(defn -connect-impl* [config opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [_ (log/debug :datahike/connect {:config (update-in config [:store] dissoc :password)})
                     store-config (:store config)
                     conn-id (ds/connection-id config)
                     acquisition-key (connection-acquisition-key config)
                     physical-store-key (ds/physical-store-key store-config)
                     requested-completion (async/promise-chan)
                     {:keys [state conn completion existing-key write-hooks]}
                     (reserve-connection-opening! conn-id requested-completion
                                                  acquisition-key physical-store-key)]
                 (case state
                   :existing
                   conn

                   :opening
                   (let [opened #?(:clj (if (:sync? opts)
                                          (blocking-take completion)
                                          (<?- completion))
                                    :cljs (<?- completion))]
                     (when (instance? #?(:clj Throwable :cljs js/Error) opened)
                       (throw opened))
                     (if opened
                       opened
                       (log/raise "Connection opening completed without a published connection."
                                  {:type :connection-opening-not-published
                                   :conn-id conn-id})))

                   :config-mismatch
                   (log/raise "Configuration does not match existing connections."
                              {:type :config-does-not-match-existing-connections
                               :config acquisition-key
                               :existing-connections-config existing-key
                               :diff (diff acquisition-key existing-key)})

                   :releasing
                   (log/raise "Connection is being released."
                              {:type :connection-is-being-released
                               :conn-id conn-id})

                   :owner
                   (let [resources (volatile! {})]
                     (try
                       (let [raw-store (<?- (ks/connect-store store-config opts))
                         _         (vswap! resources assoc :store raw-store)
                         _         (when-not raw-store
                                     (log/raise "Backend does not exist." {:type   :backend-does-not-exist
                                                                           :config store-config}))
                         store     (ds/add-cache-and-handlers raw-store config)
                         _         (vswap! resources assoc :store store)
                         _ (<?- (ds/ready-store (assoc store-config :opts opts) store))
                         stored-db (<?- (k/get store (:branch config) nil opts))
                         _         (when-not stored-db
                                     (log/raise "Database does not exist." {:type   :db-does-not-exist
                                                                            :config config}))
                         [config store stored-db]
                         (let [intended-index (:index config)
                               stored-index   (get-in stored-db [:config :index])]
                           (if-not (= intended-index stored-index)
                             (do
                               (log/warn :datahike/index-mismatch {:stored-index stored-index})
                               (let [config    (assoc config :index stored-index)
                                     store     (ds/add-cache-and-handlers raw-store config)
                                     _ (<?- (ds/ready-store (assoc store-config :opts opts) store))
                                     stored-db (<?- (k/get store (:branch config) nil opts))]
                                 [config store stored-db]))
                             [config store stored-db]))
                         ;; Adopt create-time-fixed index settings (:index-config
                         ;; {:branching-factor :diff-buf-size}) from the stored config.
                         ;; When adoption changes the config, re-derive the store
                         ;; handlers with it (same pattern as the index reconciliation
                         ;; above) so e.g. a legacy store's non-default branching-factor
                         ;; reaches the node read handlers.
                         [config store stored-db]
                         (let [config' (adopt-create-time-fixed config (:config stored-db))]
                           (if (= config' config)
                             [config store stored-db]
                             (let [store     (ds/add-cache-and-handlers raw-store config')
                                   _ (<?- (ds/ready-store (assoc store-config :opts opts) store))
                                   stored-db (<?- (k/get store (:branch config') nil opts))]
                               [config' store stored-db])))
                         store (kp/-set-write-hooks! store write-hooks)
                         _ (vswap! resources assoc :store store)
                         _ (version-check stored-db)
                         _ (when-not (:allow-unsafe-config config)
                             (ensure-stored-config-consistency config (:config stored-db)))
                         conn      (conn-from-db (dsi/stored->db (assoc stored-db :config config) store))
                         _         (vswap! resources assoc :conn conn)]
                     (swap! (:wrapped-atom conn) assoc :writer
                            (w/create-writer (:writer config) conn))
                     ;; Recovery: backfill secondary indices that are :building
                     ;; and were not restored from durable storage
                     #?(:clj
                        (let [db @(:wrapped-atom conn)
                              schema (:schema db)
                              writer (:writer db)
                              sec-idx-keys (:secondary-index-keys stored-db)]
                          (doseq [[ident entry] schema
                                  :when (and (map? entry)
                                             (:db.secondary/type entry)
                                             (= :building (:db.secondary/status entry))
                                             ;; Only backfill if no stored key-map exists
                                             (not (get sec-idx-keys ident)))]
                            (log/trace :datahike/secondary-index-backfill {:ident ident})
                            (go
                              (let [build-result (<! (w/dispatch! writer
                                                                  {:op 'build-secondary-index!
                                                                   :args [ident]}))]
                                (when (map? build-result)
                                  (w/dispatch! writer {:op 'install-secondary-index!
                                                       :args [build-result]})))))))
                         (complete-connection-opening! conn-id completion conn)
                         (async/put! completion conn)
                         conn)
                       (catch #?(:clj Throwable :cljs js/Error) e
                         (let [{opened-conn :conn opened-store :store} @resources
                               opened-db (some-> opened-conn :wrapped-atom deref)
                               writer (:writer opened-db)]
                           (when writer
                             (try
                               #?(:clj (if (:sync? opts)
                                         (blocking-take (w/shutdown writer))
                                         (<?- (w/shutdown writer)))
                                  :cljs (<?- (w/shutdown writer)))
                               (catch #?(:clj Throwable :cljs js/Error) cleanup-error
                                 (log/warn :datahike/connection-open-writer-cleanup-failed
                                           {:error cleanup-error}))))
                           ;; stored->db can restore resource-owning secondary
                           ;; handles before a later config/writer/publication
                           ;; failure. Close them before releasing their store.
                           (close-secondary-indices opened-db)
                           (when opened-store
                             (try
                               (<?- (ks/release-store store-config opened-store opts))
                               (catch #?(:clj Throwable :cljs js/Error) cleanup-error
                                 (log/warn :datahike/connection-open-store-cleanup-failed
                                           {:error cleanup-error})))))
                         (fail-connection-opening! conn-id completion)
                         (async/put! completion e)
                         (throw e)))))))))

;; Multimethod dispatch for different writer backends

(defn backend-dispatch [config & _]
  (get-in config [:writer :backend] :self))

(defmulti -connect* #'backend-dispatch)

(defmethod -connect* :self [config opts]
  (-connect-impl* config opts))

;; public API

(defn connect
  "Connect to a Datahike database.
   
   Config can be a map or URI string. Opts map supports:
   - :sync? (default true) - Block and return connection, or return channel for async"
  ([] (connect {} {}))
  ([config] (connect config {}))
  ([config opts]
   (let [opts (merge {:sync? true} opts)
         normalized (cond
                      (string? config) (dc/uri->config config)
                      (map? config) config
                      :else config)
         loaded (dissoc (dc/load-config normalized) :initial-tx :remote-peer :name)]
     (-connect* loaded opts))))

(defn release
  ([connection] (release connection false))
  ([connection release-all?]
   (let [opts {:sync? #?(:clj true :cljs false)}]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  (when-not (= @(:wrapped-atom connection) :released)
                    (let [db @(:wrapped-atom connection)
                          _ (log/trace :datahike/release-connection
                                       {:backend (get-in db [:config :store :backend])})
                          conn-id (ds/connection-id (:config db))
                          requested-completion (async/promise-chan)
                          {:keys [state completion]}
                          (release-connection-reference! conn-id release-all?
                                                         requested-completion)]
                      (case state
                        :absent
                        (log/trace :datahike/connection-already-released {:conn-id conn-id})

                        :in-progress
                        (do
                          (log/trace :datahike/connection-release-in-progress {:conn-id conn-id})
                          (let [result #?(:clj (if (:sync? opts)
                                                (blocking-take completion)
                                                (<?- completion))
                                          :cljs (<?- completion))]
                            (when (instance? #?(:clj Throwable :cljs js/Error) result)
                              (throw result))))

                        :last
                        (let [shutdown-error
                              (try
                            ;; shutdown closes admission synchronously; await its
                            ;; completion so every already-accepted write has
                            ;; either committed or failed before release returns.
                                #?(:clj
                                   (let [result (if (:sync? opts)
                                                  (blocking-take (w/shutdown (:writer db)))
                                                  (<?- (w/shutdown (:writer db))))]
                                     (when (instance? Throwable result)
                                       (throw result)))
                                   :cljs
                                   (<?- (w/shutdown (:writer db))))
                                nil
                                (catch #?(:clj Throwable :cljs js/Error) e e))
                            ;; Secondary writers may be touched by queued writes,
                            ;; so close them only after the primary writer drains.
                              secondary-errors (close-secondary-indices db)
                            ;; Release the underlying store only after the writer
                            ;; can no longer address it.
                              store-error
                              (try
                                (<?- (ks/release-store (get-in db [:config :store])
                                                       (:store db) opts))
                                nil
                                (catch #?(:clj Throwable :cljs js/Error) e e))
                              errors (vec (remove nil?
                                                  (into [shutdown-error store-error]
                                                        secondary-errors)))
                              outcome (when (seq errors)
                                        (ex-info "Connection release failed."
                                                 {:type :connection-release-failed
                                                  :conn-id conn-id
                                                  :error-count (count errors)
                                                  :errors errors}
                                                 (first errors)))]
                          (delete-connection! conn-id)
                          (async/put! completion (or outcome :released))
                          (when outcome
                            (throw outcome)))

                        :retained nil)))
                  nil)))))
