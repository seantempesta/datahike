(ns ^:no-doc datahike.connections)

(def ^:dynamic *connections* (atom {}))

(defn active-connection
  "Return an active connection without acquiring an owning reference."
  [conn-id]
  (let [{:keys [conn count]} (get @*connections* conn-id)]
    (when (and conn (pos? count)) conn)))

(defn release-connection-reference!
  "Atomically release one reference and identify the final releaser."
  [conn-id release-all? completion]
  (let [[before after]
        (swap-vals! *connections*
                    (fn [connections]
                      (if-let [{:keys [count]} (get connections conn-id)]
                        (cond
                          (not (pos? count)) connections
                          release-all? (-> connections
                                           (assoc-in [conn-id :count] 0)
                                           (assoc-in [conn-id :release-completion] completion))
                          (= count 1) (-> connections
                                          (assoc-in [conn-id :count] 0)
                                          (assoc-in [conn-id :release-completion] completion))
                          :else (update-in connections [conn-id :count] dec))
                        connections)))
        before-entry (get before conn-id)
        after-entry (get after conn-id)]
    (cond
      (nil? before-entry) {:state :absent}
      (not (pos? (:count before-entry)))
      {:state :in-progress :completion (:release-completion before-entry)}
      (zero? (:count after-entry)) {:state :last :completion completion}
      :else {:state :retained})))

(defn reserve-connection-opening!
  "Atomically acquire an existing connection or reserve its first open.
  A first open reserves the node cache it will use: a live or opening sibling's
  for the same node store (`datahike.store/node-store-key`) and threshold, else
  a fresh one from `make-cache` (which must be pure: a swap! retry may discard
  it). Write hooks are shared only within one physical store."
  [conn-id completion acquisition-key physical-store-key node-store-key threshold make-cache]
  (let [[before after]
        (swap-vals! *connections*
                    (fn [connections]
                      (if-let [{:keys [conn count opening?]
                                stored-key :acquisition-key
                                stored-physical-key :physical-store-key}
                               (get connections conn-id)]
                        (cond
                          (and conn (pos? count)
                               (= acquisition-key stored-key)
                               (= physical-store-key stored-physical-key))
                          (update-in connections [conn-id :count] inc)

                          (and opening?
                               (= acquisition-key stored-key)
                               (= physical-store-key stored-physical-key))
                          (update-in connections [conn-id :waiters] (fnil inc 0))

                          :else connections)
                        (let [write-hooks
                              (or (some (fn [[_ entry]]
                                          (when (= physical-store-key
                                                   (:physical-store-key entry))
                                            (:write-hooks entry)))
                                        connections)
                                  (atom {}))
                              node-cache
                              (or (some (fn [[_ entry]]
                                          (when (and (= node-store-key (:node-store-key entry))
                                                     (= threshold (:threshold entry)))
                                            (:node-cache entry)))
                                        connections)
                                  (make-cache))]
                          (assoc connections conn-id
                                 {:opening? true
                                  :completion completion
                                  :generation (random-uuid)
                                  :waiters 0
                                  :acquisition-key acquisition-key
                                  :physical-store-key physical-store-key
                                  :node-store-key node-store-key
                                  :write-hooks write-hooks
                                  :threshold threshold
                                  :node-cache node-cache})))))
        before-entry (get before conn-id)
        after-entry (get after conn-id)]
    (cond
      (nil? before-entry) {:state :owner
                           :completion completion
                           :generation (:generation after-entry)
                           :write-hooks (:write-hooks after-entry)
                           :node-cache (:node-cache after-entry)}
      (or (not= acquisition-key (:acquisition-key before-entry))
          (not= physical-store-key (:physical-store-key before-entry)))
      {:state :config-mismatch
       :existing-key (:acquisition-key before-entry)}
      (:opening? before-entry) {:state :opening
                                :generation (:generation before-entry)
                                :completion (:completion before-entry)}
      (and (:conn before-entry) (pos? (:count before-entry)))
      {:state :existing :conn (:conn after-entry)
       :generation (:generation after-entry)}
      :else {:state :releasing
             :completion (:release-completion before-entry)})))

(defn after-fork!
  "Make the registry of a process forked from another (fork(2)) its own. The parent's
  connections must not be used across the fork (their stores' handles are the parent's),
  so they are forgotten, not released: releasing would close what the parent still holds.
  Each node cache they held stays, alone, under an inherited key with no connection and a
  zero count: a node is a decoded value at a content address, valid in any process, so the
  fork's first connect to that store adopts the cache (`reserve-connection-opening!` shares
  one by node store and threshold) and reads what the parent decoded instead of fetching
  and decoding it again. Call it first thing in the child, before any connect."
  []
  (swap! *connections*
         (fn [connections]
           (into {} (for [[_ {:keys [node-store-key threshold node-cache]}] connections
                          :when node-cache]
                      [[::inherited node-store-key threshold]
                       {:count 0
                        :node-store-key node-store-key
                        :threshold threshold
                        :node-cache node-cache}])))))

(defn complete-connection-opening!
  "Publish a reserved connection before waking opening waiters."
  [conn-id completion conn]
  (swap! *connections*
         (fn [connections]
           (let [entry (get connections conn-id)]
             (if (and (:opening? entry)
                      (identical? completion (:completion entry)))
               (assoc connections conn-id
                      {:conn conn
                       :count (inc (:waiters entry 0))
                       :generation (:generation entry)
                       :acquisition-key (:acquisition-key entry)
                       :physical-store-key (:physical-store-key entry)
                       :node-store-key (:node-store-key entry)
                       :write-hooks (:write-hooks entry)
                       :threshold (:threshold entry)
                       :node-cache (:node-cache entry)})
               (throw (ex-info "Connection opening reservation was lost."
                               {:type :connection-opening-reservation-lost
                                :conn-id conn-id})))))))

(defn fail-connection-opening!
  "Remove a matching failed opening reservation."
  [conn-id completion]
  (swap! *connections*
         (fn [connections]
           (let [entry (get connections conn-id)]
             (if (and (:opening? entry)
                      (identical? completion (:completion entry)))
               (dissoc connections conn-id)
               connections)))))

(defn delete-connection! [conn-id]
  (when-let [conn (get-in @*connections* [conn-id :conn])]
    #?(:jolt (reset! (:wrapped-atom conn) :released) :clj (reset! conn :released))
    (swap! *connections* dissoc conn-id)))
