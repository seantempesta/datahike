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
  "Atomically acquire an existing connection or reserve its first open."
  [conn-id completion acquisition-key physical-store-key]
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
                                  (atom {}))]
                          (assoc connections conn-id
                                 {:opening? true
                                  :completion completion
                                  :generation (random-uuid)
                                  :waiters 0
                                  :acquisition-key acquisition-key
                                  :physical-store-key physical-store-key
                                  :write-hooks write-hooks})))))
        before-entry (get before conn-id)
        after-entry (get after conn-id)]
    (cond
      (nil? before-entry) {:state :owner
                           :completion completion
                           :generation (:generation after-entry)
                           :write-hooks (:write-hooks after-entry)}
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
                       :write-hooks (:write-hooks entry)})
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
    (reset! conn :released)
    (swap! *connections* dissoc conn-id)))
