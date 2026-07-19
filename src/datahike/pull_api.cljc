(ns ^:no-doc datahike.pull-api
  (:require
   [datahike.db.utils :as dbu]
   [datahike.db.interface :as dbi]
   [datahike.resource :as resource]
   [datalog.parser.pull :as dpp #?@(:cljs [:refer [PullSpec]])])
  #?(:clj
     (:import
      [datahike.datom Datom]
      [datalog.parser.pull PullSpec])))

(defn- into!
  [transient-coll items]
  (reduce conj! transient-coll items))

(def ^:private ^:const +default-limit+ 1000)

(defn pull-spec-attribute-dependencies
  "Returns canonical stored attributes from a parsed PullSpec.

   Returns `:all` when
   a wildcard or dynamic attribute prevents a sound narrower projection."
  [spec]
  (if (:wildcard? spec)
    :all
    (reduce-kv
     (fn [attributes _display-key options]
       (let [attribute (:attr options)
             nested (when-let [subpattern (:subpattern options)]
                      (pull-spec-attribute-dependencies subpattern))]
         (cond
           (not (keyword? attribute)) (reduced :all)
           (= nested :all) (reduced :all)
           :else (cond-> (conj attributes attribute)
                   nested (into nested)))))
     #{}
     (:attrs spec))))

(defn- entity-ref-attribute-dependencies
  [entity-refs]
  (reduce
   (fn [attributes entity-ref]
     (cond
       (keyword? entity-ref) (conj attributes :db/ident)
       (and (sequential? entity-ref)
            (= 2 (count entity-ref))
            (keyword? (first entity-ref)))
       (conj attributes (first entity-ref))
       :else attributes))
   #{}
   entity-refs))

(defn pull-dependency-plan
  "Returns a Datahike dependency plan.

   Parses one pull selector and its
   entity refs without retaining a database value."
  [selector entity-refs]
  (let [selector-attributes
        (pull-spec-attribute-dependencies (dpp/parse-pull selector))
        attributes
        (if (= selector-attributes :all)
          :all
          (into selector-attributes
                (entity-ref-attribute-dependencies entity-refs)))]
    {:datahike.query.dependency/sources
     [{:datahike.query.source/symbol '$
       :datahike.query.source/argument-position 0
       :datahike.query.source/attributes attributes}]}))

(defn- initial-frame
  "Creates an empty pattern frame according to pattern information."
  ([pattern eids multi?]
   (initial-frame pattern eids multi? false))
  ([pattern eids multi? pull-many?]
   {:state      :pattern
    :pattern    pattern
    :wildcard?  (:wildcard? pattern)
    :specs      (-> pattern :attrs seq)
    :results    (transient [])
    :kvps       (transient {})
    :eids       eids
    :multi?     multi?
    :pull-many? pull-many?
    :recursion  {:depth {} :seen #{}}}))

(defn subpattern-frame
  "Returns frame specific for given attribute"
  [pattern eids multi? attr]
  (assoc (initial-frame pattern eids multi?) :attr attr))

(defn reset-frame
  "Recalculate frame attributes from frame pattern and transfer end results to frame-specific result section"
  [frame eids kvps]
  (let [pattern (:pattern frame)
        keep-result? (or (:pull-many? frame) (seq kvps))]
    (when (and (:pull-many? frame) (nil? kvps))
      (resource/charge-result-node!))
    (assoc frame
           :eids      eids
           :specs     (seq (:attrs pattern))
           :wildcard? (:wildcard? pattern)
           :kvps      (transient {})
           :results   (cond-> (:results frame)
                        keep-result? (conj! kvps)))))

(defn push-recursion
  "Push newly processed entity and increase recursion depth."
  [rec attr eid]
  (let [{:keys [depth seen]} rec]
    (assoc rec
           :depth (update depth attr (fnil inc 0))
           :seen (conj seen eid))))

(defn seen-eid?
  [frame eid]
  (-> frame
      (get-in [:recursion :seen] #{})
      (contains? eid)))

(defn pull-seen-eid
  "Add eid to result set if entity already seen. Else return nil."
  [frame frames eid]
  (when (seen-eid? frame eid)
    (resource/charge-result-node!)
    (conj frames (update frame :results conj! {:db/id eid}))))

(defn single-frame-result
  [key frame]
  (some-> (:kvps frame) persistent! (get key)))

(defn recursion-result [frame]
  (single-frame-result ::recursion frame))

(defn recursion-frame
  [parent eid]
  (let [attr (:attr parent)
        rec  (push-recursion (:recursion parent) attr eid)]
    (assoc (subpattern-frame (:pattern parent) [eid] false ::recursion)
           :recursion rec)))

(defn pull-recursion-frame
  "Processes recursion for one entity ID or collects results.
  Replaces current frame with
  - one frame with remaining entiy IDs and
  - one subpattern frame"
  [db [frame & frames]]
  (if-let [eids (seq (:eids frame))]
    (let [frame  (reset-frame frame (rest eids) (recursion-result frame))
          eid    (first eids)]
      (or (pull-seen-eid frame frames eid)
          (conj frames frame (recursion-frame frame eid))))
    (let [kvps    (recursion-result frame)
          results (cond-> (:results frame)
                    (seq kvps) (conj! kvps))]
      (conj frames (assoc frame :state :done :results results)))))

(defn recurse-attr
  "Adds recursion frame to frame set if maximum recursion depth not reached"
  [db attr multi? eids eid parent frames]
  (let [{:keys [recursion pattern]} parent
        depth  (-> recursion (get :depth) (get attr 0))]
    (if (-> pattern :attrs (get attr) :recursion (= depth))
      (conj frames parent)
      (pull-recursion-frame
       db
       (conj frames parent
             {:state :recursion :pattern pattern
              :attr attr :multi? multi? :eids eids
              :recursion recursion
              :results (transient [])})))))

(let [pattern (PullSpec. true {})]                          ;; For performance purposes?
  (defn- expand-frame
    [parent eid attr-key multi? eids]
    (let [rec (push-recursion (:recursion parent) attr-key eid)]
      (-> pattern
          (subpattern-frame eids multi? attr-key)
          (assoc :recursion rec)))))

(defn db-ident-and-id [db x]
  (let [{:keys [ident ref]} (dbu/attr-info db x :allow-missing)]
    (if (dbu/ident-name? ident)
      {:db/id ref :db/ident ident}
      {:db/id ref})))

(defn pull-attr-datoms
  "Processes datoms found to requested pattern for given attribute, i.e.
   - limits the result set to specified or default limit,
   - renames attribute key if requested,
   - adds default value on missing attributes if requested.
   Adds frame if
   - subpattern requested on attribute,
   - recursion requested,
   - attribute is reference.
   Returns frame set."
  [db attr-key attr eid forward? datoms opts [parent & frames]]
  (let [limit (get opts :limit +default-limit+)
        attr-key (or (:as opts) attr-key)
        found (not-empty
               (into []
                     (comp (map (fn [datom]
                                  (resource/charge-work!)
                                  (resource/charge-value! (.-v ^Datom datom))
                                  datom))
                           (if limit (take limit) identity))
                     datoms))]
    (if found
      (let [ref?       (dbu/ref? db attr)
            system-attrib-ref? (dbu/system-attrib-ref? db attr)
            component? (and ref? (dbu/component? db attr))
            multi?     (if forward? (dbu/multival? db attr)
                           (not component?))
            datom-val  (if forward? (fn [d] (.-v ^Datom d))
                           (fn [d] (.-e ^Datom d)))]

        (cond
          (contains? opts :subpattern)
          (->> (subpattern-frame (:subpattern opts)
                                 (mapv datom-val found)
                                 multi? attr-key)
               (conj frames parent))

          (contains? opts :recursion)
          (recurse-attr db attr-key multi?
                        (mapv datom-val found)
                        eid parent frames)

          (and component? forward?)
          (->> found
               (mapv datom-val)
               (expand-frame parent eid attr-key multi?)
               (conj frames parent))

          :else
          (let [as-value  (if (or ref? system-attrib-ref?)
                            #(db-ident-and-id db (datom-val %))
                            datom-val)
                single?   (not multi?)]
            (let [value (cond-> (into [] (map as-value) found)
                          single? first)]
              (resource/charge-result-node!)
              (conj frames (update parent :kvps assoc! attr-key value))))))
      (if (contains? opts :default)
        (do
          (resource/charge-result-node!)
          (resource/charge-value! (:default opts))
          (conj frames (update parent :kvps assoc! attr-key (:default opts))))
        (conj frames parent)))))

(defn pull-attr
  "Retrieve datoms for given entity id and specification from database"
  [db spec eid frames]
  (let [[attr-key opts] spec]
    (if (= :db/id attr-key)
      (if (not-empty (dbi/datoms db :eavt [eid]))
        (do
          (resource/charge-result-node!)
          (conj (rest frames)
                (update (first frames) :kvps assoc! :db/id eid)))
        frames)
      (let [attr     (:attr opts)
            forward? (= attr-key attr)
            a (if (and (:attribute-refs? (dbi/-config db))
                       (not (number? attr)))
                (dbi/-ref-for db attr)
                attr)
            results  (if (nil? a)
                       []
                       (if forward?
                         (dbi/datoms db :eavt [eid a])
                         (dbi/datoms db :avet [a eid])))]
        (pull-attr-datoms db attr-key attr eid forward?
                          results opts frames)))))

(def ^:private filter-reverse-attrs
  (filter (fn [[k v]] (not= k (:attr v)))))

(defn expand-reverse-subpattern-frame
  [parent eid rattrs]
  (-> (:pattern parent)
      (assoc :attrs rattrs :wildcard? false)
      (subpattern-frame [eid] false ::expand-rev)))

(defn expand-result
  "Add intermediate result to frame next in line. Return frame set."
  [frames kvps]
  (->> kvps
       (persistent!)
       (update (first frames) :kvps into!)
       (conj (rest frames))))

(defn pull-expand-reverse-frame
  "Adds expand results of current frame to next frame in frame set."
  [db [frame & frames]]
  (->> (or (single-frame-result ::expand-rev frame) {})
       (into! (:expand-kvps frame))
       (expand-result frames)))

(defn pull-expand-frame
  "Processes datoms for one attribute or changes frame state to process reverse attributes
  and spawns new frame for subpattern."
  [db [frame & frames]]
  (if-let [datoms-by-attr (seq (:datoms frame))]
    (let [[attr datoms] (first datoms-by-attr)
          opts          (-> frame
                            (get-in [:pattern :attrs])
                            (get attr {}))]
      (pull-attr-datoms db attr attr (:eid frame) true datoms opts
                        (conj frames (update frame :datoms rest))))
    (if-let [rattrs (->> (get-in frame [:pattern :attrs])
                         (into {} filter-reverse-attrs)
                         not-empty)]
      (let [frame  (assoc frame
                          :state       :expand-rev
                          :expand-kvps (:kvps frame)
                          :kvps        (transient {}))]
        (->> rattrs
             (expand-reverse-subpattern-frame frame (:eid frame))
             (conj frames frame)))
      (expand-result frames (:kvps frame)))))

(defn pull-wildcard-expand
  [db frame frames eid pattern]
  (let [datoms (reduce (fn [grouped ^Datom datom]
                         (resource/charge-work!)
                         (let [attr (if (:attribute-refs? (dbi/-config db))
                                      (dbi/ident-for db (.-a datom) :error-on-missing)
                                      (.-a datom))]
                           (update grouped attr (fnil conj []) datom)))
                       {}
                       (dbi/datoms db :eavt [eid]))
        {:keys [attr recursion]} frame
        rec (cond-> recursion
              (some? attr) (push-recursion attr eid))]
    (resource/charge-result-node!)
    (->> {:state :expand :kvps (transient {:db/id eid})
          :eid eid :pattern pattern :datoms (seq datoms)
          :recursion rec}
         (conj frames frame)
         (pull-expand-frame db))))

(defn pull-wildcard
  [db frame frames]
  (let [{:keys [eid pattern]} frame]
    (or (pull-seen-eid frame frames eid)
        (pull-wildcard-expand db frame frames eid pattern))))

(defn pull-pattern-frame
  [db [frame & frames]]
  (if-let [eids (seq (:eids frame))]
    (if (nil? (first eids))
      (->> (reset-frame frame (rest eids) nil)
           (conj frames)
           (recur db))
      (if (:wildcard? frame)
        (pull-wildcard db
                       (assoc frame
                              :specs []
                              :eid (first eids)
                              :wildcard? false)
                       frames)
        (if-let [specs (seq (:specs frame))]
          (let [spec       (first specs)
                new-frames (conj frames (assoc frame :specs (rest specs)))]
            (pull-attr db spec (first eids) new-frames))
          (->> frame :kvps persistent! not-empty
               (reset-frame frame (rest eids))
               (conj frames)
               (recur db)))))
    (conj frames (assoc frame :state :done))))

(defn pull-pattern
  [db frames]
  (resource/charge-work!)
  (case (:state (first frames))
    :expand     (recur db (pull-expand-frame db frames))
    :expand-rev (recur db (pull-expand-reverse-frame db frames))
    :pattern    (recur db (pull-pattern-frame db frames))
    :recursion  (recur db (pull-recursion-frame db frames))
    :done       (let [[f & remaining] frames
                      result (cond-> (persistent! (:results f))
                               (not (:multi? f)) first)]
                  (if (seq remaining)
                    (->> (cond-> (first remaining)
                           result (update :kvps assoc! (:attr f) result))
                         (conj (rest remaining))
                         (recur db))
                    result))))

(defn- resolve-pull-eid
  "Returns an existing pull entity ID or nil for a missing entity ref."
  [db entity-ref]
  (resource/charge-work!)
  (when-let [eid (dbu/entid db entity-ref)]
    (when (or (not (number? entity-ref))
              (first (dbi/datoms db :eavt [eid])))
      eid)))

(defn pull-spec
  ([db pattern eids multi?]
   (pull-spec db pattern eids multi? nil))
  ([db pattern eids multi? options]
   (let [budget (resource/make-budget options)]
     (binding [resource/*budget* (or budget resource/*budget*)]
       (let [eids (into [] (map #(resolve-pull-eid db %)) eids)]
         (resource/certify-result!
          (pull-pattern db (list (initial-frame pattern eids multi? multi?)))
          (dissoc options :max-results)))))))

(defn pull
  ([db {:keys [selector eid max-work max-results max-result-weight]}]
   {:pre [(dbu/db? db)]}
   (pull-spec db (dpp/parse-pull selector) [eid] false
              {:max-work max-work
               :max-results max-results
               :max-result-weight max-result-weight}))
  ([db selector eid]
   {:pre [(dbu/db? db)]}
   (pull-spec db (dpp/parse-pull selector) [eid] false)))

(defn pull-many
  "Pulls one input-aligned eager result for each entity ref.

   Well-formed missing refs return nil in their original positions. Malformed
   refs and lookup attributes without uniqueness remain errors."
  ([db {:keys [selector eids max-work max-results max-result-weight]}]
   {:pre [(dbu/db? db)]}
   (pull-spec db (dpp/parse-pull selector) eids true
              {:max-work max-work
               :max-results max-results
               :max-result-weight max-result-weight}))
  ([db selector eids]
   {:pre [(dbu/db? db)]}
   (pull-spec db (dpp/parse-pull selector) eids true)))

(defn pull-with-evidence
  "Pulls one entity and returns the value with its parsed dependency plan."
  ([db {:keys [selector eid] :as options}]
   {:datahike.pull/result (pull db options)
    :datahike.read/dependency-plan (pull-dependency-plan selector [eid])})
  ([db selector eid]
   {:datahike.pull/result (pull db selector eid)
    :datahike.read/dependency-plan (pull-dependency-plan selector [eid])}))

(defn pull-many-with-evidence
  "Pulls input-aligned entities and returns the values with one shared parsed
   dependency plan."
  ([db {:keys [selector eids] :as options}]
   {:datahike.pull-many/result (pull-many db options)
    :datahike.read/dependency-plan (pull-dependency-plan selector eids)})
  ([db selector eids]
   {:datahike.pull-many/result (pull-many db selector eids)
    :datahike.read/dependency-plan (pull-dependency-plan selector eids)}))
