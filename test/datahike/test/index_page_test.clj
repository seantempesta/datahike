(ns datahike.test.index-page-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.datom :as dd]
            [datahike.db.interface :as dbi]))

(def schema
  [{:db/ident :item/id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :item/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :item/friend
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :item/blob
    :db/valueType :db.type/bytes
    :db/cardinality :db.cardinality/one
    :db/index true}])

(defn- with-database [f]
  (let [configuration {:store {:backend :memory :id (random-uuid)}
                       :schema-flexibility :write
                       :keep-history? true
                       :initial-tx schema}]
    (d/create-database configuration)
    (let [connection (d/connect configuration)]
      (try
        (f connection)
        (finally
          (d/release connection)
          (d/delete-database configuration))))))

(defn- datom-data [datom]
  [(:e datom) (:a datom) (:v datom)
   (dd/datom-tx datom) (dd/datom-added datom)])

(defn- all-pages [db options]
  (loop [cursor nil
         result []]
    (let [page (d/index-page db (cond-> options cursor (assoc :cursor cursor)))
          result (into result (:datahike.index-page/datoms page))]
      (if (:datahike.index-page/complete? page)
        result
        (recur (:datahike.index-page/cursor page) result)))))

(defn- same-native-datoms? [db index left right]
  (let [comparator
        (dd/index-type->cmp-quick
         index (not (dbi/context-temporal? (dbi/-search-context db))))]
    (and (= (count left) (count right))
         (every? true?
                 (map (fn [left-datom right-datom]
                        (and (zero? (comparator left-datom right-datom))
                             (= (dd/datom-added left-datom)
                                (dd/datom-added right-datom))))
                      left right)))))

(deftest current-and-history-pages-compose-in-both-directions
  (with-database
    (fn [connection]
      (d/transact connection [{:item/id "one" :item/name "before"}
                              {:item/id "two" :item/name "stable"}])
      (d/transact connection [{:item/id "one" :item/name "after"}])
      (let [current @connection
            history (d/history current)
            forward {:index :aevt :components [:item/name]
                     :direction :forward :limit 1}
            reverse-options (assoc forward :direction :reverse)
            current-values (mapv :v (all-pages current forward))
            forward-datoms (all-pages history forward)
            reverse-datoms (all-pages history reverse-options)
            history-forward (mapv datom-data forward-datoms)
            comparator (dd/index-type->cmp-quick :aevt false)]
        (is (= ["after" "stable"] current-values))
        (is (= (count forward-datoms) (count reverse-datoms)))
        (is (every?
             true?
             (map (fn [forward-datom reverse-datom]
                    (and (zero? (comparator forward-datom reverse-datom))
                         (= (dd/datom-added forward-datom)
                            (dd/datom-added reverse-datom))))
                  forward-datoms
                  (reverse reverse-datoms))))
        (is (some #(and (= "before" (nth % 2)) (false? (nth % 4)))
                  history-forward))
        (is (some #(and (= "after" (nth % 2)) (true? (nth % 4)))
                  history-forward))))))

(deftest as-of-avet-pages-compose-after-a-cardinality-one-replacement
  (let [configuration {:store {:backend :memory :id (random-uuid)}
                       :schema-flexibility :write
                       :keep-history? true
                       ;; Bootstrap datoms outside the later AVET prefix expose
                       ;; temporal reconstruction order; four is the smallest
                       ;; stable fixture that interrupts the old reverse page.
                       :initial-tx
                       (mapv (fn [n]
                               {:db/ident (keyword "bootstrap" (str "a" n))
                                :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one})
                             (range 4))}]
    (d/create-database configuration)
    (let [connection (d/connect configuration)]
      (try
        (d/transact connection
                    [{:db/ident :rank/id
                      :db/valueType :db.type/string
                      :db/cardinality :db.cardinality/one
                      :db/unique :db.unique/identity}
                     {:db/ident :rank/value
                      :db/valueType :db.type/long
                      :db/cardinality :db.cardinality/one
                      :db/index true}
                     {:rank/id "alice" :rank/value 1}
                     {:rank/id "bob" :rank/value 2}])
        (let [cut (:max-tx @connection)]
          (d/transact connection
                      [[:db/add [:rank/id "alice"] :rank/value 9]])
          (let [as-of (d/as-of @connection cut)
                forward {:index :avet :components [:rank/value]
                         :direction :forward :limit 1}
                reverse (assoc forward :direction :reverse)]
            (is (= [1 2] (mapv :v (all-pages as-of forward))))
            (is (= [2 1] (mapv :v (all-pages as-of reverse))))))
        (finally
          (d/release connection)
          (d/delete-database configuration))))))

(deftest polarity-is-part-of-an-exact-history-cursor
  (with-database
    (fn [connection]
      (d/transact connection [{:item/id "one" :item/name "value"}])
      (let [history (d/history @connection)
            existing (first (d/datoms history :aevt :item/name))
            tx (dd/datom-tx existing)
            retracted (dd/datom (:e existing) (:a existing) (:v existing)
                                tx false)
            asserted (dd/datom (:e existing) (:a existing) (:v existing)
                               tx true)
            forward [retracted asserted]
            reverse (reverse forward)
            request {:index :aevt
                     :components [(:a existing) (:e existing)
                                  (:v existing) tx]
                     :direction :forward
                     :limit 1}]
        (with-redefs [dbi/seek-datoms (fn [_ _ _] forward)
                      dbi/rseek-datoms (fn [_ _ _] reverse)]
          (let [first-forward (d/index-page history request)
                second-forward
                (d/index-page history
                              (assoc request
                                     :cursor
                                     (:datahike.index-page/cursor first-forward)))
                first-reverse
                (d/index-page history (assoc request :direction :reverse))
                second-reverse
                (d/index-page history
                              (assoc request
                                     :direction :reverse
                                     :cursor
                                     (:datahike.index-page/cursor first-reverse)))]
            (is (= [false] (mapv dd/datom-added
                                (:datahike.index-page/datoms first-forward))))
            (is (= [true] (mapv dd/datom-added
                               (:datahike.index-page/datoms second-forward))))
            (is (= [true] (mapv dd/datom-added
                               (:datahike.index-page/datoms first-reverse))))
            (is (= [false] (mapv dd/datom-added
                                (:datahike.index-page/datoms second-reverse))))))))))

(deftest every-native-index-concatenates-forward-and-reverse
  (with-database
    (fn [connection]
      (d/transact connection [{:item/id "a" :item/name "a"}
                              {:item/id "b" :item/name "b"}])
      (d/transact connection [{:item/id "a" :item/name "updated"}])
      (doseq [database [@connection (d/history @connection)]
              index [:eavt :aevt :avet]]
        (let [forward (all-pages database
                                 {:index index :components []
                                  :direction :forward :limit 2})
              reverse (all-pages database
                                 {:index index :components []
                                  :direction :reverse :limit 2})]
          (is (same-native-datoms? database index
                                   (vec (d/seek-datoms database index))
                                   forward))
          (is (same-native-datoms? database index
                                   (vec (d/rseek-datoms database index))
                                   reverse)))))))

(deftest lookup-refs-ref-values-and-byte-arrays-use-native-resolution
  (with-database
    (fn [connection]
      (d/transact connection
                  [{:item/id "target" :item/name "target"}
                   {:item/id "a" :item/name "a"
                    :item/friend [:item/id "target"]
                    :item/blob (byte-array [1 2 3])}
                   {:item/id "b" :item/name "b"
                    :item/friend [:item/id "target"]
                    :item/blob (byte-array [1 2 3])}])
      (let [database @connection
            refs (all-pages database
                            {:index :avet
                             :components [:item/friend [:item/id "target"]]
                             :direction :forward
                             :limit 1})
            first-page (d/index-page database
                                     {:index :avet
                                      :components [:item/blob
                                                   (byte-array [1 2 3])]
                                      :direction :forward
                                      :limit 1})
            [_ a v tx added?] (:datahike.index-page/cursor first-page)
            rebuilt-cursor [(-> first-page :datahike.index-page/datoms first :e)
                            a (byte-array v) tx added?]
            second-page (d/index-page database
                                      {:index :avet
                                       :components [:item/blob
                                                    (byte-array [1 2 3])]
                                       :direction :forward
                                       :limit 1
                                       :cursor rebuilt-cursor})]
        (is (= 2 (count refs)))
        (is (= 1 (count (:datahike.index-page/datoms second-page))))
        (is (:datahike.index-page/complete? second-page))))))

(deftest empty-exact-and-limit-plus-one-have_unambiguous_completion
  (with-database
    (fn [connection]
      (d/transact connection [{:item/id "a" :item/name "same"}
                              {:item/id "b" :item/name "same"}
                              {:item/id "c" :item/name "same"}])
      (let [database @connection
            empty-page (d/index-page database
                                     {:index :avet
                                      :components [:item/name "absent"]
                                      :direction :forward :limit 2})
            exact-page (d/index-page database
                                     {:index :avet
                                      :components [:item/name "same"]
                                      :direction :forward :limit 3})
            extra-page (d/index-page database
                                     {:index :avet
                                      :components [:item/name "same"]
                                      :direction :forward :limit 2})]
        (is (= [] (:datahike.index-page/datoms empty-page)))
        (is (:datahike.index-page/complete? empty-page))
        (is (not (contains? empty-page :datahike.index-page/cursor)))
        (is (= 3 (count (:datahike.index-page/datoms exact-page))))
        (is (:datahike.index-page/complete? exact-page))
        (is (not (contains? exact-page :datahike.index-page/cursor)))
        (is (= 2 (count (:datahike.index-page/datoms extra-page))))
        (is (false? (:datahike.index-page/complete? extra-page)))
        (is (= 5 (count (:datahike.index-page/cursor extra-page))))))))

(deftest invalid-cursors-and-result-weight-are-structured
  (with-database
    (fn [connection]
      (d/transact connection [{:item/id "a" :item/name "a"}
                              {:item/id "b" :item/name "b"}])
      (let [database @connection
            page (d/index-page database
                               {:index :aevt :components [:item/name]
                                :direction :forward :limit 1})
            [e a v tx added? :as cursor]
            (:datahike.index-page/cursor page)
            reason (fn [request]
                     (try (d/index-page database request)
                          nil
                          (catch clojure.lang.ExceptionInfo error
                            (:datahike.index-page/reason (ex-data error)))))]
        (is (= :cursor-absent
               (reason {:index :aevt :components [:item/name]
                        :direction :forward :limit 1
                        :cursor [e a v (+ tx 1000) added?]})))
        (is (= :cursor-prefix
               (reason {:index :avet :components [:item/name "b"]
                        :direction :forward :limit 1 :cursor cursor})))
        (is (= :cursor-absent
               (reason {:index :aevt :components [:item/name]
                        :direction :forward :limit 1
                        :cursor [e a v tx (not added?)]})))
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"result-weight budget exceeded"
             (d/index-page database
                           {:index :aevt :components [:item/name]
                            :direction :forward :limit 1
                            :max-result-weight 1})))))))

(deftest paging-touches-only-cursor-verification-and-limit-plus-one
  (with-database
    (fn [connection]
      (d/transact connection
                  (mapv (fn [n] {:item/id (str n) :item/name "same"})
                        (range 20)))
      (let [database @connection
            original-seek dbi/seek-datoms
            touched (atom 0)
            tracked (fn tracked [datoms]
                      (lazy-seq
                       (when-let [datoms (seq datoms)]
                         (cons (do (swap! touched inc) (first datoms))
                               (tracked (rest datoms))))))
            request {:index :avet :components [:item/name "same"]
                     :direction :forward :limit 2}]
        (with-redefs [dbi/seek-datoms
                      (fn [db index components]
                        (tracked (original-seek db index components)))]
          (let [first-page (d/index-page database request)]
            (is (<= @touched 3))
            (reset! touched 0)
            (d/index-page database
                          (assoc request :cursor
                                 (:datahike.index-page/cursor first-page)))
            (is (<= @touched 4))))))))
