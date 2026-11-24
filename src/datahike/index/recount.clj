;; Maintenance for databases created before every index branch kept its count.
(ns datahike.index.recount
  (:require [datahike.index.interface :as dii]
            [datahike.writer :as dw]
            [replikativ.logging :as log]))

(def ^:private index-keys
  [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet])

(defn census
  "`{:complete? b :indices {index-key {:branches n :unknown m}}}` for `conn`'s
   database: whether it is marked as having every count, and per index how many
   branches it has and how many are stored without their element count.
   Reads every branch of every index and no leaf. Nothing is written."
  [conn]
  (let [db @(:wrapped-atom conn)
        _ (when (= :released db)
            (log/raise "The connection has been released." {:type :connection-has-been-released}))
        index (get-in db [:config :index])]
    {:complete? (dii/counts-complete? db)
     :indices (into {} (for [k index-keys
                             :let [ix (get db k)
                                   c (when ix (dii/count-census index ix))]
                             :when c]
                         [k c]))}))

(defn recount!
  "Store every branch of every index of `conn`'s database with its element count,
   and mark the database as complete. Returns what `census` said before.
   Supports both `:self` and `:sync` local writers."
  [conn]
  (let [db @(:wrapped-atom conn)
        _ (when (= :released db)
            (log/raise "The connection has been released." {:type :connection-has-been-released}))
        writer-backend (get-in db [:config :writer :backend] :self)]
    (when-not (contains? #{:self :sync} writer-backend)
      (log/raise "recount! needs a connection with a local writer."
                 {:type :recount/needs-local-writer
                  :writer writer-backend}))
    (when-not (= :datahike.index/persistent-set (get-in db [:config :index]))
      (log/raise "Only persistent-set indices keep counts; there is nothing to recount."
                 {:type :recount/unsupported-index :index (get-in db [:config :index])}))
    (let [before (census conn)]
      (when-not (and (:complete? before)
                     (every? (comp zero? :unknown) (vals (:indices before))))
        (let [report @(dw/recount-indices! conn)]
          (when-not (map? report)
            (throw report))))
      before)))
