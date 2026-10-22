(ns datahike.ids
  "A store's ids: entity ids and transaction ids, each issued once across every branch of
  the store, so an id names one entity or one transaction wherever a datom travels (a
  merge carries a datom with its own e and tx). The writer takes them from the store's
  id source (`datahike.writing/ids`); a speculation (`db-with`) takes none.")

(defprotocol IIds
  (-take! [ids kind] "A fresh id of `kind` (:eid or :tx), issued by no one before.")
  (-reach! [ids kind id] "Every id of `kind` issued from now on is past `id`. nil."))

#?(:clj
   (defn- reach-long! [^java.util.concurrent.atomic.AtomicLong a id]
     ;; jolt: the AtomicLong shim has no accumulateAndGet
     (loop [] (let [c (.get a)]
                (when (and (< c id) (not (.compareAndSet a c id))) (recur))))
     nil))

#?(:clj
   (defrecord ProcessIds [^java.util.concurrent.atomic.AtomicLong eid ^java.util.concurrent.atomic.AtomicLong tx]
     IIds
     (-take! [_ kind] (.incrementAndGet (if (= :eid kind) eid tx)))
     (-reach! [_ kind id] (reach-long! (if (= :eid kind) eid tx) id))))

#?(:clj
   (defn process-ids
     "Ids one process issues for a store, past `max-eid` and `max-tx`: two AtomicLongs. A
     store several processes write takes one source they share instead (`set-ids!`)."
     [max-eid max-tx]
     (->ProcessIds (java.util.concurrent.atomic.AtomicLong. max-eid)
                   (java.util.concurrent.atomic.AtomicLong. max-tx))))
