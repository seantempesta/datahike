(ns hooks.superv
  (:require [clj-kondo.hooks-api :as api]))

(defn go-loop-try
  "Analyze the supervisor expression and the macro's ordinary loop."
  [{:keys [node]}]
  (let [[_ supervisor bindings & body] (:children node)]
    {:node (api/list-node
            [(api/token-node 'do) supervisor
             (api/list-node (list* (api/token-node 'loop) bindings body))])}))
