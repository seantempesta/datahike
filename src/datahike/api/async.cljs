(ns datahike.api.async
  "CLJS-side helpers for the unified Promise-returning API.

  Datahike's CLJS implementations either return values directly (sync
  ops like `q`, `entity`, `pull` on a db snapshot) or `core.async`
  channels (anything that touches storage). The `datahike.api`
  CLJS expansion wraps every call site through `chan->promise` so
  callers get a uniform Promise contract — `(await (d/transact! ...))`
  works regardless of whether the impl was sync or async internally.

  Error semantics: channel results that are `js/Error` or `ExceptionInfo`
  instances reject the Promise (matching `datahike.js.api`'s
  `maybe-chan->promise`). Plain values resolve."
  (:require [cljs.core.async :refer [<!]])
  (:require-macros [cljs.core.async :refer [go]]))

(defn chan->promise
  "Convert a value-or-channel to a Promise. Non-channel values become
  resolved Promises immediately. Channel values are bridged via a
  short-lived `go` block — exceptions or `Error`-typed channel results
  reject; everything else resolves.

  Internal: `datahike.api`'s `emit-api` macro wraps every CLJS fn in
  this. Direct calls are fine if you want to convert a one-off channel."
  [x]
  (if (satisfies? cljs.core.async.impl.protocols/Channel x)
    (js/Promise.
      (fn [resolve reject]
        (go
          (try
            (let [result (<! x)]
              (if (or (instance? js/Error result)
                      (instance? ExceptionInfo result))
                (reject result)
                (resolve result)))
            (catch :default e
              (reject e))))))
    (js/Promise.resolve x)))
