(ns datahike.api
  "Public API for datahike. Expanded from api.specification.

  In CLJ, each fn is a direct alias to its implementation.

  In CLJS the expansion depends on the spec's `:referentially-transparent?`
  flag:

   - Async (`:referentially-transparent? false`) — `transact!`, `connect`,
     `create-database`, `release`, `branch!`, `merge-db!`, etc. The
     underlying impl returns a `core.async` channel; the wrapper bridges
     it to a `js/Promise` via `datahike.api.async/chan->promise`. Errors
     reject; successes resolve.

   - Pure (`:referentially-transparent? true`) — `q`, `entity`, `pull`,
     `datoms`, `db`, `as-of`, `history`, `with`, etc. The wrapper just
     forwards to the impl; the call returns a plain value, the same as
     on CLJ.

  This mirrors JS idiom — you don't `await` `array.map`, you DO `await`
  `fetch`. With CLJS 1.12.145's `^:async`/`(await ...)` macros (CLJS-3470)
  the calling code reads naturally:

    (defn ^:async run! [cfg]
      (let [conn (await (d/connect cfg))]          ; async — Promise
        (await (d/transact! conn data))            ; async — Promise
        (let [rows (d/q '[:find ?e :where ...] @conn)] ; sync — value
          rows)))

  Callers that want raw channels for async fns can call the underlying
  `datahike.api.impl` / `datahike.connector` symbols directly."
  (:refer-clojure :exclude [filter])
  #?(:cljs (:require-macros [datahike.api :refer [emit-api]]))
  (:require [datahike.connector :as dc]
            [datahike.config :as config]
            [datahike.api.specification :refer [api-specification
                                                host-api-specification
                                                malli-schema->argslist]]
            [datahike.api.impl]
            #?(:cljs [datahike.api.async])
            [datahike.writer :as dw]
            #?(:clj [datahike.http.writer])
            [datahike.writing :as writing]
            [konserve.store]
            [datahike.constants :as const]
            [datahike.core :as dcore]
            [datahike.pull-api :as dp]
            [datahike.query :as dq]
            [datahike.schema :as ds]
            [datahike.tools :as dt]
            [datahike.db :as db #?@(:cljs [:refer [HistoricalDB AsOfDB SinceDB FilteredDB]])]
            [datahike.db.interface :as dbi]
            [datahike.db.transaction :as dbt]
            [datahike.impl.entity :as de])
  #?(:clj
     (:import [clojure.lang Keyword PersistentArrayMap]
              [datahike.db HistoricalDB AsOfDB SinceDB FilteredDB]
              [datahike.impl.entity Entity])))

(defmacro ^:private emit-api []
  (let [cljs? (some? (:js-globals &env))]
    `(do
       ~@(reduce
          (fn [acc [n {:keys [args doc impl referentially-transparent?]}]]
            (conj acc
                  (if (and cljs? (not referentially-transparent?))
                    ;; CLJS async fn — wrap so the call returns a
                    ;; js/Promise (channel impls bridge; throws/Errors
                    ;; reject). Native `(await ...)` works directly.
                    `(defn ~(with-meta n
                              {:arglists `(malli-schema->argslist (quote ~args))
                               :doc      doc})
                       [& args#]
                       (datahike.api.async/chan->promise
                        (apply ~impl args#)))
                    ;; CLJ — or CLJS sync fn — alias straight to the
                    ;; impl. Callers use the result as a value.
                    `(def ~(with-meta n
                             {:arglists `(malli-schema->argslist (quote ~args))
                              :doc      doc})
                       ~impl))))
          ()
          (into (sorted-map)
                (if cljs?
                  api-specification
                  (concat api-specification host-api-specification)))))))

(emit-api)
