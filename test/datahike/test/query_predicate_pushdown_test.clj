(ns datahike.test.query-predicate-pushdown-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.db :as db]))

(deftest bound-range-predicates-preserve-answers
  (let [database (d/db-with
                  (db/empty-db {:node/next {:db/valueType :db.type/ref}})
                  [[:db/add 1 :node/next 2] [:db/add 2 :node/next 3]
                   [:db/add 3 :node/next 4] [:db/add 4 :node/next 1]])]
    (doseq [comparison ['(< ?lo ?z) '(> ?z ?lo)]]
      (is (= #{[1] [2]}
             (d/q {:find '[?x] :in '[$ [?lo ...]]
                   :where ['[?x :node/next ?z] [comparison] '[(< ?z 4)]]}
                  database [1]))))))
