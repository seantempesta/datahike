(ns datahike.test.specification-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing]])
   #?(:clj [datahike.api :as d])
   [datahike.api.specification :refer [api-specification
                                       host-api-specification
                                       malli-schema->argslist]]
   [datahike.api.types :as types]
   [malli.core :as m]))

#?(:clj
   (deftest shallow-weight-is-an-exact-host-only-api
     (let [operation (get host-api-specification 'shallow-weight-within)
           realized (atom 0)
           uncounted (lazy-seq (swap! realized inc) [1 2 3])]
       (testing "the API metadata excludes generated and remote surfaces"
         (is (= :experimental (:stability operation)))
         (is (= [:resource :host] (:categories operation)))
         (is (false? (:supports-remote? operation)))
         (is (:referentially-transparent? operation))
         (is (= '([arg0 arg1]) (:arglists (meta #'d/shallow-weight-within))))
         (is (= (:doc operation) (:doc (meta #'d/shallow-weight-within))))
         (is (not (contains? api-specification 'shallow-weight-within)))
         (is (nil? (get-in (d/capabilities)
                           [:datahike.capability/operations
                            :datahike.operation/shallow-weight-within]))))
       (testing "ordinary eager values have exact structural weight"
         (is (= 0 (d/shallow-weight-within nil 0)))
         (is (= 1 (d/shallow-weight-within :value 1)))
         (is (= 4 (d/shallow-weight-within "abc" 4)))
         (is (= 5 (d/shallow-weight-within [nil :value "ab"] 5)))
         (is (= 5 (d/shallow-weight-within {:value [1 2]} 5))))
       (testing "the bound rejects without traversing uncounted values"
         (is (nil? (d/shallow-weight-within {:value [1 2]} 4)))
         (is (nil? (d/shallow-weight-within :value 0)))
         (is (nil? (d/shallow-weight-within uncounted 100)))
         (is (zero? @realized))))))

(deftest pull-options-have-distinct-exact-shapes
  (is (m/validate types/SPullOptions {:selector [:name] :eid 1}))
  (is (not (m/validate types/SPullOptions {:selector [:name] :eids [1]})))
  (is (not (m/validate types/SPullOptions
                       {:selector [:name] :eid 1 :extra true})))
  (is (m/validate types/SPullManyOptions
                  {:selector [:name] :eids [1 [:person/id "petr"]]}))
  (is (not (m/validate types/SPullManyOptions
                       {:selector [:name] :eid 1})))
  (is (not (m/validate types/SPullManyOptions
                       {:selector [:name] :eids 1})))
  (let [spec (get api-specification 'pull-many)]
    (is (= [:vector [:maybe :map]] (:ret spec)))
    (is (= '([arg0 arg1] [arg0 arg1 arg2])
           (malli-schema->argslist (:args spec))))))

(deftest query-evidence-and-index-page-have-exact-public-shapes
  (is (m/validate types/SQueryAttributeDependencies #{:unqualified :a/name}))
  (is (m/validate types/SQueryAttributeDependencies :all))
  (is (m/validate types/SIndexPageArgs
                  {:index :avet
                   :components [:a/name "A"]
                   :direction :forward
                   :limit 20
                   :cursor [1 :a/name "A" 536870913 true]
                   :max-result-weight 1000}))
  (is (not (m/validate types/SIndexPageArgs
                       {:index :avet :components []
                        :direction :forward :limit 201}))))

(deftest malli-to-argslist-translation
  (testing "Testing core cases of malli to argslist translator."
    ;; Multi-arity: [:function [:=> [:cat Type1] ret] [:=> [:cat] ret]]
    (is (= (malli-schema->argslist '[:function
                                     [:=> [:cat :datahike/SConfig] :any]
                                     [:=> [:cat] :any]])
           '([arg0] [])))

    ;; Single arity: [:=> [:cat Type1 Type2] ret]
    (is (= (malli-schema->argslist '[:=> [:cat :datahike/SConnection :datahike/STransactions] :any])
           '([arg0 arg1])))

    ;; Multi-arity with rest args: [:function [:=> [:cat Type] ret] [:=> [:cat [:or ...] [:* :any]] ret]]
    (is (= (malli-schema->argslist '[:function
                                     [:=> [:cat :datahike/SQueryArgs] :any]
                                     [:=> [:cat [:or :vector :map] [:* :any]] :any]])
           '([arg0] [arg0 arg1])))

    ;; Multi-arity simple: [:function [:=> [:cat Type1 Type2] ret] [:=> [:cat Type1 Type3 Type4] ret]]
    (is (= (malli-schema->argslist '[:function
                                     [:=> [:cat :datahike/SDB :datahike/SPullOptions] :any]
                                     [:=> [:cat :datahike/SDB :any :datahike/SEId] :any]])
           '([arg0 arg1] [arg0 arg1 arg2])))))
