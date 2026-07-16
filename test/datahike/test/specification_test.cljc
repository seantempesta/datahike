(ns datahike.test.specification-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing]])
   [datahike.api.specification :refer [malli-schema->argslist]]
   [datahike.api.types :as types]
   [malli.core :as m]))

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
