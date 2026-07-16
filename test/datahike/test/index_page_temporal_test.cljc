(ns datahike.test.index-page-temporal-test
  (:require
   #?(:cljs [cljs.test :refer-macros [deftest is testing]]
      :clj [clojure.test :refer [deftest is testing]])
   [datahike.api :as d]
   [datahike.constants :refer [tx0]]
   [datahike.datom :as dd]
   [datahike.db :as db]
   [datahike.db.interface :as dbi]))

(def test-db
  (db/init-db
   [(dd/datom 1 :item/rank 1 tx0)
    (dd/datom 2 :item/rank 2 tx0)]
   {:item/rank {:db/index true}}))

(defn- page-values [page]
  (mapv :v (:datahike.index-page/datoms page)))

(deftest time-filtered-reverse-page-restores-order-before-prefix
  (let [as-of (db/->AsOfDB test-db tx0)
        rank-one (dd/datom 1 :item/rank 1 tx0 true)
        rank-two (dd/datom 2 :item/rank 2 tx0 true)
        outside (dd/datom 3 :aaa/outside 3 tx0 true)
        request {:index :avet :components [:item/rank]
                 :direction :reverse :limit 1}]
    (with-redefs [dbi/rseek-datoms
                  (fn [_db _index _components]
                    [rank-two outside rank-one])]
      (let [first-page (d/index-page as-of request)
            second-page
            (d/index-page as-of
                          (assoc request :cursor
                                 (:datahike.index-page/cursor first-page)))]
        (is (= [2] (page-values first-page)))
        (is (false? (:datahike.index-page/complete? first-page)))
        (is (= [1] (page-values second-page)))
        (is (:datahike.index-page/complete? second-page))))))

(deftest time-filtered-history-cursor-preserves-polarity
  (let [history (db/->HistoricalDB (db/->AsOfDB test-db tx0))
        retracted (dd/datom 2 :item/rank 2 tx0 false)
        asserted (dd/datom 2 :item/rank 2 tx0 true)
        outside (dd/datom 3 :aaa/outside 3 tx0 true)
        request {:index :avet :components [:item/rank]
                 :direction :reverse :limit 1}]
    (with-redefs [dbi/rseek-datoms
                  (fn [_db _index _components]
                    [asserted outside retracted])]
      (let [first-page (d/index-page history request)
            second-page
            (d/index-page history
                          (assoc request :cursor
                                 (:datahike.index-page/cursor first-page)))]
        (testing "added polarity participates in temporal cursor order"
          (is (= [true]
                 (mapv dd/datom-added
                       (:datahike.index-page/datoms first-page))))
          (is (= [false]
                 (mapv dd/datom-added
                       (:datahike.index-page/datoms second-page)))))))))
