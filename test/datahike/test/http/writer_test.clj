(ns datahike.test.http.writer-test
  (:require
   [clojure.test :as t :refer [is deftest testing]]
   [datahike.http.server :refer [start-server stop-server]]
   [datahike.http.writer]
   [datahike.api :as d]))

(deftest test-http-writer
  (testing "Testing distributed datahike.http.writer implementation."
    (let [port 31283
          cfg {:store              {:backend :file
                                    :path "/tmp/distributed_writer"
                                    :id #uuid "17100000-0000-0000-0000-000000000001"}
               :keep-history?      true
               :schema-flexibility :read
               :writer             {:backend :datahike-server
                                    :url (str "http://localhost:" port)
                                    :token "securerandompassword"}}
          conn* (atom nil)
          server (start-server {:port     port
                                :join?    false
                                :dev-mode false
                                :token    "securerandompassword"})]
      (try
        (let [conn (do
                     (when (d/database-exists? cfg)
                       (d/delete-database cfg))
                     (d/create-database cfg)
                     (d/connect cfg))
              _ (reset! conn* conn)]

          (d/transact conn [{:name "Alice"
                             :age  25}])
          (is (= #{[25 "Alice"]}
                 (d/q '[:find ?a ?v
                        :in $ ?a
                        :where
                        [?e :name ?v]
                        [?e :age ?a]]
                      @conn
                      25)))

          (d/transact conn [{:name "Peter"
                             :age  18}])
          (is (= #{[18 "Peter"]}
                 (d/q '[:find ?a ?v
                        :in $ ?a
                        :where
                        [?e :name ?v]
                        [?e :age ?a]]
                      @conn
                      18)))

          (d/release conn)
          (reset! conn* nil)
          (d/delete-database cfg))
        (finally
          (when-let [conn @conn*]
            (d/release conn)
            (when (d/database-exists? cfg)
              (d/delete-database cfg)))
          (stop-server server))))))

(deftest test-http-writer-failure-without-server
  (testing "Db creation fails without writer connection."
    (let [port   38217
          cfg    {:store              {:backend :memory :id #uuid "00170000-0000-0000-0000-000000000017"}
                  :keep-history?      true
                  :schema-flexibility :read
                  :writer             {:backend :datahike-server
                                       :url     (str "http://localhost:" port)
                                       :token   "securerandompassword"}}]
      (is (thrown? Exception
                   (do
                     (d/delete-database cfg)
                     (d/create-database cfg)
                     (d/connect cfg))))))
  (testing "Transact fails without writer connection."
    (let [port 38217
          cfg  {:store              {:backend :memory :id #uuid "00180000-0000-0000-0000-000000000018"}
                :keep-history?      true
                :schema-flexibility :read
                :writer             {:backend :datahike-server
                                     :url     (str "http://localhost:" port)
                                     :token   "securerandompassword"}}
          server-cfg {:store              {:backend :memory :id #uuid "00180000-0000-0000-0000-000000000018"}
                      :keep-history?      true
                      :schema-flexibility :read}]
        ;; make sure the database exists before testing transact
      (do (d/delete-database server-cfg)
          (d/create-database server-cfg))
      (let [conn (d/connect cfg)]
        (try
          (is (thrown? Exception
                       (d/transact conn [{:name "Should fail."}])))
          (finally
            (d/release conn)
            (d/delete-database server-cfg)))))))
