(ns clj-jdbcutil.core-test
  (:require
    [clojure.pprint    :as pp]
    [clj-jdbcutil.core :as spec]
    [clj-dbcp.core     :as dbcp])
  (:import
    (java.sql  Connection ResultSet)
    (javax.sql DataSource))
  (:use clojure.test))


(def query "SELECT 1")


(defn make-datasource
  []
  (dbcp/make-datasource :h2 {:target :memory :database :default}))


(defn echo
  [x]
  (pp/pprint x)
  x)

(def driver "org.h2.Driver")
(def db-url1 "jdbc:h2:mem:db1")
(def db-url2 "jdbc:h2:mem:db2")
(def db-url3 "jdbc:h2:mem:db3")
(def db-url4 "jdbc:h2:mem:db4")

(deftest test-make-connection
  (testing "Make connection without username/password"
    (is (instance? Connection (spec/make-connection driver db-url1))))
  (testing "Make connection with username/password"
    (is (instance? Connection (spec/make-connection driver db-url2 "sa" ""))))
  (testing "Metadata"
    (is (map? (echo (spec/dbmeta
                      (spec/make-connection driver db-url3 "sa" ""))))))
  (testing "Make datasource"
    (is (instance? DataSource (spec/make-datasource driver db-url4)))))


(defn exec-query
  [^Connection conn]
  (is (instance? ResultSet
        (-> (.createStatement conn)
          (.executeQuery query)))))


(deftest test-with-datasource-conn
  (testing "Get connection from DataSource and execute code in context"
    (let [ds (make-datasource)]
      (spec/with-connection
          {:datasource ds}
          (exec-query (:connection spec/*dbspec*)))))
  (testing "Create connection and execute as dbspec"
    (let [sp (spec/make-dbspec (spec/make-connection driver db-url1))]
      (spec/with-connection
        sp
        (exec-query (:connection spec/*dbspec*)))))
  (testing "Create datasource and execute as dbspec"
    (let [sp (spec/make-dbspec (spec/make-datasource driver db-url4))]
      (spec/with-connection
        sp
        (exec-query (:connection spec/*dbspec*))))))


(deftest test-iden-conversion
  (testing "Convert Clojure identifier to database identifier"
    (is (= (spec/db-iden :Hello-Morris) "Hello_Morris")))
  (testing "Convert Clojure identifiers to comma separated database identifiers"
    (is (= (spec/comma-sep-dbiden [:a-a :b-b]) "a_a,b_b")))
  (testing "Convert database identifier to Clojure identifier"
    (is (= (spec/clj-iden "Hello_Morris") :hello-morris))))


(deftest test-db-introspect
  (testing "Database introspection"
    (let [ds (make-datasource)]
      (spec/with-connection
        {:datasource ds}
        (let [dm (.getMetaData ^Connection (:connection spec/*dbspec*))
              ; run tests
              rt (fn [retval ^String case-name]
                   (do
                     (is (vector? retval)
                         (format "%s returns a vector" case-name))
                     (is (or (empty? retval) (every? spec/row? retval))
                         (format "Every element returned by %s should be a map, found %s"
                                 case-name (with-out-str (pp/pprint retval))))))]
          ; get-catalogs
          (rt (spec/get-catalogs dm) "get-catalogs")
          ; get-schemas
          (rt (spec/get-schemas dm)  "get-schemas")
          ; get-tables
          (rt (spec/get-tables dm)   "get-tables")
          (rt (spec/get-tables
                dm
                :catalog        nil
                :schema-pattern nil
                :table-pattern  nil
                :types          (into-array String [])) "get-tables"))))))


(defn test-ns-hook []
  (test-make-connection)
  (test-with-datasource-conn)
  (test-iden-conversion)
  (test-db-introspect))
