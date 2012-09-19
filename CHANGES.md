# Changes and TODO


## 0.1 / 2012-Sep-??

* Support for column names collection in `clj-to-db`
* Default spec as a static var `default-dbspec`
* Convenience macros/functions for context-free resources
* Dynamic var `*dbspec*` for dbspec containing the following keys
  * :datasource
  * :connection
  * :dbmetadata
  * :catalog
  * :schema
  * :show-sql
  * :show-sql-fn
  * :clj-to-db
  * :db-to-clj
  * :read-only
  * :fetch-size
  * :query-timeout
* Function to build Connection from supplied parameters
* Macro `with-connection` to get Connection from DataSource and execute body
* Convenience function for converting Clojure to database name
* Convenience function for converting database to Clojure name
* Alternative function for clojure.core/resultset-seq: row-seq
* Schema/tables/columns discovery functions