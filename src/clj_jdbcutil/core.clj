(ns clj-jdbcutil.core
  "JDBC utility functions and vars to provide building blocks for libraries."
  (:require
    [clojure.set]
    [clojure.string :as sr]
    [clojure.pprint :as pp]
    [clj-jdbcutil.internal :as in])
  (:import
    (java.util List Properties)
    (java.sql  Connection DatabaseMetaData DriverManager ResultSet)
    (javax.sql DataSource)))


(def ^{:doc "clj-jdbcutil version (only major and minor)"}
      version [0 1])


(defn ^Connection make-connection
  "Create JDBC connection from supplied parameters. You must ensure that the
  JDBC driver already exists in the classpath."
  ([^String driver-classname
    ^String jdbc-url]
    (Class/forName driver-classname)
    (DriverManager/getConnection jdbc-url))
  ([^String driver-classname
    ^String jdbc-url
    ^Properties props]
    (Class/forName driver-classname)
    (DriverManager/getConnection jdbc-url props))
  ([^String driver-classname
    ^String jdbc-url
    ^String username
    ^String password]
    (Class/forName driver-classname)
    (DriverManager/getConnection jdbc-url username password)))


(defn ^DataSource make-datasource
  "Create a non-connection-pooling JDBC DataSource. You MAY use this function
  for testing and experimentation. You SHOULD NOT use this for production."
  ([^String driver-classname ^String jdbc-url]
    (proxy [DataSource] []
      (getConnection
        ([]
          (make-connection driver-classname jdbc-url))
        ([^String username ^String password]
          (make-connection driver-classname jdbc-url username password)))))
  ([^String driver-classname ^String jdbc-url ^String username ^String password]
    (proxy [DataSource] []
      (getConnection
        ([]
          (make-connection driver-classname jdbc-url username password))
        ([^String username ^String password]
          (make-connection driver-classname jdbc-url username password))))))


(defn ^Connection get-datasource-connection
  "Obtain and return a java.sql.Connection object from specified argument of
  type javax.sql.DataSource"
  [^DataSource ds]
  (.getConnection ds))


;; ----- The Db-Spec definition (data structure) -----


(def ^{:doc "Default dbspec; see also: *dbspec*"} default-dbspec
  (array-map
   :datasource    nil
   :connection    nil
   :dbmetadata    (array-map)
   :catalog       nil
   :schema        nil
   :read-only     false
   :show-sql      true
   :show-sql-fn   (fn [^String sql] (pp/pprint sql))
   :clj-to-db     (fn c2d [iden]
                    (cond
                      (string? iden) iden
                      (coll?   iden) (into [] (map c2d iden))
                      :else          (apply str (replace {\- \_}
                                                         (in/as-string iden)))))
   :db-to-clj     (fn [^String iden]
                    (keyword (apply str (replace {\_ \-}
                                          (sr/lower-case iden)))))
   :fetch-size    1000
   :query-timeout 0))


(def ^{:doc "Database configuration specification - map of values with the following
  Well-known keys:
    :datasource    (javax.sql.DataSource, default: nil)
                    1. When you rebind this var with :datasource you SHOULD also
                       include a cached :dbmetadata value.
                    2. Everytime a connection is taken from the datasource you
                       SHOULD re-bind this var to include the :connection object.
    :connection    (java.sql.Connection, default: nil)
                    1. If the connection is not taken from a datasource you SHOULD
                       include a cached :dbmetadata value while re-binding.
    :dbmetadata    (map, default: empty map)
                    1. This is usually result of dbmeta function.
    :catalog       (Clojure form - String, Keyword etc.; default nil)
                    1. Catalog name - SHOULD be converted using db-iden
    :schema        (Clojure form - String, Keyword etc.; default nil)
                    1. Schema name - SHOULD be converted using db-iden
    :read-only     (Boolean default false)
                    1. If true, all READ operations should execute as usual and
                       all WRITE operations should throw IllegalStateException.
                    2. I false, both READ and WRITE operations execute fine
    :show-sql      (Boolean default true)
                    1. If true, SQL statements should be printed.
    :show-sql-fn   (function, default: to print SQL statement using `println`)
                    You may like to rebind this to fn that prints via a logger
    :clj-to-db     (function, default: to string, replace '-' => '_')
                    1. Dictates how should identifiers be converted from
                       Clojure to the Database.
                    2. The default value is the least common denominator that
                       works with most databases. Override as necessary.
    :db-to-clj     (function, default: to lower-case keyword, replace '_' => '-')
                    1. Dictates how should identifiers be converted from the
                       Database to Clojure.
                    2. The default value is the least common denominator that
                       works with most databases. Override as necessary.
    :fetch-size    (Integer, default: 1000)
                    1. Number of rows to fetch per DB roundtrip. Helps throttle
                       as well as optimize large DB reads. 0 means unlimited.
    :query-timeout (Integer, default: 0)
                    1. Number of seconds to wait for query to execute, after
                       which timeout occurs raising SqlException. Not all JDBC
                       drivers support this so check driver manual before use.

  Libraries
  1. MAY expose their own API to re-bind this var to new values
  2. MUST NOT alter the type/semantics of the well-defined keys
  3. MAY introduce custom keys with unique prefixes e.g. :com.foo.con-pool-name
     in order to prevent name collision with other libraries."
       :dynamic true}
      *dbspec* default-dbspec)


;; ===== Convenience macros/functions for binding values into DB-Spec =====


(defmacro with-connection
  [spec & body]
  `(let [e-spec# (into (or *dbspec* {}) ~spec)]
     (if (:connection e-spec#)
       (binding [*dbspec* e-spec#] ~@body)
       (with-open [conn# (.getConnection ^DataSource (:datasource e-spec#))]
         (binding [*dbspec* (merge e-spec# {:connection conn#})] ~@body)))))


;(defmacro with-dbspec
;  "Convenience macro to bind *dbspec* to `spec` merged into *dbspec* and
;  execute body of code in that context."
;  [spec & body]
;  `(binding [*dbspec* (into (or *dbspec* {}) ~spec)]
;     ~@body))


;(defn value
;  "Assuming that (1) a function is not a value, (2) and that `v` can either be
;  a no-arg function that returns value, or a value itself, return value."
;  [v]
;  (if (fn? v) (v)
;    v))


;; --- Resource that do not require context-building or cleanup ---


(defn assoc-kvmap
  "Associate specified kvmap with dbspec."
  [spec kvmap] {:post [(map? %)]
                :pre  [(map? spec) (map? kvmap) (every? keyword? (keys kvmap))]}
  (into spec kvmap))


(defn assoc-datasource
  "Add :datasource object to the spec. `ds` can be a DataSource object or a
  no-arg function that returns one.
  Example usage:
  ;; assume: `ds` is a javax.sql.DataSource object
  (def spec (assoc-datasource {} ds))
  (with-dbspec spec
    ;; code that operates on the datasource
    ..body..)"
  [spec ds] {:post [(map? %)]
             :pre  [(map? spec) (or (instance? DataSource ds) (fn? ds))]}
  (assoc-kvmap spec {:datasource ds ; (value ds)
                     :connection nil}))


(defn assoc-readonly
  "Add :read-only flag to the spec. `flag` can be either a boolean value or a
  no-arg function that returns one."
  ([spec flag] {:post [(map? %)]
                :pre  [(map? spec)]}
    (assoc-kvmap spec {:read-only flag ; (value flag)
                       }))
  ([spec] {:post [(map? %)]
           :pre  [(map? spec)]}
    (assoc-readonly spec true)))


(defn read-only?
  "Return true if spec is in read-only mode, false otherwise."
  ([spec] {:pre [(map? spec)]}
    (if (:read-only spec) true
      false))
  ([] (read-only? *dbspec*)))


(defn verify-writable
  "Return true if spec is NOT in :read-only mode, throw IllegalStateException
  otherwise."
  ([spec] {:pre [(map? spec)]}
    (or (not (read-only? spec))
      (throw (IllegalStateException.
               (format "Spec is in READ-ONLY mode: %s" (with-out-str
                                                         (pp/pprint spec)))))))
  ([]
    (verify-writable *dbspec*)))


;; --- Resources with context-building (and possibly cleanup) ---


;; TODO - wrappers below MUST be either private, or deleted

;(defn wrap-dbspec
;  "Bind *dbspec* to 'spec' and execute f in that context. Libraries MAY provide
;  their own middleware to modify the values in *dbspec*."
;  [spec f] {:post [(fn? %)]
;            :pre  [(fn? f) (map? spec)]}
;  (fn [& args]
;    (binding [*dbspec* (into (or *dbspec* {}) spec)]
;      (apply f args))))
;
;
;(defn wrap-datasource
;  "Add a :datasource object to the spec and excute f in that context. You may
;  need use this when dealing with multiple datasources at the same time. For
;  normal scenarios where only one datasource is required, consider using
;  `assoc-datasource` and `with-datasource`."
;  [^DataSource ds f] {:post [(fn? %)]
;                      :pre  [(fn? f) (instance? DataSource ds)]}
;  (wrap-dbspec {:datasource ds
;                :connection nil}
;    f))
;
;
;(defn wrap-read-only
;  "Add a :read-only flag to the spec and excute f in that context. `flag` is
;  either a no-arg function that returns boolean flag, or is a boolean flag.
;  `f` is the application function to be wrapped."
;  [flag f] {:post [(fn? %)]
;            :pre  [(fn? f)]}
;  (let [b (if (fn? flag) (flag)
;            (or (and flag true) false))]
;    (wrap-dbspec {:read-only b}
;      f)))
;
;
;(defn wrap-datasource-conn
;  "Middleware to associate a new :connection object in *dbspec*. Assuming `f`
;  is a fn that may read *dbspec*, return fn that binds :connection in *dbspec*
;  to a connection obtained from specified datasource, or from :datasource
;  object in *dbspec*. The connection will be closed after the body is executed."
;  ([^DataSource ds f] {:post [(fn? %)]
;                       :pre  [(fn? f) (instance? DataSource ds)]}
;    (fn [& args]
;      (let [conn ^Connection (.getConnection ds)]
;        (try
;          (apply (wrap-dbspec {:connection conn}
;                   f) args)
;          (finally
;            (try (.close conn)
;              (catch Exception _)))))))
;  ([f] {:post [(fn? %)]
;        :pre  [(fn? f)]}
;    (wrap-datasource-conn (:datasource *dbspec*) f)))
;
;
;(defn wrap-connection
;  "Middleware to ensure :connection object in *dbspec*. Return fn that should
;  be passed same arguments that you would pass to `f`. The returned fn ensures
;  that :connection is available before it invokes `f` - obtaining one from
;  :datasource if unavailable."
;  ([^Connection conn f] {:post [(fn? %)]
;                         :pre  [(fn? f) (instance? Connection conn)]}
;    (fn [& args]
;      (wrap-dbspec {:connection conn}
;        f)))
;  ([f] {:post [(fn? %)]
;        :pre  [(fn? f)]}
;    (fn [& args]
;      (if (:connection *dbspec*) (apply f args)
;        (apply (wrap-datasource-conn f) args)))))
;
;
;(defmacro with-connection
;  "Macro (like c.c.sql/with-connection) to execute body of code in the context
;  of a spec while ensuring that a connection exists."
;  [spec & body] {:pre [`(map? ~spec)]}
;  `(with-dbspec ~spec
;     (let [g# (wrap-connection (fn [] ~@body))]
;       (g#))))


;; ----- Factory functions for creating DB-Spec -----


(defmulti make-dbspec
  "Make DB-spec based on class of the parameter passed. You can use the return
  value to bind to *dbspec* (using 'with-spec')."
  class)


(declare dbmeta)

(defmethod make-dbspec DataSource [ds]
  {:datasource ds})


(defmethod make-dbspec Connection [conn]
  {:connection conn})


;; ----- Clojure/Database identifier conversion -----


(defn ^String db-iden
  "Convert Clojure form to database identifier (schema/table/column name etc.)"
  [clj-form] {:post [(string? %)]
              :pre  [(not (nil? clj-form))]}
  ((:clj-to-db *dbspec*) clj-form))


(defn ^String comma-sep-dbiden
  "Convert a list of Clojure forms to comma separated string of database
  identifiers."
  [clj-forms] {:post [(string? %)]
               :pre  [(coll? clj-forms)]}
  (apply str (interpose \, (map db-iden clj-forms))))


(defn clj-iden
  "Convert database identifier (schema/table/column name etc.) to Clojure form."
  [^String db-form] {:post [(not (nil? %))]
                     :pre  [(string? db-form)]}
  ((:db-to-clj *dbspec*) db-form))


;; ----- Result-set and Metadata functions -----


(defprotocol IRow
  (labels [this] "Return a vector of labels in the order they exist in the row")
  (asVec  [this] "Return a vector of values in the order they exist in the row")
  (asMap  [this] "Return the map representation"))


;; A row is a sequence of column values. It also behaves as a map - you can
;; access column values using integer index (like vector) or using the Clojure
;; form of the column label (like a map).
;; Note: Rows may have duplicate column titles (but index is always unique).
(deftype Row [collabel-vec colvalue-vec colvalue-map conv-fn]
  clojure.lang.ILookup
    (valAt  [this key not-found] (conv-fn key not-found))
    (valAt  [this key]           (.valAt this key nil))
  clojure.lang.IFn
    (invoke [this key not-found] (.valAt this key not-found))
    (invoke [this key]           (.valAt this key nil))
    (invoke [this]               (.valAt this 0))
  IRow
    (labels [this] collabel-vec)
    (asVec  [this] colvalue-vec)
    (asMap  [this] colvalue-map)
  Object
    (toString [this] (with-out-str
                       (println collabel-vec)
                       (println colvalue-vec)
                       (println colvalue-map)))
    (hashCode [this] (.hashCode (.toString this)))
    (equals   [this that] (and (instance? Row that)
                            (= colvalue-vec (.asVec ^Row that))
                            (= colvalue-map (.asMap ^Row that)))))


(defn row?
  "Return true if it is a row instance, false otherwise."
  [x]
  (instance? Row x))


(defn ^Row make-row
  "Create an instance of Row type."
  ([collabel-vec colvalue-vec colvalue-map conv-fn]
    (Row. collabel-vec colvalue-vec colvalue-map conv-fn))
  ([collabel-vec colvalue-vec colvalue-map]
    (make-row collabel-vec colvalue-vec colvalue-map
      (fn [key not-found]
        (cond
          (contains? colvalue-vec key) (colvalue-vec key)
          (contains? colvalue-map key) (colvalue-map key)
          :else not-found)))))


;; Enable pretty printing
(defmethod pp/simple-dispatch Row [^Row x]
  (pp/pprint (.asMap x)))


(defn row-seq
  "Create and return a lazy sequence of Row instances corresponding to
  the rows in the java.sql.ResultSet rs
  Note: This function is a modified version of clojure.core/resultset-seq and
        solves the following issues:
   1. Maintains order of columns in a row (resultset-seq maintains order too)
   2. Preserves original column titles in the order they exist in the resultset
   3. All columns can be accessed using corresponding integer index (zero based)
   4. Duplicate column titles (and hence values) can exist in a row
   5. Lets you define how to convert column label to Clojure form"
  [^ResultSet rs]
  (let [rsmeta     (. rs (getMetaData))
        idxs       (range 1 (inc (. rsmeta (getColumnCount))))
        labels     (map (fn [i] (. rsmeta (getColumnLabel i))) idxs)
        keys       (map clj-iden (distinct labels))
        row-struct (apply create-struct keys)
        create-row (fn [colvalue-seq]
                     (make-row (into [] labels) (into [] colvalue-seq)
                       (reduce into (struct row-struct)
                         (map (fn [n ^String k] {n (.getObject rs k)})
                           keys labels))))
        row-values (fn [] (map (fn [^Integer i] (. rs (getObject i))) idxs))
        rows (fn thisfn []
               (when (. rs (next))
                 (cons (create-row (row-values)) (lazy-seq (thisfn)))))]
    (rows)))


(defn colvalue-seq
  "Return column values as a seq from rows for the given column-key."
  [rows column-key]
  (map #(get % column-key) rows))


(defn dbmeta
  "Return a map of database metadata.
  See also: http://download.oracle.com/javase/6/docs/api/java/sql/DatabaseMetaData.html"
  [^Connection conn]
  (let [metadata ^DatabaseMetaData (.getMetaData conn)]
    (array-map
      ;; (string/numeric) basic information
      :product-name           (.getDatabaseProductName    metadata)
      :product-version        (.getDatabaseProductVersion metadata)
      :driver-name            (.getDriverName             metadata)
      :driver-version         (.getDriverVersion          metadata)
      :url                    (.getURL                    metadata)
      :username               (.getUserName               metadata)
      ;; (int) major/minor version numbers
      :db-major-version       (.getDatabaseMajorVersion   metadata)
      :db-minor-version       (.getDatabaseMinorVersion   metadata)
      :driver-major-version   (.getDriverMajorVersion     metadata)
      :driver-minor-version   (.getDriverMinorVersion     metadata)
      :jdbc-major-version     (.getJDBCMajorVersion       metadata)
      :jdbc-minor-version     (.getJDBCMinorVersion       metadata)
      ;; (string) terms and quotes, separators
      :ident-quote-string     (.getIdentifierQuoteString  metadata)
      :term-catalog           (.getCatalogTerm            metadata)
      :term-proc              (.getProcedureTerm          metadata)
      :term-schema            (.getSchemaTerm             metadata)
      :sep-catalog            (.getCatalogSeparator       metadata)
      :extra-name-chars       (.getExtraNameCharacters    metadata)
      ;; (string) comma separated names
      :sql-keywords           (.getSQLKeywords            metadata)
      :string-functions       (.getStringFunctions        metadata)
      :math-functions         (.getNumericFunctions       metadata)
      :system-functions       (.getSystemFunctions        metadata)
      :timedate-functions     (.getTimeDateFunctions      metadata)
      ;; (boolean) access information
      :all-procs-callable     (.allProceduresAreCallable  metadata)
      :all-tables-selectable  (.allTablesAreSelectable    metadata)
      :read-only              (.isReadOnly                metadata)
      ;; (boolean) null values info
      :null-plus-nonnull-null (.nullPlusNonNullIsNull     metadata)
      :nulls-sorted-at-end    (.nullsAreSortedAtEnd       metadata)
      :nulls-sorted-at-start  (.nullsAreSortedAtStart     metadata)
      :nulls-sorted-high      (.nullsAreSortedHigh        metadata)
      :nulls-sorted-low       (.nullsAreSortedLow         metadata)
      ;; (boolean) DDL, DML and their relation with transactions
      :ddl-commits-txn        (.dataDefinitionCausesTransactionCommit    metadata)
      :txn-ignores-ddl        (.dataDefinitionIgnoredInTransactions      metadata)
      :txn-supports-ddl-dml   (.supportsDataDefinitionAndDataManipulationTransactions metadata)
      :txn-supports-dml-only  (.supportsDataManipulationTransactionsOnly metadata)
      ;; (int) maximum length
      :maxlen-binary-literal  (.getMaxBinaryLiteralLength metadata)
      :maxlen-catalog-name    (.getMaxCatalogNameLength   metadata)
      :maxlen-char-literal    (.getMaxCharLiteralLength   metadata)
      :maxlen-column-name     (.getMaxColumnNameLength    metadata)
      :maxlen-cursor-name     (.getMaxCursorNameLength    metadata)
      :maxlen-index           (.getMaxIndexLength         metadata)
      :maxlen-proc-name       (.getMaxProcedureNameLength metadata)
      :maxlen-schema-name     (.getMaxSchemaNameLength    metadata)
      :maxlen-statement       (.getMaxStatementLength     metadata)
      :maxlen-table-name      (.getMaxTableNameLength     metadata)
      :maxlen-user-name       (.getMaxUserNameLength      metadata)
      ;; (int) max count
      :max-columns-in-groupby (.getMaxColumnsInGroupBy    metadata)
      :max-columns-in-index   (.getMaxColumnsInIndex      metadata)
      :max-columns-in-orderby (.getMaxColumnsInOrderBy    metadata)
      :max-columns-in-select  (.getMaxColumnsInSelect     metadata)
      :max-columns-in-table   (.getMaxColumnsInTable      metadata)
      :max-connections        (.getMaxConnections         metadata)
      :max-row-size           (.getMaxRowSize             metadata)
      :max-statements         (.getMaxStatements          metadata)
      :max-tables-in-select   (.getMaxTablesInSelect      metadata)
      ;; (boolean) LOB information
      :lob-locators-update-copy          (.locatorsUpdateCopy                     metadata)
      :max-rowsize-includes-blobs        (.doesMaxRowSizeIncludeBlobs             metadata)
      ;; (boolean) case sensitivity of identifiers and storage
      :case-sensitive-ident              (.supportsMixedCaseIdentifiers           metadata)
      :case-sensitive-quoted-ident       (.supportsMixedCaseQuotedIdentifiers     metadata)
      :stores-ident-in-lower-case        (.storesLowerCaseIdentifiers             metadata)
      :stores-ident-in-mixed-case        (.storesMixedCaseIdentifiers             metadata)
      :stores-ident-in-upper-case        (.storesUpperCaseIdentifiers             metadata)
      :stores-quoted-ident-in-lower-case (.storesLowerCaseQuotedIdentifiers       metadata)
      :stores-quoted-ident-in-mixed-case (.storesMixedCaseQuotedIdentifiers       metadata)
      :stores-quoted-ident-in-upper-case (.storesUpperCaseQuotedIdentifiers       metadata)
      ;; (boolean) support for ANSI-92 and ODBC
      :sup-ansi92-sql-entrylevel         (.supportsANSI92EntryLevelSQL            metadata)
      :sup-ansi92-sql-intermediate       (.supportsANSI92IntermediateSQL          metadata)
      :sup-ansi92-sql-full               (.supportsANSI92FullSQL                  metadata)
      :sup-odbc-minimum-sql-grammar      (.supportsMinimumSQLGrammar              metadata)
      :sup-odbc-core-sql-grammar         (.supportsCoreSQLGrammar                 metadata)
      :sup-odbc-extended-sql-grammar     (.supportsExtendedSQLGrammar             metadata)
      ;; (boolean) support for DML features
      :sup-expr-in-orderby               (.supportsExpressionsInOrderBy           metadata)
      :sup-outer-joins                   (.supportsOuterJoins                     metadata)
      :sup-limited-outer-joins           (.supportsLimitedOuterJoins              metadata)
      :sup-full-outer-joins              (.supportsFullOuterJoins                 metadata)
      :sup-get-generated-keys            (.supportsGetGeneratedKeys               metadata)
      :sup-groupby                       (.supportsGroupBy                        metadata)
      :sup-groupby-beyond-select         (.supportsGroupByBeyondSelect            metadata)
      :sup-groupby-unrelated             (.supportsGroupByUnrelated               metadata)
      :sup-orderby-unrelated             (.supportsOrderByUnrelated               metadata)
      :sup-like-escape-clause            (.supportsLikeEscapeClause               metadata)
      :sup-subqueries-in-compare         (.supportsSubqueriesInComparisons        metadata)
      :sup-subqueries-in-exists          (.supportsSubqueriesInExists             metadata)
      :sup-subqueries-in-in              (.supportsSubqueriesInIns                metadata)
      :sup-subqueries-in-quantifieds     (.supportsSubqueriesInQuantifieds        metadata)
      :sup-union                         (.supportsUnion                          metadata)
      :sup-union-all                     (.supportsUnionAll                       metadata)
      ;; (boolean) support for cursors
      :sup-select-for-update             (.supportsSelectForUpdate                metadata)
      ;; (boolean) support for correlation
      :sup-table-correlation-names       (.supportsTableCorrelationNames          metadata)
      :sup-correlated-subqueries         (.supportsCorrelatedSubqueries           metadata)
      :sup-diff-table-correlation-names  (.supportsDifferentTableCorrelationNames metadata)
      ;; (boolean) support for runtime features
      :sup-batch-updates                 (.supportsBatchUpdates                   metadata)
      :sup-column-aliasing               (.supportsColumnAliasing                 metadata)
      :sup-jdbc-convert-fn               (.supportsConvert                        metadata)
      :sup-multiple-results              (.supportsMultipleResultSets             metadata)
      :sup-txns                          (.supportsTransactions                   metadata)
      :sup-multiple-txns                 (.supportsMultipleTransactions           metadata)
      :sup-notnull-columns               (.supportsNonNullableColumns             metadata)
      :sup-positioned-delete             (.supportsPositionedDelete               metadata)
      :sup-positioned-update             (.supportsPositionedUpdate               metadata)
      :sup-savepoints                    (.supportsSavepoints                     metadata)
      :sup-statement-pooling             (.supportsStatementPooling               metadata)
      :sup-open-cursors-across-commit    (.supportsOpenCursorsAcrossCommit        metadata)
      :sup-open-cursors-across-rollback  (.supportsOpenCursorsAcrossRollback      metadata)
      :sup-open-stmts-across-commit      (.supportsOpenStatementsAcrossCommit     metadata)
      :sup-open-stmts-across-rollback    (.supportsOpenStatementsAcrossRollback   metadata)
      ;; (boolean) supprt for DDL features
      :sup-altertable-addcolumn          (.supportsAlterTableWithAddColumn        metadata)
      :sup-altertable-dropcolumn         (.supportsAlterTableWithDropColumn       metadata)
      :sup-integrity-enhancement         (.supportsIntegrityEnhancementFacility   metadata)
      ;; (boolean) support for stored procedure calls
      :sup-proc                          (.supportsStoredProcedures               metadata)
      :sup-proc-multiple-results         (.supportsMultipleOpenResults            metadata)
      :sup-proc-named-params             (.supportsNamedParameters                metadata)
      :sup-proc-syntax-for-functions     (.supportsStoredFunctionsUsingCallSyntax metadata)
      ;; (boolean) whether schemas supported in various kinds of statements
      :sup-schemas-in-dml                (.supportsSchemasInDataManipulation      metadata)
      :sup-schemas-in-indexdefs          (.supportsSchemasInIndexDefinitions      metadata)
      :sup-schemas-in-privildefs         (.supportsSchemasInPrivilegeDefinitions  metadata)
      :sup-schemas-in-proccalls          (.supportsSchemasInProcedureCalls        metadata)
      :sup-schemas-in-tabledefs          (.supportsSchemasInTableDefinitions      metadata)
      ;; (boolean) whether catalogs supported in various kinds of statements
      :sup-catalogs-in-dml               (.supportsCatalogsInDataManipulation     metadata)
      :sup-catalogs-in-indexdefs         (.supportsCatalogsInIndexDefinitions     metadata)
      :sup-catalogs-in-privildefs        (.supportsCatalogsInPrivilegeDefinitions metadata)
      :sup-catalogs-in-proccalls         (.supportsCatalogsInProcedureCalls       metadata)
      :sup-catalogs-in-tabledefs         (.supportsCatalogsInTableDefinitions     metadata)
      ;; (sequence of maps) assorted ResultSet attributes
      :table-types (into [] (row-seq (.getTableTypes metadata)))
      :type-info   (into [] (row-seq (.getTypeInfo   metadata))))))


;; ----- Database introspection -----


(defn ^DatabaseMetaData get-dbmeta
  "Return DatabaseMetaData object from Connection conn. Not to be confused with
  dbmeta function that returns a map."
  [^Connection conn]
  (.getMetaData conn))


(defn get-catalogs
  "Return a vector of catalogs in the database, where each catalog information
  is a map with column :table-cat (depending on you 'db-to-clj' configuration)
  and a string value (i.e. the catalog name).
  See also:
  http://j.mp/eNWOa8 (Java 6 API, class DatabaseMetaData, method getCatalogs)"
  [^DatabaseMetaData dm]
  (into [] (row-seq (.getCatalogs dm))))


(defn get-schemas
  "Retrieve a vector of schema names (each as a map of column-value pairs)
  available in this database. The schema columns are:
   :table-schem   - String:  schema name
   :table-catalog - String:  catalog name (may be nil)
   :is-default    - Boolean: whether this is the default schema
  Args:
   dm  (java.sql.DatabaseMetaData)
  See also:
  http://j.mp/gSlOtD (Java 6 API, class DatabaseMetaData, method getSchemas)"
  [^DatabaseMetaData dm]
  (into [] (row-seq (.getSchemas dm))))


(defn get-tables
  "Return a vector of table descriptions in database. By default include only
  all tables in current database/catalog/schema. Depending on the 'db-to-clj'
  configuration, each table description has the following columns:
   :table-cat                 String => table catalog (may be nil)
   :table-schem               String => table schema (may be nil)
   :table-name                String => table name
   :table-type                String => table type. Typical types are
                                        \"TABLE\", \"VIEW\", \"SYSTEM TABLE\",
                                        \"GLOBAL TEMPORARY\", \"LOCAL TEMPORARY\",
                                        \"ALIAS\", \"SYNONYM\".
   :remarks                   String => explanatory comment on the table
   :type-cat                  String => the types catalog (may be nil)
   :type-schem                String => the types schema (may be nil)
   :type-name                 String => type name (may be nil)
   :self-referencing-col-name String => name of the designated \"identifier\"
                                        column of a typed table (may be nil)
   :ref-generation            String => specifies how values in :self-referencing-col-name
                                        are created. Values are \"SYSTEM\",
                                        \"USER\", \"DERIVED\". (may be nil)
   :sql                       String => the SQL/DDL used to create it (may be nil)
  Arguments:
   dm (java.sql.DatabaseMetaData)
  Optional arguments:
   :catalog        (String) a catalog name; must match the catalog name as it is
                            stored in the database; \"\" retrieves those without
                            a catalog; nil means that the catalog name should not
                            be used to narrow the search
   :schema-pattern (String) short key name - :schema
                            a schema name pattern; must match the schema name as
                            it is stored in the database; \"\" retrieves those
                            without a schema; nil means that the schema name
                            should not be used to narrow the search
   :table-pattern  (String) short key name - :table
                            a table name pattern; must match the table name as
                            it is stored in the database; nil selects all
   :types          (String) a list of table types, which must be from the list
                            of table types returned from function dbmeta
                            (key :table-types, typical values listed below);
                            nil returns all types
                             \"TABLE\", \"VIEW\", \"SYSTEM TABLE\",
                             \"GLOBAL TEMPORARY\", \"LOCAL TEMPORARY\",
                             \"ALIAS\", \"SYNONYM\"
  See also:
  http://j.mp/dUeYXT (Java 6 API, class DatabaseMetaData, method getTables)"
  [^DatabaseMetaData dm
   & {:keys [catalog
             schema-pattern
             table-pattern
             types]
      :or {catalog        nil
           schema-pattern nil
           table-pattern  nil
           types          (into-array
                            String ["TABLE"])}
      :as opt}]
  {:pre [(clojure.set/subset? (set (keys opt))
           #{:catalog
             :schema-pattern
             :table-pattern
             :types})]}
  (let [rs (.getTables dm
             ^String catalog       ^String schema-pattern
             ^String table-pattern
             ^"[Ljava.lang.String;" (#(or (and (coll? %) (into-array String %))
                                        %) types))]
    (into [] (row-seq rs))))


(defn table-names
  "Return table names from the collection returned by get-tables fn."
  [rows]
  (let [tname (clj-iden "TABLE_NAME")]
    (into [] (colvalue-seq rows tname))))


(defn get-columns
  "Retrieve a description of table columns available in the specified catalog.
  Only column descriptions matching the catalog, schema, table and column name
  criteria are returned. They are ordered by:
    TABLE_CAT,TABLE_SCHEM, TABLE_NAME, and ORDINAL_POSITION.
  Each column description has the following columns:
    :table-cat          String => table catalog (may be null)
    :table-schem        String => table schema (may be null)
    :table-name         String => table name
    :column-name        String => column name
    :data-type             int => SQL type from java.sql.Types
    :type-name          String => Data source dependent type name, for a User-Defined-Type
                                  the type name is fully qualified
    :column-size           int => column size.
                                  For numeric data, this is the maximum precision.
                                  For character data, this is the length in characters.
                                  For datetime datatypes, this is the length in
                                   characters of the String representation
                                   (assuming the maximum allowed precision of the
                                   fractional seconds component).
                                  For binary data, this is the length in bytes.
                                  For the ROWID datatype, this is the length in bytes.
                                  Nil is returned for data types where the column
                                   size is not applicable.
    :buffer-length    (unused)
    :decimal-digits        int => the number of fractional digits. Nil is
                                  returned for data types where :decimal-digits
                                  is not applicable.
    :num-prec-radix        int => Radix (typically either 10 or 2)
    :nullable              int => is NULL allowed (see following values)
                                    columnNoNulls - might not allow NULL values
                                    columnNullable - definitely allows NULL values
                                    columnNullableUnknown - nullability unknown
    :remarks            String => comment describing column (may be nil)
    :column-def         String => default value for the column, which should be
                                  interpreted as a string when the value is
                                  enclosed in single quotes (may be nil)
    :sql-data-type         int => unused
    :sql-datetime-sub      int => unused
    :char-octet-length     int => for char types the maximum number of bytes in
                                  the column
    :ordinal-position      int => index of column in table (starting at 1)
    :is-nullable-string String => ISO rules are used to determine the nullability
                                  for a column.
                                    YES --- if the parameter can include NULLs
                                    NO --- if the parameter cannot include NULLs
                                    empty string --- if the nullability for the
                                                     parameter is unknown
    :scope-catalog      String => catalog of table that is the scope of a reference
                                  attribute (nil if :data-type isn't REF)
    :scope-schema       String => schema of table that is the scope of a reference
                                  attribute (nil if the :data-type isn't REF)
    :scope-table        String => table name that this the scope of a reference
                                  attribure (nil if the :data-type isn't REF)
    :source-data-type    short => source type of a distinct type or user-generated
                                  Ref type, SQL type from java.sql.Types (nil if
                                  :data-type isn't DISTINCT or user-generated REF)
    :is-autoincrement   String => Indicates whether this column is auto incremented
                                    YES --- if the column is auto incremented
                                    NO --- if the column is not auto incremented
                                    empty string --- if it cannot be determined
                                                     whether the column is auto
                                                     incremented
  Arguments:
   dm (java.sql.DatabaseMetaData)
  Optional arguments:
   :catalog        (String) a catalog name; must match the catalog name as it is
                            stored in the database; \"\" retrieves those without
                            a catalog; nil means that the catalog name should not
                            be used to narrow the search
   :schema-pattern (String) a schema name pattern; must match the schema name as
                            it is stored in the database; \"\" retrieves those
                            without a schema; nil means that the schema name
                            should not be used to narrow the search
   :table-pattern  (String) a table name pattern; must match the table name as
                            it is stored in the database
   :column-pattern (String) a column name pattern; must match the column name as
                            it is stored in the database
  See also:
   http://j.mp/fap5kl (Java 6 API, class DatabaseMetaData, method getColumns)"
  [^DatabaseMetaData dm
   & {:keys [catalog
             schema-pattern
             table-pattern
             column-pattern]
      :or {catalog nil
           schema-pattern nil
           table-pattern  nil
           column-pattern nil}
      :as opt}]
  {:pre [(clojure.set/subset? (set (keys opt))
           #{:catalog
             :schema-pattern
             :table-pattern
             :column-pattern})]}
  (let [rs (.getColumns dm
             ^String catalog       ^String schema-pattern
             ^String table-pattern ^String column-pattern)]
    (into [] (row-seq rs))))


(defn get-column-privileges
  "Retrieve a description of the access rights for a table's columns. Only
  privileges matching the column name criteria are returned. They are ordered by
  :column-name and :privilege.
  Each privilige description has the following columns:
    :table-cat    String => table catalog (may be nil)
    :table-schem  String => table schema (may be nil)
    :table-name   String => table name
    :column-name  String => column name
    :grantor      String => grantor of access (may be nil)
    :grantee      String => grantee of access
    :privilege    String => name of access (SELECT, INSERT, UPDATE, REFRENCES, ...)
    :is-grantable String => \"YES\" if grantee is permitted to grant to others;
                            \"NO\" if not; nil if unknown
  Arguments:
   dm (java.sql.DatabaseMetaData)
  Optional arguments:
   :catalog        (String) a catalog name; must match the catalog name as it is
                            stored in the database; \"\" retrieves those without
                            a catalog; nil means that the catalog name should not
                            be used to narrow the search
   :schema         (String) a schema name; must match the schema name as it is
                            stored in the database; \"\" retrieves those without
                            a schema; nil means that the schema name should not
                            be used to narrow the search
   :table          (String) a table name; must match the table name as it is
                            stored in the database
   :column-pattern (String) a column name pattern; must match the column name as
                            it is stored in the database
  See also:
   http://j.mp/hmaOI4 (Java 6 API, class DatabaseMetaData, method getColumnPrivileges)"
  [^DatabaseMetaData dm
   & {:keys [catalog
             schema
             table
             column-pattern]
      :or {catalog nil
           schema  nil
           table   nil
           column-pattern nil}
      :as opt}]
  {:pre [(clojure.set/subset? (set (keys opt))
           #{:catalog
             :schema
             :table
             :column-pattern})]}
  (let [rs (.getColumnPrivileges dm
             ^String catalog ^String schema
             ^String table   ^String column-pattern)]
    (into [] (row-seq rs))))


(defn get-crossref
  "Given a parent-table and a foreign-table, retrieve a description of the
  foreign key columns in the foreign-table that reference the primary key or the
  columns representing a unique constraint of the parent-table (could be the
  same or a different table). The number of columns returned from parent-table
  must match the number of columns that make up the foreign key. They are
  ordered by
    FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ.

  Each foreign key column description has the following columns:
  
  :pktable-cat   String => parent key table catalog (may be nil)
  :pktable-schem String => parent key table schema (may be nil)
  :pktable-name  String => parent key table name
  :pkcolumn-name String => parent key column name
  :fktable-cat   String => foreign key table catalog being exported (may be nil)
  :fktable-schem String => foreign key table schema being exported (may be nil)
  :fktable-name  String => foreign key table name being exported
  :fkcolumn-name String => foreign key column name being exported
  :key-seq       short  => sequence number within foreign key;
                           value 1 represents the 1st column of the foreign key,
                           value 2 represents the 2nd column of the foreign key
  :update-rule    short => What happens to foreign key when parent key is updated:
                           (below are static fields in java.sql.DatabaseMetaData)
                            importedKeyNoAction   - do not allow update of parent
                                                    key if it has been imported
                            importedKeyCascade    - change imported key to agree
                                                    with parent key update
                            importedKeySetNull    - change imported key to NULL if
                                                    its parent key has been updated
                            importedKeySetDefault - change imported key to default
                                                    values if its parent key has
                                                    been updated
                            importedKeyRestrict   - same as importedKeyNoAction
                                                    (for ODBC 2.x compatibility)
  :delete-rule    short => What happens to the foreign key when parent key is deleted:
                           (below are static fields in java.sql.DatabaseMetaData)
                            importedKeyNoAction   - do not allow delete of parent key
                                                    if it has been imported
                            importedKeyCascade    - delete rows that import a deleted key
                            importedKeySetNull    - change imported key to NULL if its
                                                    primary key has been deleted
                            importedKeyRestrict   - same as importedKeyNoAction
                                                    (for ODBC 2.x compatibility)
                            importedKeySetDefault - change imported key to default if
                                                    its parent key has been deleted
  :fk-name       String => foreign key name (may be null)
  :pk-name       String => parent key name (may be null)
  :deferrability  short => can the evaluation of foreign key constraints be deferred until commit
                           (below are static fields in java.sql.DatabaseMetaData)
                            importedKeyInitiallyDeferred  - see SQL92 for definition
                            importedKeyInitiallyImmediate - see SQL92 for definition
                            importedKeyNotDeferrable      - see SQL92 for definition
  Arguments:
   dm            (java.sql.DatabaseMetaData)
   parent-table  (String) the name of the table that exports the key; must match
                          the table name as it is stored in the database
   foreign-table (String) the name of the table that imports the key; must match
                          the table name as it is stored in the database
  Optional arguments:
   :parent-catalog  (String) a catalog name; must match the catalog name as it is
                             stored in the database; \"\" retrieves those without
                             a catalog; nil (default) means drop catalog name from
                             the selection criteria
   :parent-schema   (String) a schema name; must match the schema name as it is
                             stored in the database; \"\" retrieves those without
                             a schema; nil (default) means drop schema name from
                             the selection criteria
   :foreign-catalog (String) a catalog name; must match the catalog name as it is
                             stored in the database; \"\" retrieves those without
                             a catalog; nil (default) means drop catalog name from
                             the selection criteria
   :foreign-schema  (String) a schema name; must match the schema name as it is
                             stored in the database; \"\" retrieves those without
                             a schema; nil (default) means drop schema name from
                             the selection criteria
  See also:
   http://j.mp/h6bM4u (Java 6 API, class DatabaseMetaData, method getCrossReference)"
  [^DatabaseMetaData dm ^String parent-table ^String foreign-table
   & {:keys [parent-catalog
             parent-schema
             foreign-catalog
             foreign-schema]
      :or {parent-catalog  nil
           parent-schema   nil
           foreign-catalog nil
           foreign-schema  nil}
      :as opt}]
  {:pre [(clojure.set/subset? (set (keys opt))
           #{:parent-catalog
             :parent-schema
             :foreign-catalog
             :foreign-schema})]}
  (let [rs (.getCrossReference dm
             ^String parent-catalog  ^String parent-schema  parent-table
             ^String foreign-catalog ^String foreign-schema foreign-table)]
    (into [] (row-seq rs))))
