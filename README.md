# clj-jdbcutil

Clojure JDBC utility functions meant for other higher-level libraries.

## Usage

On Clojars: _This library is not on Clojars yet._

Leiningen dependency: `[clj-jdbcutil "0.1.0"]`

Everything exists in the `clj-jdbcutil.core` namespace.

```clojure
(ns example.app
  (:require [clj-jdbcutil.core :as ju]))
```

### Dynamic var for database configuration keys

The dynamic var **`*dbspec*`** may be bound to a map containing well known keys and
corresponding values:


| key              | type                                  | default | description |
|------------------|---------------------------------------|---------|-------------|
| `:datasource`    | javax.sql.DataSource                  | `nil`   |             |
| `:connection`    | java.sql.Connection                   | `nil`   |             |
| `:dbmetadata`    | map                                   | `{}`    | use `dbmeta` to retrieve this value |
| `:catalog`       | String, Keyword etc.                  | `nil`   | SHOULD be converted using `db-iden` |
| `:schema`        | String, Keyword etc.                  | `nil`   | SHOULD be converted using `db-iden` |
| `:read-only`     | boolean                               | `false` | true SHOULD disallow write operations |
| `:show-sql`      | boolean                               | `true`  | true SHOULD print SQL statements |
| `:show-sql-fn`   | function (w/ 1 arg)                   | fn that prints SQL using `println`     | you may rebind this to fn that sends to logger |
| `:clj-to-db`     | function (w/ 1 arg, returns string)   | fn that converts hyphen to underscore  | dictates how should identifiers be converted from Clojure to the database |
| `:db-to-clj`     | function (w/ 1 arg, returns clj form) | fn that converts to lower-case keyword | dictates how should identifiers be converted from the database to Clojure |
| `:fetch-size`    | Integer                               | 1000    | number of rows to fetch per DB roundtrip; helps throttle/optimize large DB reads; 0 means unlimited |
| `:query-timeout` | Integer                               | 0       | number of seconds to wait for query to execute, after which timeout occurs raising SqlException (not all JDBC drivers support this so check driver manual before use) |


**Notes:**

* Users MAY expose their own API to re-bind this var to new values.
* They MUST NOT alter the type/semantics of the well-defined keys.
* They MAY introduce custom keys with unique prefixes e.g.
  `:com.foo.xlib.conn-pool-name` in order to prevent name collision.

### Creating datasources

You must already have the JDBC driver in the classpath. Then you can follow
either of the following to create a datasource.

```clojure
(make-datasource driver-classname jdbc-url)
(make-datasource driver-classname jdbc-url username password)
```

Note that the API above will not create a pooling datasource, and hence may not
be suitable for production use. For information on how to create pooling JDBC
datasources, check out [clj-dbcp](https://github.com/kumarshantanu/clj-dbcp)

### Connecting

The macro `with-connection` can execute a body of code in the context of
JDBC connection as `:connection` (unless already populated). This is like
[`clojure.java.jdbc/with-connection`](https://github.com/clojure/java.jdbc)
with the exception that you must provide `:datasource` in the map.

```clojure
(with-connection {key1 val1 key2 val2 ...}
  ...)
```

## License

Copyright © 2012 Shantanu Kumar

Distributed under the Eclipse Public License, the same as Clojure.
