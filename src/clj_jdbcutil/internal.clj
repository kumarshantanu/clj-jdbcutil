(ns clj-jdbcutil.internal
  (:require
    [clojure.string :as sr]))


(defmacro
  maybe
  [& body]
  `(try [~@body nil]
     (catch Exception e#
       [nil e#])))


(defmacro
  maybe-val
  [& body]
  `(let [[v# e#] (maybe ~@body)]
     v#))


(defn as-string
  "Convert given argument to string.
  Example:
    (str       \":one\") ; returns \":one\"
    (as-string \":one\") ; returns \"one\""
  [x]
  (if (or (keyword? x) (symbol? x)) (name x)
    (str x)))
