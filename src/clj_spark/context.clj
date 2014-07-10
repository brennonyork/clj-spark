(ns ^{:doc "Provides users with convenient Clojure-based functions for handling
      Spark context objects"
      :author "Brennon York"}
  clj-spark.context
  (:import [org.apache.spark.api.java JavaSparkContext]))

(defmacro with-context
  "Defines a wrapper around a SparkContext to ensure it gets shut down
  properly.

  Example: (with-context sc [\"local[2]\" \"my-app\"] ... )"
  [sym [& ctx-args] & body]
  `(let [~(symbol sym) (JavaSparkContext. ~@ctx-args)]
     ~@body
     (.stop ~(symbol sym))))
