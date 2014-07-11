(ns ^{:doc "Provides users with convenient Clojure-based functions for handling
      Spark context objects"
      :author "Brennon York"}
  clj-spark.context
  (:import [org.apache.spark.api.java JavaSparkContext])
  (:require [clj-spark.util :as util]))

(defmacro with-context
  "Defines a wrapper around a SparkContext to ensure it gets shut down
  properly.

  Example: (with-context sc [\"local[2]\" \"my-app\"] ... )"
  [sym [& ctx-args] & body]
  `(let [~(symbol sym) (JavaSparkContext. ~@ctx-args)
         start-time# (.startTime ~(symbol sym))]
     ~@body
     (.stop ~(symbol sym))
     (- (System/currentTimeMillis) start-time#) 1000))

(defmacro parallelize
  ""
  ([coll ctx]
   `(.parallelize ~ctx (java.util.ArrayList. ~coll)))
  ([coll numSlices ctx]
   `(.parallelize ~ctx (java.util.ArrayList. ~coll) ~numSlices)))

(defmacro parallelize->double
  ""
  ([coll ctx]
   `(.parallelizeDoubles ~ctx (java.util.ArrayList. (map double ~coll))))
  ([coll numSlices ctx]
   `(.parallelizeDoubles ~ctx (java.util.ArrayList. (map double ~coll))
                         ~numSlices)))

(defmacro parallelize->pair
  ""
  ([coll ctx]
   `(.parallelizePairs ~ctx (java.util.ArrayList.
                             (map (fn [[x y]] (util/box-tuple2 x y)) ~coll))))
  ([coll numSlices ctx]
   `(.parallelizePairs ~ctx (java.util.ArrayList.
                             (map (fn [[x y]] (util/box-tuple2 x y)) ~coll))
                       ~numSlices)))
