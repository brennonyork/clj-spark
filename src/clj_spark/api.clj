(ns clj-spark.api
  (:refer-clojure :exclude [map reduce])
  (:import [org.apache.spark.api.java JavaRDDLike]
           [org.apache.spark.api.java.function
            DoubleFlatMapFunction
            DoubleFunction
            FlatMapFunction
            FlatMapFunction2
            Function
            Function2
            Function3
            PairFlatMapFunction
            PairFunction
            VoidFunction])
  (:gen-class))

(defmacro map
  [f coll]
  `(cond
    (instance? JavaRDDLike ~coll)
    (.map ~coll (reify Function
                  (call [this v1#] (~f v1#))))
    :else (clojure.core/map ~f ~coll)))

(defmacro reduce
  [f coll]
  `(cond
    (instance? JavaRDDLike ~coll)
    (.reduce ~coll (reify Function2
                     (call [this v1# v2#] (~f v1# v2#))))
    :else (clojure.core/reduce ~f ~coll)))
