(ns clj-spark.api
  (:refer-clojure :exclude [count filter first map reduce take])
  (:import [org.apache.spark.api.java
            JavaSparkContext
            JavaRDDLike
            JavaRDD]
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

(defn ctx
  "Returns a JavaSparkContext"
  ([] (JavaSparkContext.))
  ([a] (JavaSparkContext. a))
  ([a b] (JavaSparkContext. a b))
  ([a b c] (JavaSparkContext. a b c))
  ([a b c d] (JavaSparkContext. a b c d))
  ([a b c d e] (JavaSparkContext. a b c d e)))

(defmacro aggregate
  "Aggregate the elements of each partition, and then the results for all the
  partitions, using given combine functions and a neutral \"zero value\"."
  [z f1 f2 coll]
  `(cond
    (instance? JavaRDDLike ~coll) (.aggregate ~coll ~z ~f1 ~f2)
    :else "Error: Unsupported function for ISeq"))

(defmacro cartesian
  "Return the Cartesian product of this RDD and another one, that is, the RDD
  of all pairs of elements (a, b) where a is in this and b is in other."
  [ocoll coll]
  `(cond
    (instance? JavaRDDLike ~coll) (.cartesian ~coll ~ocoll)
    :else "Error: Unsupported function for ISeq"))

(defmacro checkpoint
  "Mark this RDD for checkpointing."
  [coll]
  `(.checkpoint ~coll))

(defmacro class-tag [coll] `(.classTag ~coll))

(defmacro collect
  "Return an array that contains all of the elements in this RDD."
  [coll]
  `(vec (.collect ~coll)))

(defmacro collect-partitions
  "Return an array that contains all of the elements in a specific partition of
  this RDD."
  [ocoll coll]
  `(map vec (.collectPartitions ~coll ~ocoll)))

(defmacro context
  "The SparkContext that this RDD was created on."
  [coll]
  `(.context ~coll))

(defmacro count
  "Return the number of elements in the RDD."
  [coll]
  `(cond
    (instance? JavaRDDLike ~coll) (.count ~coll)
    :else (clojure.core/count ~coll)))

(defmacro filter
  [f coll]
  `(cond
    (instance? JavaRDDLike ~coll)
    (.filter ~coll (reify Function
                     (call [this v1#] (~f v1#))))
    :else (clojure.core/filter ~f ~coll)))

(defmacro first
  [coll]
  `(cond
    (instance? JavaRDDLike ~coll) (.first ~coll)
    :else (clojure.core/first ~coll)))

(defn flatmap
  [])

(defn flatmap->double
  [])

(defn flatmap->pair
  [])

(defn fold
  [])

(defmacro map
  ([f coll]
  `(cond
    (instance? JavaRDDLike ~coll)
    (.map ~coll (reify Function
                  (call [this v1#] (~f v1#))))
    :else (clojure.core/map ~f ~coll)))
  ([f coll1 coll2 & colls]
   `(clojure.core/map ~f ~coll1 ~coll2 ~@colls)))

(defmacro reduce
  [f coll]
  `(cond
    (instance? JavaRDDLike ~coll)
    (.reduce ~coll (reify Function2
                     (call [this v1# v2#] (~f v1# v2#))))
    :else (clojure.core/reduce ~f ~coll)))

(defmacro take
  [n coll]
  `(cond
    (instance? JavaRDDLike ~coll) (.take ~coll ~n)
    :else (clojure.core/take ~n ~coll)))
