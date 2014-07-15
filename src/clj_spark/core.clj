(ns ^{:doc
      "Maintains all Spark functions that correlate directly to clojure.core
      functions. These functions adhere to the contract that, agnostic of their
      input (instance? ISeq or JavaRDDLike), they will return their correct
      values regardless of data structure."
      :author "Brennon York"}
  clj-spark.core
  (:refer-clojure :exclude [count distinct filter first group-by keys map max
                            min name partition-by reduce take vals])
  (:import [org.apache.spark Partitioner]
           [org.apache.spark.api.java JavaRDDLike JavaPairRDD]
           [org.apache.spark.api.java.function Function Function2])
  (:require [clj-spark.util :as util]))

(defmacro count
  "Return the number of elements in the RDD."
  [coll]
  `(cond
    (instance? JavaRDDLike ~coll) (.count ~coll)
    :else (clojure.core/count ~coll)))

(defmacro distinct
  "Return a new RDD containing the distinct elements in this RDD."
  ([coll]
   `(cond
     (instance? JavaRDDLike ~coll) (.distinct ~coll)
     :else (clojure.core/distinct ~coll)))
  ([num-partitions coll]
   `(.distinct ~coll ~num-partitions)))

(defmacro filter
  [pred coll]
  `(cond
    (instance? JavaPairRDD ~coll)
    (.filter ~coll (reify Function
                     (call [this v#] (~pred (util/unbox-tuple2 v#)))))
    (instance? JavaRDDLike ~coll)
    (.filter ~coll (reify Function
                     (call [this v#] (~pred v#))))
    :else (clojure.core/filter ~pred ~coll)))

(defmacro first
  [coll]
  `(cond
    (instance? JavaPairRDD ~coll) (let [i# (.first ~coll)] (util/unbox-tuple2 i#))
    (instance? JavaRDDLike ~coll) (.first ~coll)
    :else (clojure.core/first ~coll)))

(defmacro group-by
  "Return an RDD of grouped elements."
  ([f coll]
   `(cond
     (instance? JavaRDDLike ~coll)
     (.groupBy ~coll (reify Function
                       (call [this v#] (~f v#))))
     :else (clojure.core/group-by ~f ~coll)))
  ([f num-partitions coll]
   `(.groupBy ~coll (reify Function
                      (call [this v#] (~f v#))) ~num-partitions)))

(defmacro keys
  "Return an RDD with the keys of each tuple."
  [coll]
  `(cond
    (instance? JavaPairRDD ~coll) (.keys ~coll)
    :else (clojure.core/keys ~coll)))

(defmacro map
  ([f coll]
   `(cond
     (instance? JavaRDDLike ~coll)
     (.map ~coll (reify Function
                   (call [this v#] (~f v#))))
     :else (clojure.core/map ~f ~coll)))
  ([f coll & more]
   `(clojure.core/map ~f ~coll ~@more)))

(defmacro max
  "Returns the maximum element from this RDD as defined by the specified
  Comparator[T]."
  ([x] `(clojure.core/max ~x))
  ([pred coll]
   `(cond
     (instance? JavaRDDLike ~coll) (.max ~coll (comparator ~pred))
     :else (clojure.core/max ~pred ~coll)))
  ([x y & more] `(clojure.core/max ~x ~y ~@more)))

(defmacro min
  "Returns the minimum element from this RDD as defined by the specified
  Comparator[T]."
  ([x] `(clojure.core/min ~x))
  ([pred coll]
   `(cond
     (instance? JavaRDDLike ~coll) (.min ~coll (comparator ~pred))
     :else (clojure.core/min ~pred ~coll)))
  ([x y & more] `(clojure.core/min ~x ~y ~@more)))

(defmacro name
  [coll]
  `(cond
    (instance? JavaRDDLike ~coll) (.name ~coll)
    :else (clojure.core/name ~coll)))

(defmacro partition-by
  "Return a copy of the RDD partitioned using the specified partitioner. If no
  partitioner is specified the `Partitioner/defaultPartitioner` will be used."
  [f coll]
  `(cond
    (instance? JavaPairRDD ~coll)
    (if ~f
      (.partitionBy ~coll ~f)
      (.partitionBy ~coll (Partitioner/defaultPartitioner
                            (.rdd ~coll)
                            (scala.collection.mutable.LinkedList.))))
    :else (clojure.core/partition-by ~f ~coll)))

(defmacro reduce
  "Reduces the elements of this RDD using the specified commutative and
  associative binary operator."
  ([f coll]
   `(cond
     (instance? JavaRDDLike ~coll)
     (.reduce ~coll (reify Function2
                      (call [this v# v2#] (~f v# v2#))))
     :else (clojure.core/reduce ~f ~coll)))
  ([f v coll] `(clojure.core/reduce ~f ~v ~coll)))

(defmacro take
  [n coll]
  `(cond
    (instance? JavaRDDLike ~coll) (vec (.take ~coll ~n))
    :else (clojure.core/take ~n ~coll)))

(defmacro vals
  "Return an RDD with the values of each tuple. Overrides the Spark RDD method
  of `.values`."
  [coll]
  `(cond
    (instance? JavaPairRDD ~coll) (.values ~coll)
    :else (clojure.core/vals ~coll)))
