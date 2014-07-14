(ns ^{:doc
      "Maintains all Spark functions that do not necessarily map directly to
      any Clojure functions, but provide input for traditional Clojure data
      structures. These functions adhere to the contract that, agnostic of
      their input (instance? ISeq or JavaRDDLike), they will return intuitive
      results for seamless transitions between collections."
      :author "Brennon York"}
  clj-spark.contrib
  (:import [org.apache.spark.api.java JavaRDDLike JavaRDD JavaPairRDD
            JavaDoubleRDD]
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
  (:require [clj-spark.util :as util]
            [clojure.tools.logging :as log]))

(defmacro collect
  "Return an array that contains all of the elements in this RDD. If Clojure
  structures are passed will merely call `vec` and acts as a method to
  realize a LazySeq."
  [coll]
  `(cond
    (instance? JavaPairRDD ~coll) (vec (map (fn [x#]
                                              (util/unbox-tuple2 x#))
                                            (.collect ~coll)))
    (instance? JavaRDDLike ~coll) (vec (.collect ~coll))
    (coll? ~coll) (vec ~coll)
    :else (log/errorf "Cannot `collect` with type %s." (type ~coll))))

;; Pair
(defmacro count-by-key
  "Count the number of elements for each key, and return the result to the
  master as a Map."
  [coll]
  `(cond
    (instance? JavaPairRDD ~coll) (into {} (.countByKey ~coll))
    (coll? ~coll)
    (into {} (map (fn [[x# y#]] [x# (if (coll? y#) (count y#) 0)]) ~coll))
    :else (log/errorf "Cannot `count-by-key` with type %s." (type ~coll))))

(defmacro count-by-value
  "Return the count of each unique value in this RDD as a map of (value, count)
  pairs."
  [t coll]
  `(into {} (.countByValue ~coll ~t)))

(defmacro foreach
  "Applies a function f to all elements of this RDD."
  [f coll]
  `(cond
    (instance? JavaRDDLike ~coll)
    (.foreach ~coll (reify VoidFunction (call [this v#] (~f v#))))
    :else "Error: Unsupported function for ISeq"))

;; Pair
(defmacro lookup
  "Return the list of values in the RDD for key key."
  [k coll]
  `(cond
    (instance? JavaPairRDD ~coll) (.lookup ~coll ~k)
    :else (get ~coll ~k)))

;; Pair
(defmacro map-values
  "Pass each value in the key-value pair RDD through a map function without
  changing the keys; this also retains the original RDD's partitioning."
  [f coll]
  `(cond
    (instance? JavaPairRDD ~coll) (.mapValues ~coll
                                              (reify Function
                                                (call [this v#] (~f v#))))
    (map? ~coll) (into {} (map (fn [[x# y#]] [x# (~f y#)])))
    :else (log/errorf "Cannot `map-values` with type %s." (type ~coll))))

;; Doub
(defmacro mean
  "Compute the mean of this RDD's elements."
  [coll]
  `(cond
    (instance? JavaDoubleRDD ~coll) (.mean ~coll)
    (not (map? ~coll)) (double (/ (reduce + ~coll) (count ~coll)))
    :else (log/errorf "Cannot `mean` with type %s." (type ~coll))))

;; Pair
;; TODO: Watch this for the arity type issue!!
(defmacro reduce-by-key
  "Merge the values for each key using an associative reduce function."
  ([f coll]
   `(.reduceByKey ~coll (reify Function2
                          (call [this v# v2#] (~f v# v2#)))))
  ([f p coll]
   `(.reduceByKey ~coll (reify Function2
                          (call [this v# v2#] (~f v# v2#))) ~p)))

;; Doub
(defmacro sum
  "Add up the elements in this RDD."
  [coll]
  `(cond
    (instance? JavaDoubleRDD ~coll) (.sum ~coll)
    (not (map? ~coll)) (reduce + ~coll)
    :else (log/errorf "Cannot `sum` with type %s." (type ~coll))))

;; Pair
(defmacro values
  "Return an RDD with the values of each tuple."
  [coll]
  `(cond
    (instance? JavaPairRDD ~coll) (.values ~coll)
    (map? ~coll) (vals ~coll)
    :else (log/errorf "Cannot `values` with type %s." (type ~coll))))
