(ns ^{:doc
      "clj-spark.contrib maintains all Spark functions that do not necessarily
      map directly to any Clojure functions, but provide input for traditional
      Clojure data structures. These functions adhere to the contract that,
      agnostic of their input (instance? ISeq or JavaRDDLike), they will return
      intuitive results for seamless transitions between collections."
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
  (:require [clj-spark.util :as util]))

(defmacro collect
  "Return an array that contains all of the elements in this RDD. If Clojure
  structures are passed will merely call `vec` and acts as a method to
  realize a LazySeq."
  [coll]
  `(cond
    (instance? JavaPairRDD ~coll) (vec (map (fn [x#] (util/unbox-tuple2 x#)) (.collect ~coll)))
    (instance? JavaRDDLike ~coll) (vec (.collect ~coll))
    :else (vec ~coll)))
