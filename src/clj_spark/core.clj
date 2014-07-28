(ns ^{:doc
      "Maintains all Spark functions that correlate directly to clojure.core
      functions. These functions adhere to the contract that, agnostic of their
      input (instance? ISeq or JavaRDDLike), they will return their correct
      values regardless of data structure."
      :author "Brennon York"}
  clj-spark.core
  (:refer-clojure :exclude [count distinct filter first get group-by keys map
                            max min name partition-by reduce take vals])
  (:import [clj_spark.function Func Func2]
           [org.apache.spark Partitioner]
           [org.apache.spark.api.java JavaRDDLike JavaPairRDD])
  (:require [clj-spark.util :as util]))

(defn count
  "Return the number of elements in the RDD."
  [coll]
  (cond
   (instance? JavaRDDLike coll) (.count coll)
   :else (clojure.core/count coll)))

(defn distinct
  "Return a new RDD containing the distinct elements in this RDD."
  ([coll]
   (cond
    (instance? JavaRDDLike coll) (.distinct coll)
    :else (clojure.core/distinct coll)))
  ([num-partitions coll]
   (.distinct coll num-partitions)))

(defn filter
  [pred coll]
  (cond
   (instance? JavaPairRDD coll)
   (.filter coll (Func. (fn [x] (pred (util/unbox-tuple2 x)))))
   (instance? JavaRDDLike coll) (.filter coll (Func. pred))
   :else (clojure.core/filter pred coll)))

(defn first
  [coll]
  (cond
   (instance? JavaPairRDD coll) (util/unbox-tuple2 (.first coll))
   (instance? JavaRDDLike coll) (.first coll)
   :else (clojure.core/first coll)))

(defn get
  "Return the list of values in the RDD for key `k`. Overrides the Spark RDD
  method of `.lookup`."
  ([k coll]
   (cond
    (instance? JavaPairRDD coll) (.lookup coll k)
    :else (clojure.core/get coll k)))
  ([k coll not-found]
   (cond
    (instance? JavaPairRDD coll) (let [ret (.lookup coll k)]
                                   (if (not (empty? ret))
                                     ret
                                     not-found))
    :else (clojure.core/get coll k not-found))))

(defn group-by
  "Return an RDD of grouped elements."
  ([f coll]
   (cond
    (instance? JavaRDDLike coll)
    (.groupBy coll (Func. f))
    :else (clojure.core/group-by f coll)))
  ([f num-partitions coll]
   (.groupBy coll (Func. f) num-partitions)))

(defn keys
  "Return an RDD with the keys of each tuple."
  [coll]
  `(cond
    (instance? JavaPairRDD coll) (.keys coll)
    :else (clojure.core/keys coll)))

(defn map
  ([f coll]
   (cond
    (instance? JavaRDDLike coll) (.map coll (Func. f))
    :else (clojure.core/map f coll)))
  ([f coll & more]
   (apply clojure.core/map (apply flatten f coll more))))

(defn max
  "Returns the maximum element from this RDD as defined by the specified
  Comparator[T]."
  ([x] (clojure.core/max x))
  ([pred coll]
   (cond
    (instance? JavaRDDLike coll) (.max coll (comparator pred))
    :else (clojure.core/max pred coll)))
  ([x y & more] `(clojure.core/max ~x ~y ~@more)))

(defn min
  "Returns the minimum element from this RDD as defined by the specified
  Comparator[T]."
  ([x] (clojure.core/min x))
  ([pred coll]
   (cond
    (instance? JavaRDDLike coll) (.min coll (comparator pred))
    :else (clojure.core/min pred coll)))
  ([x y & more] (apply clojure.core/min (flatten x y more))))

(defn name
  [coll]
  (cond
   (instance? JavaRDDLike coll) (.name coll)
   :else (clojure.core/name coll)))

(defn partition-by
  "Return a copy of the RDD partitioned using the specified partitioner. If no
  partitioner is specified the `Partitioner/defaultPartitioner` will be used."
  [f coll]
  (cond
   (instance? JavaPairRDD coll)
   (if f
     (.partitionBy coll f)
     (.partitionBy coll (Partitioner/defaultPartitioner
                          (.rdd coll)
                          (scala.collection.mutable.LinkedList.))))
   :else (clojure.core/partition-by f coll)))

(defn reduce
  "Reduces the elements of this RDD using the specified commutative and
  associative binary operator."
  ([f coll]
   (cond
    (instance? JavaRDDLike coll) (.reduce coll (Func2. f))
    :else (clojure.core/reduce f coll)))
  ([f v coll] (clojure.core/reduce f v coll)))

(defn take
  [n coll]
  (cond
   (instance? JavaRDDLike coll) (vec (.take coll n))
   :else (clojure.core/take n coll)))

(defn vals
  "Return an RDD with the values of each tuple. Overrides the Spark RDD method
  of `.values`."
  [coll]
  (cond
   (instance? JavaPairRDD coll) (.values coll)
   :else (clojure.core/vals coll)))
