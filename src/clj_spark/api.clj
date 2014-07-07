(ns clj-spark.api
  (:refer-clojure :exclude [count
                            distinct
                            filter
                            first
                            group-by
                            keys
                            map
                            max
                            min
                            name
                            partition-by
                            reduce
                            take])
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
  (:require [clj-spark.util :as util])
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

;; Reg, Pair, Doub
(defmacro cache
  "Persist this RDD with the default storage level (`MEMORY_ONLY`)."
  [coll]
  `(.cache ~coll))

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

;; Reg, Pair, Doub
(defmacro coalesce
  "Return a new RDD that is reduced into numPartitions partitions."
  ([n coll]
   `(.coalesce ~coll ~n))
  ([n b coll]
   `(.coalesce ~coll ~n ~b)))

;; Pair
(defmacro cogroup
  "For each key k in this or other1 or other2, return a resulting RDD that
  contains a tuple with the list of values for that key in this, other1 and
  other2."
  ([rdd coll]
   `(.cogroup ~coll ~rdd))
  ([rdd p coll]
   `(.cogroup ~coll ~rdd ~p))
  ([rdd rdd2 p coll]
   `(.cogroup ~coll ~rdd ~rdd2 ~p)))

(defmacro collect
  "Return an array that contains all of the elements in this RDD."
  [coll]
  `(vec (.collect ~coll)))

;; Pair
(defmacro collect-as-map
  "Return the key-value pairs in this RDD to the master as a Map."
  [coll]
  `(into {} (.collectAsMap ~coll)))

(defmacro collect-partitions
  "Return an array that contains all of the elements in a specific partition of
  this RDD."
  [ocoll coll]
  `(map vec (.collectPartitions ~coll ~ocoll)))

;; Pair
(defmacro combine-by-key
  "Generic function to combine the elements for each key using a custom set of
  aggregation functions."
  ([f f2 f3 coll]
   `(.combineByKey ~coll
                   (reify Function
                     (call [this v#] (~f v#)))
                   (reify Function2
                     (call [this v# v2#] (~f2 v# v2#)))
                   (reify Function2
                     (call [this v# v2#] (~f3 v# v2#)))))
  ([f f2 f3 p coll]
   `(.combineByKey ~coll
                   (reify Function
                     (call [this v#] (~f v#)))
                   (reify Function2
                     (call [this v# v2#] (~f2 v# v2#)))
                   (reify Function2
                     (call [this v# v2#] (~f3 v# v2#)))
                   ~p)))

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

(defmacro count-approx
  ":: Experimental :: Approximate version of count() that returns a potentially
  incomplete result within a timeout, even if not all tasks have finished."
  ([t coll])
  ([t c coll]))

(defmacro count-approx-distinct
  "Return approximate number of distinct elements in the RDD."
  [r coll]
  `(.countApproxDistinct ~coll ~r))

;; Pair
(defmacro count-approx-distinct-by-key
  "Return approximate number of distinct values for each key this RDD."
  ([r coll]
   `(.countApproxDistinctByKey ~coll ~r))
  ([r p coll]
   `(.countApproxDistinctByKey ~coll ~r ~p)))

;; Pair
(defmacro count-by-key
  "Count the number of elements for each key, and return the result to the
  master as a Map."
  [coll]
  `(into {} (.countByKey ~coll)))

;; Pair
(defmacro count-by-key-approx
  ":: Experimental :: Approximate version of countByKey that can return a
  partial result if it does not finish within a timeout."
  ([t coll]
   `(.countByKeyApprox ~coll ~t))
  ([t c coll]
   `(.countByKeyApprox ~coll ~t ~c)))

(defmacro count-by-value
  "Return the count of each unique value in this RDD as a map of (value, count)
  pairs."
  [t coll]
  `(into {} (.countByValue ~coll ~t)))

(defmacro count-by-value-approx
  "(Experimental) Approximate version of countByValue()."
  ([t coll])
  ([t c coll]))

;; Reg, Pair, Doub
(defmacro distinct
  "Return a new RDD containing the distinct elements in this RDD."
  ([coll]
   `(cond
     (instance? JavaRDDLike) (.distinct ~coll)
     :else (clojure.core/distinct ~coll)))
  ([n coll]
   `(.distinct ~coll ~n)))

;; Reg, Pair, Doub
(defmacro filter
  [f coll]
  `(cond
    (instance? JavaPairRDD ~coll)
    (.filter ~coll (reify Function
                     (call [this v#] (~f (util/unbox-tuple2 v#)))))
    (instance? JavaRDDLike ~coll)
    (.filter ~coll (reify Function
                     (call [this v#] (~f v#))))
    :else (clojure.core/filter ~f ~coll)))

;; Reg, Pair, Doub
(defmacro first
  [coll]
  `(cond
    (instance? JavaPairRDD ~coll) (let [i# (.first ~coll)] (util/unbox-tuple2 i#))
    (instance? JavaRDDLike ~coll) (.first ~coll)
    :else (clojure.core/first ~coll)))

(defmacro flatmap
  "Return a new RDD by first applying a function to all elements of this RDD,
  and then flattening the results."
  [f coll]
  `(.flatMap ~coll (reify FlatMapFunction
                     (call [this v#] (~f v#)))))

;; Pair
(defmacro flatmap-values
  "Pass each value in the key-value pair RDD through a flatMap function without
  changing the keys; this also retains the original RDD's partitioning."
  [f coll]
  `(.flatmapValues ~coll (reify Function
                           (call [this v#] (~f v#)))))

(defmacro flatmap->double
  "Return a new RDD by first applying a function to all elements of this RDD,
  and then flattening the results."
  [f coll]
  `(.flatMapToDouble ~coll (reify DoubleFlatMapFunction
                             (call [this v#] (~f v#)))))

(defmacro flatmap->pair
  "Return a new RDD by first applying a function to all elements of this RDD,
  and then flattening the results."
  [f coll]
  `(.flatMapToPair ~coll (reify PairFlatMapFunction
                           (call [this v#] (~f v#)))))

(defmacro fold
  "Aggregate the elements of each partition, and then the results for all the
  partitions, using a given associative function and a neutral \"zero value\"."
  [t f coll]
  `(cond
    (instance? JavaRDDLike ~coll)
    (.fold ~coll ~t (reify Function2
                      (call [this v# v2#] (~f v# v2#))))
    :else "Error: Unsupported function for ISeq"))

;; Pair
(defmacro fold-by-key
  "Merge the values for each key using an associative function and a neutral
  \"zero value\" which may be added to the result an arbitrary number of times,
  and must not change the result (e.g ., Nil for list concatenation, 0 for
  addition, or 1 for multiplication.)."
  ([v f coll]
   `(.foldByKey ~coll ~v (reify Function2
                           (call [this v# v2#] (~f v# v2#)))))
  ([v p f coll]
   `(.foldByKey ~coll ~v ~p (reify Function2
                              (call [this v# v2#] (~f v# v2#))))))

(defmacro foreach
  "Applies a function f to all elements of this RDD."
  [f coll]
  `(cond
    (instance? JavaRDDLike ~coll)
    (.foreach ~coll (reify VoidFunction
                      (call [this v#] (~f v#))))
    :else "Error: Unsupported function for ISeq"))

(defmacro foreach-partition
  "Applies a function f to each partition of this RDD."
  [f coll]
  `(cond
    (instance? JavaRDDLike ~coll)
    (.foreachPartition ~coll (reify VoidFunction
                               (call [this v#] (~f v#))))
    :else "Error: Unsupported function for ISeq"))

;; Pair
(defmacro from-java-rdd
  "Convert a JavaRDD of key-value pairs to JavaPairRDD."
  [coll]
  (.fromJavaRDD ~coll))

;; Reg, Pair, Doub
(defmacro from-rdd
  ([rdd coll] ; Doub
   `(.fromRDD ~coll ~rdd))
  ([rdd e coll] ; Reg
   `(.fromRDD ~coll ~rdd ~e))
  ([rdd e e2 coll] ; Pair
   `(.fromRDD ~coll ~rdd ~e ~e2)))

(defmacro get-checkpoint-file
  "Gets the name of the file to which this RDD was checkpointed"
  [coll]
  `(.getCheckpointFile ~coll))

(defmacro get-storage-level
  "Get the RDD's current storage level, or StorageLevel.NONE if none is set."
  [coll]
  `(.getStorageLevel ~coll))

(defmacro glom
  "Return an RDD created by coalescing all elements within each partition into
  an array."
  [coll]
  `(vec (.glom ~coll)))

(defmacro group-by
  "Return an RDD of grouped elements."
  ([f coll]
   `(cond
     (instance? JavaRDDLike ~coll)
     (.groupBy ~coll (reify Function
                       (call [this v#] (~f v#))))
     :else (clojure.core/group-by ~f ~coll)))
  ([f n coll]
   `(.groupBy ~coll (reify Function
                      (call [this v#] (~f v#))) ~n)))

;; Pair
(defmacro group-by-key
  "Group the values for each key in the RDD into a single sequence."
  ([coll]
   `(.groupByKey ~coll))
  ([p coll]
   `(.groupByKey ~coll ~p)))

;; Pair
(defmacro group-with
  "Alias for cogroup."
  ([rdd coll]
   (cogroup rdd coll))
  ([rdd rdd2 coll]
   (cogroup rdd rdd2 coll)))

;; Doub
(defmacro histogram
  "Compute a histogram of the data using bucketCount number of buckets evenly
  spaced between the minimum and maximum of the RDD."
  ([b coll]
   `(.histogram ~coll ~b))
  ([b b2 coll]
   `(.histogram ~coll ~b ~b2)))

(defmacro id
  "A unique ID for this RDD (within its SparkContext)."
  [coll]
  `(.id ~coll))

;; SET, Reg, Pair, Doub
(defmacro intersection
  "Return the intersection of this RDD and another one."
  [rdd coll]
  `(.intersection ~coll ~rdd))

(defmacro is-checkpointed
  "Return whether this RDD has been checkpointed or not"
  [coll]
  `(.isCheckpointed ~coll))

(defmacro iterator
  "Internal method to this RDD; will read from cache if applicable, or
  otherwise compute it."
  [s t coll]
  `(.iterator ~coll ~s ~t))

;; Pair
(defmacro join
  "Return an RDD containing all pairs of elements with matching keys in this
  and other."
  ([rdd coll]
   `(.join ~coll ~rdd))
  ([rdd p coll]
   `(.join ~coll ~rdd ~p)))

;; Pair
(defmacro k-class-tag [coll] `(.kClassTag ~coll))

;; Pair
(defmacro keys
  "Return an RDD with the keys of each tuple."
  [coll]
  `(cond
    (instance? JavaPairRDD ~coll) (.keys ~coll)
    :else (clojure.core/keys ~coll)))

(defmacro key-by
  "Creates tuples of the elements in this RDD by applying f."
  [f coll]
  `(.keyBy ~coll (reify Function
                   (call [this v#] (~f v#)))))

;; Pair
(defmacro left-outer-join
  "Perform a left outer join of this and other."
  ([rdd coll]
   `(.leftOuterJoin ~coll ~rdd))
  ([rdd p coll]
   `(.leftOuterJoin ~coll ~rdd ~p)))

;; Pair
(defmacro lookup
  "Return the list of values in the RDD for key key."
  [k coll]
  `(.lookup ~coll ~k))

(defmacro map
  ([f coll]
   `(cond
     (instance? JavaRDDLike ~coll)
     (.map ~coll (reify Function
                   (call [this v#] (~f v#))))
     :else (clojure.core/map ~f ~coll)))
  ([f coll & more]
   `(clojure.core/map ~f ~coll ~@more)))

(defmacro map-partitions
  "Return a new RDD by applying a function to each partition of this RDD."
  ([f coll]
   `(.mapPartitions ~coll (reify FlatMapFunction
                            (call [this v#] (~f v#)))))
  ([f b coll]
   `(.mapPartitions ~coll (reify FlatMapFunction
                            (call [this v#] (~f v#))) ~b)))

(defmacro map-partitions->double
  "Return a new RDD by applying a function to each partition of this RDD."
  ([f coll]
   `(.mapPartitionsToDouble ~coll (reify DoubleFlatMapFunction
                                    (call [this v#] (~f v#)))))
  ([f b coll]
   `(.mapPartitionsToDouble ~coll (reify DoubleFlatMapFunction
                                    (call [this v#] (~f v#))) ~b)))

(defmacro map-partitions->pair
  "Return a new RDD by applying a function to each partition of this RDD."
  ([f coll]
   `(.mapPartitionsToPair ~coll (reify PairFlatMapFunction
                                  (call [this v#] (~f v#)))))
  ([f b coll]
   `(.mapPartitionsToPair ~coll (reify PairFlatMapFunction
                                  (call [this v#] (~f v#))) ~b)))

(defmacro map-partitions-with-index
  "Return a new RDD by applying a function to each partition of this RDD,
  while tracking the index of the original partition."
  [f b coll]
  `(.mapPartitionsWithIndex ~coll (reify Function2
                                    (call [this v# v2#] (~f v# v2#))) ~b))

(defmacro map->double
  "Return a new RDD by applying a function to all elements of this RDD."
  [f coll]
  `(.mapToDouble ~coll (reify DoubleFunction
                         (call [this v#] (~f v#)))))

(defmacro map->pair
  "Return a new RDD by applying a function to all elements of this RDD."
  [f coll]
  `(.mapToPair ~coll (reify PairFunction
                       (call [this v#] (~f v#)))))

;; Pair
(defmacro map-values
  "Pass each value in the key-value pair RDD through a map function without
  changing the keys; this also retains the original RDD's partitioning."
  [f coll]
  `(.mapValues ~coll (reify Function
                       (call [this v#] (~f v#)))))

(defmacro max
  "Returns the maximum element from this RDD as defined by the specified
  Comparator[T]."
  ([x] `(clojure.core/max ~x))
  ([c coll]
  `(cond
    (instance? JavaRDDLike ~coll) (.max ~coll ~c)
    :else (clojure.core/max ~c ~coll)))
  ([x y & more] `(clojure.core/max ~x ~y ~@more)))

;; Doub
(defmacro mean
  "Compute the mean of this RDD's elements."
  [coll]
  `(.mean ~coll))

;; Doub
(defmacro mean-approx
  ":: Experimental :: Approximate operation to return the mean within a
  timeout."
  ([t coll]
   `(.meanApprox ~coll ~t))
  ([t c coll]
   `(.meanApprox ~coll ~t ~c)))

(defmacro min
  "Returns the minimum element from this RDD as defined by the specified
  Comparator[T]."
  ([x] `(clojure.core/min ~x))
  ([c coll]
  `(cond
    (instance? JavaRDDLike ~coll) (.min ~coll ~c)
    :else (clojure.core/min ~c ~coll)))
  ([x y & more] `(clojure.core/min ~x ~y ~@more)))

(defmacro name
  [coll]
  `(cond
    (instance? JavaRDDLike ~coll) (.name ~coll)
    :else (clojure.core/name ~coll)))

;; Pair
(defmacro partition-by
  "Return a copy of the RDD partitioned using the specified partitioner."
  [p coll]
  `(.partitionBy ~coll ~p))

;; Reg, Pair, Doub
(defmacro persist
  "Set this RDD's storage level to persist its values across operations after
  the first time it is computed."
  [p coll]
  `(.persist ~coll ~p))

(defmacro pipe
  "Return an RDD created by piping elements to a forked external process."
  ([s coll]
   `(.pipe ~coll ~s))
  ([s e coll]
   `(.pipe ~coll ~s ~e)))

;; Reg, Pair, Doub
(defmacro rdd
  [coll]
  `(.rdd ~coll))

(defmacro reduce
  "Reduces the elements of this RDD using the specified commutative and
  associative binary operator."
  [f coll]
  `(cond
    (instance? JavaRDDLike ~coll)
    (.reduce ~coll (reify Function2
                     (call [this v# v2#] (~f v# v2#))))
    :else (clojure.core/reduce ~f ~coll)))

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

;; Pair
(defmacro reduce-by-key-locally
  "Merge the values for each key using an associative reduce function, but
  return the results immediately to the master as a Map."
  [f coll]
  `(into {} (.reduceByKeyLocally ~coll (reify Function2
                                         (call [this v# v2#] (~f v# v2#))))))

;; Reg, Pair, Doub
(defmacro repartition
  "Return a new RDD that has exactly numPartitions partitions."
  [n coll]
  `(.repartition ~coll ~n))

;; Pair
(defmacro right-outer-join
  "Perform a right outer join of this and other."
  ([rdd coll]
   `(.rightOuterJoin ~coll ~rdd))
  ([rdd p coll]
   `(.rightOuterJoin ~coll ~rdd ~p)))

;; Reg, Pair, Doub
(defmacro sample
  "Return a sampled subset of this RDD."
  ([b d coll]
   `(.sample ~coll ~b ~d))
  ([b d s coll]
   `(.sample ~coll ~b ~d ~s)))

;; Doub
(defmacro sample-std-dev
  "Compute the sample standard deviation of this RDD's elements (which corrects
  for bias in estimating the standard deviation by dividing by N-1 instead of
  N)."
  [coll]
  `(.sampleStdev ~coll))

;; Doub
(defmacro sample-variance
  "Compute the sample variance of this RDD's elements (which corrects for bias
  in estimating the standard variance by dividing by N-1 instead of N)."
  [coll]
  `(.sampleVariance ~coll))

;; Pair
(defmacro save-as-hadoop-dataset
  "Output the RDD to any Hadoop-supported storage system, using a Hadoop
  JobConf object for that storage system."
  [c coll]
  `(.saveAsHadoopDataSet ~coll ~c))

;; Pair
(defmacro save-as-hadoop-file
  "Output the RDD to any Hadoop-supported file system, compressing with the
  supplied codec."
  ([p c c2 f coll]
   `(.saveAsHadoopFile ~coll ~p ~c ~c2 ~f))
  ([p c c2 f e coll]
   `(.saveAsHadoopFile ~coll ~p ~c ~c2 ~f ~e)))

;; Pair
(defmacro save-as-new-api-hadoop-dataset
  "Output the RDD to any Hadoop-supported storage system, using a Configuration
  object for that storage system."
  [c coll]
  `(.saveAsNewAPIHadoopDataSet ~coll ~c))

;; Pair
(defmacro save-as-new-api-hadoop-file
  "Output the RDD to any Hadoop-supported file system."
  ([p c c2 f coll]
   `(.saveAsNewAPIHadoopFile ~coll ~p ~c ~c2 ~f))
  ([p c c2 f e coll]
   `(.saveAsNewAPIHadoopFile ~coll ~p ~c ~c2 ~f ~e)))

(defmacro save-as-object-file
  "Save this RDD as a SequenceFile of serialized objects."
  [s coll]
  `(.saveAsObjectFile ~coll ~s))

(defmacro save-as-text-file
  "Save this RDD as a compressed text file, using string representations of
  elements."
  ([s coll]
   `(.saveAsTextFile ~coll ~s))
  ([s c coll]
   `(.saveAsTextFile ~coll ~s ~c)))

;; Reg, Pair, Doub
(defmacro set-name
  "Assign a name to this RDD"
  [s coll]
  `(.setName ~coll ~s))

;; Pair
(defmacro sort-by-key
  "Sort the RDD by key, so that each partition contains a sorted range of the
  elements in ascending order."
  ([coll]
   `(.sortByKey ~coll))
  ([b coll]
   `(.sortByKey ~coll ~b))
  ([c b coll]
   `(.sortByKey ~coll ~c ~b))
  ([c b p coll]
   `(.sortByKey ~coll ~c ~b ~p)))

(defmacro splits
  "Set of partitions in this RDD."
  [coll]
  `(vec (.splits ~coll)))

;; Doub
(defmacro srdd [coll] `(.srdd ~coll))

;; Doub
(defmacro stats
  "Return a StatCounter object that captures the mean, variance and count of
  the RDD's elements in one operation."
  [coll]
  `(.stats ~coll))

;; Doub
(defmacro stdev
  "Compute the standard deviation of this RDD's elements."
  [coll]
  `(.stdev ~coll))

;; Reg, Pair, Doub
(defmacro subtract
  "Return an RDD with the elements from this that are not in other."
  ([rdd coll]
   `(.subtract ~coll ~rdd))
  ([rdd p coll]
   `(.subtract ~coll ~rdd ~p)))

;; Pair
(defmacro subtract-by-key
  "Return an RDD with the pairs from this whose keys are not in other."
  ([rdd coll]
   `(.subtractByKey ~coll ~rdd))
  ([rdd p coll]
   `(.subtractByKey ~coll ~rdd ~p)))

;; Doub
(defmacro sum
  "Add up the elements in this RDD."
  [coll]
  `(.sum ~coll))

;; Doub
(defmacro sum-approx
  ":: Experimental :: Approximate operation to return the sum within a timeout."
  ([t coll]
   `(.sumApprox ~coll ~t))
  ([t c coll]
   `(.sumApprox ~coll ~t ~c)))

(defmacro take
  [n coll]
  `(cond
    (instance? JavaRDDLike ~coll) (vec (.take ~coll ~n))
    :else (clojure.core/take ~n ~coll)))

(defmacro take-ordered
  "Returns the first K elements from this RDD as defined by the specified
  Comparator[T] and maintains the order."
  ([n coll]
   `(vec (.takeOrdered ~coll ~n)))
  ([n c coll]
   `(vec (.takeOrdered ~coll ~n ~c))))

(defmacro take-sample
  ([b n coll]
   `(vec (.takeSample ~coll ~b ~n)))
  ([b n s coll]
   `(vec (.takeSample ~coll ~b ~n ~s))))

(defmacro ->debug-str
  "A description of this RDD and its recursive dependencies for debugging."
  [coll]
  `(.toDebugString ~coll))

(defmacro ->local-iterator
  "Return an iterator that contains all of the elements in this RDD."
  [coll]
  `(.toLocalIterator ~coll))

;; Reg, Pair, Doub
(defmacro ->rdd [coll] `(.toRDD ~coll))

;; Reg
(defmacro ->str [coll] `(.toString ~coll))

(defmacro top
  "Returns the top K elements from this RDD as defined by the specified
  Comparator[T]."
  ([n coll]
   `(.top ~coll ~n))
  ([n c coll]
   `(.top ~coll ~n ~c)))

;; SET, Reg, Pair, Doub
(defmacro union
  "Return the union of this RDD and another one."
  [rdd coll]
  `(.union ~coll ~rdd))

;; Reg, Pair
(defmacro unpersist
  "Mark the RDD as non-persistent, and remove all blocks for it from memory
  and disk."
  ([coll]
   `(.unpersist ~coll))
  ([b coll]
   `(.unpersist ~coll ~b)))

;; Pair
(defmacro v-class-tag [coll] `(.vClassTag ~coll))

;; Pair
(defmacro values
  "Return an RDD with the values of each tuple."
  [coll]
  `(.values ~coll))

;; Doub
(defmacro variance
  "Compute the variance of this RDD's elements."
  [coll]
  `(.variance ~coll))

;; Reg, Pair, Doub
(defmacro wrap-rdd
  [rdd coll]
  `(.wrapRDD ~coll ~rdd))

(defmacro zip
  "Zips this RDD with another one, returning key-value pairs with the first
  element in each RDD, second element in each RDD, etc."
  [rdd coll]
  `(.zip ~coll ~rdd))

(defmacro zip-partitions
  "Zip this RDD's partitions with one (or more) RDD(s) and return a new RDD by
  applying a function to the zipped partitions."
  [rdd f coll]
  `(.zipPartitions ~coll ~rdd (reify FlatMapFunction2
                                (call [this v# v2#] (~f v# v2#)))))

(defmacro zip-with-index
  "Zips this RDD with its element indices."
  [coll]
  `(.zipWithIndex ~coll))

(defmacro zip-with-unique-id
  "Zips this RDD with generated unique Long ids."
  [coll]
  `(.zipWithUniqueId ~coll))
