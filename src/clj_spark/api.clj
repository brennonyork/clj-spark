(ns clj-spark.api
  (:refer-clojure :exclude [count
                            filter
                            first
                            group-by
                            map
                            max
                            min
                            name
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

(defmacro count-approx
  ":: Experimental :: Approximate version of count() that returns a potentially
  incomplete result within a timeout, even if not all tasks have finished."
  ([t coll])
  ([t c coll]))

(defmacro count-approx-distinct
  "Return approximate number of distinct elements in the RDD."
  [r coll]
  `(.countApproxDistinct ~coll ~r))

(defmacro count-by-value
  "Return the count of each unique value in this RDD as a map of (value, count)
  pairs."
  [t coll]
  `(into {} (.countByValue ~coll ~t)))

(defmacro count-by-value-approx
  "(Experimental) Approximate version of countByValue()."
  ([t coll])
  ([t c coll]))

(defmacro filter
  [f coll]
  `(cond
    (instance? JavaRDDLike ~coll)
    (.filter ~coll (reify Function
                     (call [this v#] (~f v#))))
    :else (clojure.core/filter ~f ~coll)))

(defmacro first
  [coll]
  `(cond
    (instance? JavaRDDLike ~coll) (.first ~coll)
    :else (clojure.core/first ~coll)))

(defmacro flatmap
  "Return a new RDD by first applying a function to all elements of this RDD,
  and then flattening the results."
  [f coll]
  `(.flatMap ~coll (reify FlatMapFunction
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

(defmacro id
  "A unique ID for this RDD (within its SparkContext)."
  [coll]
  `(.id ~coll))

(defmacro is-checkpointed
  "Return whether this RDD has been checkpointed or not"
  [coll]
  `(.isCheckpointed ~coll))

(defmacro iterator
  "Internal method to this RDD; will read from cache if applicable, or
  otherwise compute it."
  [s t coll]
  `(.iterator ~coll ~s ~t))

(defmacro key-by
  "Creates tuples of the elements in this RDD by applying f."
  [f coll]
  `(.keyBy ~coll (reify Function
                   (call [this v#] (~f v#)))))

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

(defmacro max
  "Returns the maximum element from this RDD as defined by the specified
  Comparator[T]."
  ([x] `(clojure.core/max ~x))
  ([c coll]
  `(cond
    (instance? JavaRDDLike ~coll) (.max ~coll ~c)
    :else (clojure.core/max ~c ~coll)))
  ([x y & more] `(clojure.core/max ~x ~y ~@more)))

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

(defmacro pipe
  "Return an RDD created by piping elements to a forked external process."
  ([s coll]
   `(.pipe ~coll ~s))
  ([s e coll]
   `(.pipe ~coll ~s ~e)))

(defmacro rdd
  [coll]
  `(.rdd ~coll))

(defmacro reduce
  [f coll]
  `(cond
    (instance? JavaRDDLike ~coll)
    (.reduce ~coll (reify Function2
                     (call [this v# v2#] (~f v# v2#))))
    :else (clojure.core/reduce ~f ~coll)))

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

(defmacro splits
  "Set of partitions in this RDD."
  [coll]
  `(vec (.splits ~coll)))

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

(defmacro top
  "Returns the top K elements from this RDD as defined by the specified
  Comparator[T]."
  ([n coll]
   `(.top ~coll ~n))
  ([n c coll]
   `(.top ~coll ~n ~c)))

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
