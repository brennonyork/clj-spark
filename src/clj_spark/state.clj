(ns ^{:doc "clj-spark.state maintains all stateful operations on Spark RDD's."
      :author "Brennon York"}
  clj-spark.state)

;; Reg, Pair, Doub
(defmacro cache
  "Persist this RDD with the default storage level (`MEMORY_ONLY`)."
  [coll]
  `(.cache ~coll))

;; Reg, Pair, Doub
(defmacro persist
  "Set this RDD's storage level to persist its values across operations after
  the first time it is computed."
  [p coll]
  `(.persist ~coll ~p))

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

;; Reg, Pair
(defmacro unpersist
  "Mark the RDD as non-persistent, and remove all blocks for it from memory
  and disk."
  ([coll]
   `(.unpersist ~coll))
  ([b coll]
   `(.unpersist ~coll ~b)))
