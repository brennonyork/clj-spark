(ns ^{:doc
      "Provides users with convenient Clojure-based functions for handling
      Spark context objects."
      :author "Brennon York"}
  clj-spark.context
  (:import [org.apache.spark.api.java JavaSparkContext])
  (:require [clj-spark.util :as util]
            [clojure.tools.logging :as log]))

(defmacro parallelize
  "Converts a Clojure collection into a Spark RDD for processing."
  ([coll ctx]
   `(.parallelize ~ctx (java.util.ArrayList. ~coll)))
  ([coll numSlices ctx]
   `(.parallelize ~ctx (java.util.ArrayList. ~coll) ~numSlices)))

(defmacro parallelize->double
  "Converts a Clojure collection into a Spark DoubleRDD for processing."
  ([coll ctx]
   `(.parallelizeDoubles ~ctx (java.util.ArrayList. (map double ~coll))))
  ([coll numSlices ctx]
   `(.parallelizeDoubles ~ctx (java.util.ArrayList. (map double ~coll))
                         ~numSlices)))

(defmacro parallelize->pair
  "Converts a Clojure collection into a Spark PairRDD for processing. Elements
  within the collection should come in the form of vector pairs."
  ([coll ctx]
   `(.parallelizePairs ~ctx (java.util.ArrayList.
                             (clojure.core/map (fn [[x# y#]]
                                                 (util/box-tuple2 x# y#)) ~coll))))
  ([coll numSlices ctx]
   `(.parallelizePairs ~ctx (java.util.ArrayList.
                             (clojure.core/map (fn [[x# y#]]
                                                 (util/box-tuple2 x# y#)) ~coll))
                       ~numSlices)))

(defmacro open
  "Handles simplified opening of different file types into their Spark RDD
  equivalent. Below is the breakdown from Clojure keyword to their Java
  method counterparts:

  .textFile           => :file OR :f
  .sequenceFile       |
  .hadoopFile         |
  .newAPIHadoopFile   |

  .wholeTextFiles     => :directory OR :dir

  .objectFile         => :object OR :obj
  .parallelize        |
  .parallelizeDoubles |
  .parallelizePairs   |

  .hadoopRDD          => :rdd
  .newAPIHadoopRDD    |

  Examples:
  (open :file \"LICENSE\" spark-ctx)      => Calls .textFile
  (open :obj [[1 2] [3 4]] spark-ctx)   => Calls .parallelizePairs
  (open :dir \"./testDir/\" spark-ctx)    => Calls .wholeTextFiles
  (open :file \"/tmp/seqFile.seq\"        => Calls .sequenceFile
              java.lang.String
              java.lang.Long spark-ctx)"
  [k path & more]
  (if (empty? more)
    (log/error "`open` requires a SparkContext as the last parameter")
    (let [ctx (last more)
          more (drop-last more)]
      (cond
        ;; -> .textFile || .sequenceFile || .hadoopFile || .newAPIHadoopFile
        (or (= k :file) (= k :f))
        (let [mc (count (drop-last more))]
          (cond
           (or (= mc 0) (= mc 1)) `(.textFile ~ctx ~path ~@more)
           (= mc 2) `(.sequenceFile ~ctx ~path ~@more)
           (= mc 3) (if (integer? (last more))
                       `(.sequenceFile ~ctx ~path ~@more)
                       `(.hadoopFile ~ctx ~path ~@more))
           (= mc 4) (if (integer? (last more))
                       `(.hadoopFile ~ctx ~path ~@more)
                       `(.newAPIHadoopFile ~ctx ~path ~@more))))
        ;; -> .wholeTextFiles
        (or (= k :directory) (= k :dir))
        `(.wholeTextFiles ~ctx ~path ~@more)
        ;; -> .objectFile || .parallelize
        (or (= k :object) (= k :obj))
        (cond
         (string? path) `(.objectFile ~ctx ~path ~@more)
         (reduce (fn [x y] (and x y)) (map float? path))
         `(parallelize->double ~path ~@more ~ctx)
         (reduce (fn [x y] (and x y)) (map (fn [x]
                                             (and (vector? x)
                                                  (= (count x) 2))) path))
         `(parallelize->pair ~path ~@more ~ctx)
         :else `(parallelize ~path ~@more ~ctx))
        ;; -> .hadoopRDD || .newAPIHadoopRDD
        (= k :rdd)
        (cond
         (= (type path) org.apache.hadoop.mapred.JobConf)
         `(.hadoopRDD ~ctx ~path ~@more)
         (= (type path) org.apache.hadoop.conf.Configuration)
         `(.newAPIHadoopRDD ~ctx ~path ~@more))))))

(defmacro with-context
  "Defines a wrapper around a SparkContext to ensure it gets shut down
  properly. All elements inside the `ctx-args` vector will be expanded into
  the JavaSparkContext constructor.

  Example: (with-context sc [\"local[2]\" \"my-app\"] ... )"
  [sym [& ctx-args] & body]
  `(let [~(symbol sym) (JavaSparkContext. ~@ctx-args)
         start-time# (.startTime ~(symbol sym))]
     (try
       ~@body
       (finally (.stop ~(symbol sym))))
     (- (System/currentTimeMillis) start-time#)))
