(ns clj-spark.core
  (:refer-clojure :exclude [count distinct filter first group-by keys map max
                            min name partition-by reduce take]))

(defmacro count
  "Return the number of elements in the RDD."
  [coll]
  `(cond
    (instance? JavaRDDLike ~coll) (.count ~coll)
    :else (clojure.core/count ~coll)))

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

;; Pair
(defmacro partition-by
  "Return a copy of the RDD partitioned using the specified partitioner."
  [p coll]
  `(.partitionBy ~coll ~p))

(defmacro reduce
  "Reduces the elements of this RDD using the specified commutative and
  associative binary operator."
  [f coll]
  `(cond
    (instance? JavaRDDLike ~coll)
    (.reduce ~coll (reify Function2
                     (call [this v# v2#] (~f v# v2#))))
    :else (clojure.core/reduce ~f ~coll)))

(defmacro take
  [n coll]
  `(cond
    (instance? JavaRDDLike ~coll) (vec (.take ~coll ~n))
    :else (clojure.core/take ~n ~coll)))
