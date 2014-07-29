(ns ^{:doc
      "Handles all Spark functions that correlate directly to clojure.set
      functions. These functions adhere to the contract that, agnostic of
      their input (instance? ISeq or JavaRDDLike), they will return their
      correct values regardless of collection."
      :author "Brennon York"}
  clj-spark.set
  (:import [org.apache.spark.api.java JavaRDDLike])
  (:require clojure.set))

(defn intersection
  "Return the intersection of this RDD and another one."
  ([s1] (clojure.set/intersection s1))
  ([rdd coll]
   (cond
    (instance? JavaRDDLike coll) (.intersection coll rdd)
    :else (clojure.set/intersection rdd coll)))
  ([s1 s2 & sets]
   (apply clojure.set/intersection (concat [s1 s2] sets))))

(defn union
  "Return the union of this RDD and another one."
  ([]
   (clojure.set/union))
  ([s1]
   (clojure.set/union s1))
  ([rdd coll]
   (cond
    (instance? JavaRDDLike coll) (.union coll rdd)
    :else (clojure.set/union rdd coll)))
  ([s1 s2 & sets]
   (apply clojure.set/union (concat [s1 s2] sets))))
