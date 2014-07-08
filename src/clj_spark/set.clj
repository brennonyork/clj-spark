(ns ^{:doc
      "clj-spark.set maintains all Spark functions that correlate directly to
      clojure.set functions. These functions adhere to the contract that,
      agnostic of their input (instance? ISeq or JavaRDDLike), they will return
      their correct values regardless of collection."
      :author "Brennon York"}
  clj-spark.set)

;; SET, Reg, Pair, Doub
(defmacro intersection
  "Return the intersection of this RDD and another one."
  [rdd coll]
  `(.intersection ~coll ~rdd))

;; SET, Reg, Pair, Doub
(defmacro union
  "Return the union of this RDD and another one."
  [rdd coll]
  `(.union ~coll ~rdd))
