(ns clj-spark.util)

(defmacro unbox-tuple2
  [x]
  `(identity [(._1 ~x) (._2 ~x)]))

(defmacro unbox-tuple3
  [x]
  `(identity [(._1 ~x) (._2 ~x) (._3 ~x)]))
