(ns clj-spark.util)

(defmacro unbox-tuple2
  [x]
  `(identity [(._1 ~x) (._2 ~x)]))

(defmacro unbox-tuple3
  [x]
  `(identity [(._1 ~x) (._2 ~x) (._3 ~x)]))

(defmacro box-tuple2
  ([[x y]]
   `(scala.Tuple2 ~x ~y))
  ([x y]
   `(scala.Tuple2. ~x ~y)))

(defmacro box-tuple3
  [x y z]
  `(scala.Tuple3. ~x ~y ~z))
