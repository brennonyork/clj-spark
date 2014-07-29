(ns clj-spark.util)

;; Loosely based on: http://stackoverflow.com/a/20312211
(defn arg-count [f]
  (let [m (filter #(= "invoke" (.getName %)) (.getDeclaredMethods (class f)))
        p (map #(.getParameterTypes %) m)]
    (map alength p)))

(defn unbox-tuple2
  [x]
  [(._1 x) (._2 x)])

(defn unbox-tuple3
  [x]
  [(._1 x) (._2 x) (._3 x)])

(defn box-tuple2
  ([[x y]]
   (scala.Tuple2. x y))
  ([x y]
   (scala.Tuple2. x y)))

(defn box-tuple3
  ([[x y z]]
   (scala.Tuple3. x y z))
  ([x y z]
   (scala.Tuple3. x y z)))
