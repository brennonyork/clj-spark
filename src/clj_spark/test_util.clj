(ns clj-spark.test-util
  (:require [clojure.test :refer [deftest]]
            [clj-spark.context :refer [with-context open]]))

(defmacro mk-test
  "Helper function to easily generate tests spanning across Clojure and Spark
  data structures."
  [test-name [fname] & body]
  `(deftest ~(symbol (str test-name "-test"))
     (with-context ctx# ["local[2]" "core-test-app"]
       (let [~(symbol fname) (open :file "LICENSE" ctx#)]
         ~@body))))
