(ns clj-spark.example-test
  (:import [org.apache.spark.api.java.function PairFunction])
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as clj-str]
            [clj-spark.core :refer :all]
            [clj-spark.api :as api]
            [clj-spark.context :refer [with-context open]]
            [clj-spark.contrib :as contrib])
  (:gen-class))

(defmacro mk-test
  "Helper function to easily generate tests spanning across Clojure and Spark
  data structures."
  [test-name [fname] & body]
  `(deftest ~(symbol (str test-name "-test"))
     (with-context ctx# ["local[2]" "core-test-app"]
       (let [~(symbol fname) (open :file "LICENSE" ctx#)]
         ~@body))))

(mk-test word-count [f]
  (testing "word count of the LICENSE file"
    (is (= (->> f
                (api/flatmap (fn [x] (clj-str/split x #" ")))
                (api/map->pair (fn [x] [x 1]))
                (contrib/reduce-by-key (fn [x y] (+ x y)))
                (get "the"))
           [6]))))
