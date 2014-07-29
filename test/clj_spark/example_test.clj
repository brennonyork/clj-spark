(ns clj-spark.example-test
  (:import [org.apache.spark.api.java.function PairFunction])
  (:require [clojure.test :refer [is testing]]
            [clj-spark.test-util :refer [mk-test]]
            [clojure.string :as clj-str]
            [clj-spark.core :refer :all]
            [clj-spark.api :as api]
            [clj-spark.contrib :as contrib]))

(refer 'clojure.core :exclude '[count distinct filter first get group-by keys map
                                max min name partition-by reduce take vals])

(mk-test word-count [f]
  (testing "word count of the LICENSE file"
    (is (= (->> f
                (api/flatmap (fn [x] (clj-str/split x #" ")))
                (api/map->pair (fn [x] [x 1]))
                (contrib/reduce-by-key (fn [x y] (+ x y)))
                (get "the"))
           [6]))))
