(ns clj-spark.api-test
  (:refer-clojure :exclude [filter map reduce])
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as clj-str]
            [clj-spark.api :refer :all])
  (:import [org.apache.spark.api.java.function Function])
  (:gen-class))

(deftest maintain-origins
  (testing "map"
    (testing "with single arity"
      (is (= (map (fn [x] [x]) [1 2 3])
             [[1] [2] [3]])))
    (testing "with double arity"
      (is (= (map (fn [x y] [x y]) [1 2 3] [4 5 6])
             [[1 4] [2 5] [3 6]])))
    (testing "with triple arity"
      (is (= (map (fn [x y z] [x y z]) [1 2 3] [4 5 6] [7 8 9])
             [[1 4 7] [2 5 8] [3 6 9]])))))

(deftest transformations
  (let [sc (ctx "local[2]" "transform-tests")]
    (testing "map"
      (is (= (->> (.textFile sc "README.md")
                  (map (fn [x] (count (clj-str/split x #" "))))
                  (reduce (fn [x y] (+ x y))))
             11)))
    (testing "count"
      (is (= (->> (.textFile sc "README.md")
                  (count))
             4)))
    (.stop sc)))
