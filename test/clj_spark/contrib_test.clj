(ns clj-spark.contrib-test
  (:import [org.apache.spark.api.java JavaRDDLike JavaRDD JavaDoubleRDD
            JavaPairRDD JavaSparkContext])
  (:require [clojure.test :refer [deftest is testing]]
            [clj-spark.contrib :refer :all]
            [clj-spark.context :as context]))

(deftest maintain-origins
  (testing "collect"
    (is (= (collect (into {} (map #(vec [(keyword (str %)) %]) [1 2 3 4])))
           {:1 1 :2 2 :3 3 :4 4}))
    (is (= (collect [1 2 2 3]) [1 2 2 3]))
    (is (= (collect (set [1 2 3 4])) #{1 2 3 4}))
    (is (= (collect []) []))
    (is (= (collect {}) {}))
    (is (= (collect #{}) #{}))
    (is (= (collect "some string") nil)))
  (testing "count-by-key"
    (is (= (count-by-key []) nil))
    (is (= (count-by-key {:a [1 2 3 4] :b [2 4 4 6] :c [1 2]})
           {:a 4 :b 4 :c 2}))
    (is (= (count-by-key {}) {})))
  (testing "count-by-value"
    (is (= (count-by-value nil) {}))
    (is (= (count-by-value []) {}))
    (is (= (count-by-value {}) {}))
    (is (= (count-by-value {:a 1 :b [2 3 4] :c [3 3 3 3]})
           {1 1 2 1 3 5 4 1}))
    (is (= (count-by-value {:a [1 2 3] :b [:c :d :d] "1" ["a" "c"]})
           {1 1 2 1 3 1 :c 1 :d 2 "a" 1 "c" 1}))))
