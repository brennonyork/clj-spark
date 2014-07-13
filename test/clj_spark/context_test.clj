(ns clj-spark.context-test
  (:import [org.apache.spark.api.java JavaRDDLike JavaRDD JavaDoubleRDD
            JavaPairRDD JavaSparkContext])
  (:require [clojure.test :refer [deftest is testing]]
            [clj-spark.context :refer :all]))

(deftest wrappers
  (testing "context wrappers"
    (with-context example-ctx ["local[4]" "context-test"]
      (is (= (type example-ctx) JavaSparkContext)))
    (with-context another-ctx ["local[1]" "another-ctx-test"]
      (is (instance? JavaRDDLike (.textFile another-ctx "LICENSE"))))
    (let [t (with-context s ["local[10]" "timing-test"])]
      (is (and (pos? t) (integer? t))))))

(deftest file-descriptors
  (with-context test-ctx ["local[2]" "descriptor-test"]
    (testing "objects"
      (is (instance? JavaRDD (open :obj [1 2 3 4] test-ctx)))
      (is (instance? JavaDoubleRDD (open :object [1.0 2.0 3.0] test-ctx)))
      (is (instance? JavaPairRDD (open :object [[1 2] [3 4] [5 6]] test-ctx))))))
