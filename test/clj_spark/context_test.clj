(ns clj-spark.context-test
  (:import [org.apache.spark.api.java JavaRDDLike JavaSparkContext])
  (:require [clojure.test :refer [deftest is testing]]
            [clj-spark.context :refer :all]))

(deftest wrappers
  (testing "context wrappers"
    (with-context example-ctx ["local[4]" "context-test"]
      (is (= (type example-ctx) JavaSparkContext)))
    (with-context another-ctx ["local[1]" "another-ctx-test"]
      (is (instance? JavaRDDLike (.textFile another-ctx "LICENSE"))))))
