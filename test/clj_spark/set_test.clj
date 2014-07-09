(ns clj-spark.set-test
  (:require [clojure.test :refer [deftest is testing]]
            [clj-spark.set :refer :all]
            ;[clj-spark.contrib :as contrib]
            ;[clj-spark.api :as api]
            )
  (:gen-class))

(deftest maintain-origins
  (testing "intersection"
    (testing "with single arity"
      (is (= (intersection #{1}) #{1})))
    (testing "with double arity"
      (is (= (intersection #{1 2} #{2 3}) #{2})))
    (testing "with triple arity"
      (is (= (intersection #{1 2} #{2 3} #{3 4}) #{}))
      (is (= (intersection #{1 :a} #{:a 3} #{:a}) #{:a}))))
  (testing "union"
    (testing "with null arity"
      (is (= (union) #{})))
    (testing "with single arity"
      (is (= (union #{1 2}) #{1 2})))
    (testing "with double arity"
      (is (= (union #{1 2} #{2 3}) #{1 2 3})))
    (testing "with triple arity"
      (is (= (union #{1 2} #{2 3} #{3 4}) #{1 2 3 4})))))

;(deftest set-transformations)
