(ns clj-spark.set-test
  (:require [clojure.test :refer [is testing]]
            [clj-spark.test-util :refer [mk-test]]
            [clj-spark.set :refer :all])
  (:gen-class))

(mk-test intersection [f]
  (testing "as clojure"
    (testing "with single arity"
      (is (= (intersection #{1}) #{1})))
    (testing "with double arity"
      (is (= (intersection #{1 2} #{2 3}) #{2})))
    (testing "with triple arity"
      (is (= (intersection #{1 2} #{2 3} #{3 4}) #{}))
      (is (= (intersection #{1 :a} #{:a 3} #{:a}) #{:a}))))
  (testing "as spark"
    ))

(mk-test union [f]
  (testing "as clojure"
    (testing "with null arity"
      (is (= (union) #{})))
    (testing "with single arity"
      (is (= (union #{1 2}) #{1 2})))
    (testing "with double arity"
      (is (= (union #{1 2} #{2 3}) #{1 2 3})))
    (testing "with triple arity"
      (is (= (union #{1 2} #{2 3} #{3 4}) #{1 2 3 4}))))
  (testing "as spark"
    ))
