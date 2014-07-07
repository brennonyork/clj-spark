(ns clj-spark.core-test
  (:refer-clojure :exclude [count distinct filter first group-by keys map max
                            min name partition-by reduce take])
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as clj-str]
            [clj-spark.core :refer :all]
            [clj-spark.api :as api])
  (:import [org.apache.spark.api.java.function Function])
  (:gen-class))

(deftest maintain-origins
  (testing "count"
    (testing "with single arity"
      (is (= (count nil) 0))
      (is (= (count []) 0))
      (is (= (count [1 2 3]) 3))
      (is (= (count {:one 1 :two 2}) 2))
      (is (= (count [1 \a "string" [1 2] {:foo :bar}]) 5))
      (is (= (count "string") 6))))
  (testing "distinct"
    (testing "with single arity"
      (is (= (distinct [1 2 1 3 1 4 1 5]) [1 2 3 4 5]))))
  (testing "filter"
    (testing "with double arity"
      (is (= (filter even? (range 10)) [0 2 4 6 8]))
      (is (= (filter (fn [x]
                       (= (count x) 1))
                     ["a" "aa" "b" "n" "f" "lisp" "clojure" "q" ""])
             ["a" "b" "n" "f" "q"]))
      (is (= (filter #(= (count %) 1)
                     ["a" "aa" "b" "n" "f" "lisp" "clojure" "q" ""])
             ["a" "b" "n" "f" "q"]))))
  (testing "first"
    (testing "with single arity"
      (is (= (first [:alpha :bravo :charlie]) :alpha))
      (is (= (first nil) nil))
      (is (= (first []) nil))))
  (testing "group-by"
    (testing "with double arity"
      (is (= (group-by (fn [x] (count x)) ["a" "as" "asd" "aa" "asdf" "qwer"])
             {1 ["a"], 2 ["as" "aa"], 3 ["asd"], 4 ["asdf" "qwer"]}))
      (is (= (group-by odd? (range 10))
             {false [0 2 4 6 8], true [1 3 5 7 9]}))
      (is (= (group-by :user-id [{:user-id 1 :uri "/"}
                                 {:user-id 2 :uri "/foo"}
                                 {:user-id 1 :uri "/account"}])
             {1 [{:user-id 1, :uri "/"} {:user-id 1, :uri "/account"}],
              2 [{:user-id 2, :uri "/foo"}]}))))
  (testing "keys"
    (testing "with single arity"
      (is (= (keys {:keys :and, :some :values}) [:some :keys]))))
  (testing "map"
    (testing "with single arity"
      (is (= (map inc [1 2 3 4 5]) [2 3 4 5 6]))
      (is (= (map (fn [x] [x]) [1 2 3]) [[1] [2] [3]])))
    (testing "with double arity"
      (is (= (map + [1 2 3] [4 5 6]) [5 7 9]))
      (is (= (map + [1 2 3] (iterate inc 1)) [2 4 6]))
      (is (= (map (fn [x y] [x y]) [1 2 3] [4 5 6]) [[1 4] [2 5] [3 6]])))
    (testing "with triple arity"
      (is (= (apply (fn [x y z]
                      (map vector x y z)) [[:a :b :c]
                                           [:d :e :f]
                                           [:g :h :i]])
             [[:a :d :g] [:b :e :h] [:c :f :i]]))
      (is (= (map (fn [x y z] [x y z]) [1 2 3] [4 5 6] [7 8 9])
             [[1 4 7] [2 5 8] [3 6 9]]))))
  (testing "max"
    (testing "with single arity"
      (is (= (max 100) 100)))
    (testing "with multi arity"
      (is (= (max 1 2 3 4 5) 5))
      (is (= (max 5 4 3 2 1) 5))))
  (testing "min"
    (testing "with single arity"
      (is (= (min 100) 100)))
    (testing "with multi arity"
      (is (= (min 1 2 3 4 5) 1))
      (is (= (min 5 4 3 2 1) 1))))
  (testing "name"
    (testing "with single arity"
      (is (= (name :x) "x"))
      (is (= (name "x") "x"))
      (is (= (name 'x) "x"))))
  (testing "partition-by"
    (testing "with double arity"
      (is (= (partition-by #(= 3 %) [1 2 3 4 5]) [[1 2] [3] [4 5]]))
      (is (= (partition-by odd? [1 1 1 2 2 3 3]) [[1 1 1] [2 2] [3 3]]))
      (is (= (partition-by even? [1 1 1 2 2 3 3]) [[1 1 1] [2 2] [3 3]]))))
  (testing "reduce"
    (testing "with double arity"
      (is (= (reduce + [1 2 3 4 5]) 15))
      (is (= (reduce + []) 0)))
    (testing "with triple arity"
      (is (= (reduce + 1 []) 1))
      (is (= (reduce + 1 [2 3]) 6))
      (is (= (reduce conj #{} [:a :b :c]) #{:a :c :b}))))
  (testing "take"
    (testing "with double arity"
      (is (= (take 3 '(1 2 3 4 5 6)) [1 2 3]))
      (is (= (take 3 [1 2]) [1 2]))
      (is (= (take 1 []) []))
      (is (= (take 3 (drop 5 (range 1 11))) [6 7 8])))))

(deftest transformations
  (let [sc (api/ctx "local[2]" "transform-tests")]
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
