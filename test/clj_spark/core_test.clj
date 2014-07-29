(ns clj-spark.core-test
  (:refer-clojure :exclude [count distinct filter first get group-by keys map
                            max min name partition-by reduce take vals])
  (:import [org.apache.spark HashPartitioner])
  (:require [clojure.test :refer [is testing]]
            [clj-spark.test-util :refer [mk-test]]
            [clj-spark.core :refer :all]
            [clj-spark.contrib :as contrib]
            [clojure.string :as clj-str]))

(mk-test count [f]
  (testing "as clojure"
    (testing "with single arity"
      (is (= (count nil) 0))
      (is (= (count []) 0))
      (is (= (count [1 2 3]) 3))
      (is (= (count {:one 1 :two 2}) 2))
      (is (= (count [1 \a "string" [1 2] {:foo :bar}]) 5))
      (is (= (count "string") 6))))
  (testing "as spark"
    (is (= (count f) 21))))

(mk-test distinct [f]
  (testing "as clojure"
    (testing "with single arity"
      (is (= (distinct [1 2 1 3 1 4 1 5]) [1 2 3 4 5]))))
  (testing "as spark"
    (is (= (count (distinct f)) 18))))

(mk-test filter [f]
  (testing "as clojure"
    (testing "with double arity"
      (is (= (filter even? (range 10)) [0 2 4 6 8]))
      (is (= (filter (fn [x]
                       (= (count x) 1))
                     ["a" "aa" "b" "n" "f" "lisp" "clojure" "q" ""])
             ["a" "b" "n" "f" "q"]))
      (is (= (filter #(= (count %) 1)
                     ["a" "aa" "b" "n" "f" "lisp" "clojure" "q" ""])
             ["a" "b" "n" "f" "q"]))))
  (testing "as spark"
    (is (= (count (filter (fn [x] (not (clj-str/blank? x))) f)) 17))))

(mk-test first [f]
  (testing "as clojure"
    (testing "with single arity"
      (is (= (first [:alpha :bravo :charlie]) :alpha))
      (is (= (first nil) nil))
      (is (= (first []) nil))))
  (testing "as spark"
    (is (= (first f) "The MIT License (MIT)"))))

(mk-test get [f]
  (testing "as clojure"
    (testing "with double arity"
      (is (= (get :myk nil) nil))
      (is (= (get :myk {}) nil))
      (is (= (get 123 []) nil))
      (is (= (get :somek []) nil))
      (is (= (get "abdhsjf" nil) nil))
      (is (= (get "abcd" {"abcd" "qwerty" "zxcv" "asdfg"}) "qwerty"))
      (is (= (get :myk {:myk 12 :myb 23}) 12)))
    (testing "with triple arity"
      (is (= (get {:a 1 :b 2} :z "missing") "missing"))
      (is (= (get nil :b :c) :c)))
  (testing "as spark"
    (testing "with double arity"
      (is (= (str (get 21 (group-by count f)))
             "[[The MIT License (MIT)]]")))
    (testing "with triple arity"
      (is (= (get 22 (group-by count f) "missing"))
             "missing")))))

(mk-test group-by [f]
  (testing "as clojure"
    (testing "with double arity"
      (is (= (group-by count ["a" "as" "asd" "aa" "asdf" "qwer"])
             {1 ["a"], 2 ["as" "aa"], 3 ["asd"], 4 ["asdf" "qwer"]}))
      (is (= (group-by odd? (range 10))
             {false [0 2 4 6 8], true [1 3 5 7 9]}))
      (is (= (group-by :user-id [{:user-id 1 :uri "/"}
                                 {:user-id 2 :uri "/foo"}
                                 {:user-id 1 :uri "/account"}])
             {1 [{:user-id 1, :uri "/"} {:user-id 1, :uri "/account"}],
              2 [{:user-id 2, :uri "/foo"}]}))))
  (testing "as spark"
    (is (= (first (first (group-by count f))) 56))))

(mk-test keys [f]
  (testing "as clojure"
    (testing "with single arity"
      (is (= (keys {:keys :and, :some :values}) [:some :keys]))))
  (testing "as spark"
    (is (= (contrib/collect (keys (group-by count f)))
           [56 76 0 74 72 70 78 21 47 77 73 75 69 9 31]))))

(mk-test map [f]
  (testing "as clojure"
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
  (testing "as spark"
    (is (= (->> f
                (map (fn [x] (count (clj-str/split x #" "))))
                (first))
           4))))

(mk-test max [f]
  (testing "as clojure"
    (testing "with single arity"
      (is (= (max 100) 100)))
    (testing "with multi arity"
      (is (= (max 1 2 3 4 5) 5))
      (is (= (max 5 4 3 2 1) 5))))
  (testing "as spark"
    (is (= (->> f
                (map (fn [x] (count (clj-str/split x #" "))))
                (max <))
           16))))

(mk-test min [f]
  (testing "as clojure"
    (testing "with single arity"
      (is (= (min 100) 100)))
    (testing "with multi arity"
      (is (= (min 1 2 3 4 5) 1))
      (is (= (min 5 4 3 2 1) 1))))
  (testing "as spark"
    (is (= (->> f
                (map (fn [x] (count (clj-str/split x #" "))))
                (min <))
           1))))

(mk-test name [f]
  (testing "as clojure"
    (testing "with single arity"
      (is (= (name :x) "x"))
      (is (= (name "x") "x"))
      (is (= (name 'x) "x"))))
  (testing "as spark"
    (is (= (name (.setName f "License")) "License"))))

(mk-test partition-by [f]
  (testing "as clojure"
    (testing "with double arity"
      (is (= (partition-by #(= 3 %) [1 2 3 4 5]) [[1 2] [3] [4 5]]))
      (is (= (partition-by odd? [1 1 1 2 2 3 3]) [[1 1 1] [2 2] [3 3]]))
      (is (= (partition-by even? [1 1 1 2 2 3 3]) [[1 1 1] [2 2] [3 3]]))))
  (testing "as spark"
    (is (= (->> f
                (group-by count)
                (partition-by (HashPartitioner. 8))
                (first)
                (first))
           56))
    (is (= (->> f
                (group-by count)
                (partition-by nil)
                (first)
                (first))
           56))))

(mk-test reduce [f]
  (testing "as clojure"
    (testing "with double arity"
      (is (= (reduce + [1 2 3 4 5]) 15))
      (is (= (reduce + []) 0)))
    (testing "with triple arity"
      (is (= (reduce + 1 []) 1))
      (is (= (reduce + 1 [2 3]) 6))
      (is (= (reduce conj #{} [:a :b :c]) #{:a :c :b}))))
  (testing "as spark"
    (is (= (->> f
                (map (fn [x] (count (clj-str/split x #" "))))
                (reduce +))
           175))))

(mk-test take [f]
  (testing "as clojure"
    (testing "with double arity"
      (is (= (take 3 '(1 2 3 4 5 6)) [1 2 3]))
      (is (= (take 3 [1 2]) [1 2]))
      (is (= (take 1 []) []))
      (is (= (take 3 (drop 5 (range 1 11))) [6 7 8]))))
  (testing "as spark"
    (is (= (->> f
                (map (fn [x] (count (clj-str/split x #" "))))
                (take 5))
           [4 1 5 1 13]))))

(mk-test vals [f]
  (testing "as clojure"
    (testing "with single arity"
      (is (= (vals nil) nil))
      (is (= (vals []) nil))
      (is (= (vals {:a [1 2 3] :b 5}) [[1 2 3] 5]))))
  (testing "as spark"
    (is (= (->> f
                (group-by count)
                (vals)
                (first)
                str)
           "[furnished to do so, subject to the following conditions:]"))))
