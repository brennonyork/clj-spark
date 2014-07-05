(ns clj-spark.core
  (:refer-clojure :exclude [map reduce])
  (:import [org.apache.spark.api.java JavaSparkContext]
           [org.apache.spark.api.java.function Function Function2])
  (:require [clojure.string :as clj-str]
            [clj-spark.api :as api :refer [map reduce]])
  (:gen-class))

(defn mk-context
  [a b]
  (JavaSparkContext. a b))

(defn -main
  []
  (let [tf (.textFile (mk-context "local[2]" "my-app") "README.md" 2)]
    (println
     (reduce (fn [x y] (+ x y)) (map (fn [x] (count (clj-str/split x #" "))) tf)))))
