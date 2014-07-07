(defproject clj-spark "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "MIT"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.spark/spark-core_2.10 "1.0.0"]]
  :prep-tasks ["clean" "compile"]
  :aot [clj-spark.core
        clj-spark.core-test
        clj-spark.set
        clj-spark.set-test
        clj-spark.contrib
        clj-spark.contrib-test])
