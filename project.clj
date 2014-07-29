(defproject clj-spark "0.1.0-SNAPSHOT"
  :description "Idiomatic Clojure bindings for Apache Spark"
  :url "http://example.com/FIXME"
  :license {:name "MIT"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.3.0"]
                 [org.apache.spark/spark-core_2.10 "1.0.0"]]
  :prep-tasks ["clean" "compile"]
  :aot [clj-spark.context-test
        clj-spark.contrib-test
        clj-spark.core-test
        clj-spark.set-test])
