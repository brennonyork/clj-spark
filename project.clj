(defproject clj-spark "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "MIT"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.3.0"]]
  :profiles {:dev
             {:prep-tasks ["clean" "compile"]
              :aot [clj-spark.function
                    ; tests
;;                     clj-spark.contrib-test
;;                     clj-spark.core-test
;;                     clj-spark.example-test
;;                     clj-spark.set-test
                    ]}
             :test
             {:aot [; tests
                    clj-spark.contrib-test
                    clj-spark.core-test
                    clj-spark.example-test
                    clj-spark.set-test]}
             :provided
             {:dependencies
              [[org.apache.spark/spark-core_2.10 "1.0.1"]
               [org.apache.spark/spark-streaming_2.10 "1.0.1"]
               [org.apache.spark/spark-streaming-kafka_2.10 "1.0.1"]
               [org.apache.spark/spark-sql_2.10 "1.0.1"]]}}
  :repl-options {:init-ns clj-spark.core
                 :init (do
                         (require '[clj-spark.context :as ctx])
                         (require '[clj-spark.contrib :as contrib])
                         (require '[clj-spark.set :as sset])
                         (def sc (org.apache.spark.api.java.JavaSparkContext.
                                  "local" "repl-context")))})
