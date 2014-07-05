(defproject clj-spark "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "MIT"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.spark/spark-core_2.10 "1.0.0"]]
  :aot :all;[clj-spark.api]
  :main clj-spark.core
  ;:main ^:skip-aot clj-spark.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
