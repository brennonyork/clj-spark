(ns clj-spark.function
  (:import [org.apache.spark.api.java.function
            DoubleFlatMapFunction
            DoubleFunction
            FlatMapFunction
            FlatMapFunction2
            Function
            Function2
            Function3
            PairFlatMapFunction
            PairFunction
            VoidFunction]))

(deftype ^{:doc
           "A function that returns zero or more records of type Double from
           each input record."}
  DoubleFlatMapFunc [f]
  DoubleFlatMapFunction
  (^Iterable call [this v] (f v)))

(deftype ^{:doc
           "A function that returns Doubles, and can be used to construct
           DoubleRDDs."}
  DoubleFunc [f]
  DoubleFunction
  (^double call [this v] (f v)))

(deftype ^{:doc
           "A function that returns zero or more output records from each input
           record."}
  FlatMapFunc [f]
  FlatMapFunction
  (^Iterable call [this v] (f v)))

(deftype ^{:doc
           "A function that takes two inputs and returns zero or more output
           records."}
  FlatMapFunc2 [f]
  FlatMapFunction2
  (^Iterable call [this v1 v2] (f v1 v2)))

(deftype ^{:doc
           "Base interface for functions whose return types do not create
           special RDDs."}
  Func [f]
  Function
  (call [this v] (f v)))

(deftype ^{:doc
           "A two-argument function that takes arguments of type T1 and T2 and
           returns an R."}
  Func2 [f]
  Function2
  (call [this v1 v2] (f v1 v2)))

(deftype ^{:doc
           "A three-argument function that takes arguments of type T1, T2 and
           T3 and returns an R."}
  Func3 [f]
  Function3
  (call [this v1 v2 v3] (f v1 v2 v3)))

(deftype ^{:doc
           "A function that returns zero or more key-value pair records from
           each input record."}
  PairFlatMapFunc [f]
  PairFlatMapFunction
  (^Iterable call [this v] (f v)))

(deftype ^{:doc
           "A function that returns key-value pairs (Tuple2), and can be used
           to construct PairRDDs."}
  PairFunc [f]
  PairFunction
  (^scala.Tuple2 call [this v] (f v)))

(deftype ^{:doc
           "A function with no return value."}
  VoidFunc [f]
  VoidFunction
  (^void call [this v] (f v)))
