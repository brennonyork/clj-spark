clj-spark
=========

Idiomatic Clojure bindings for the [Apache Spark](https://spark.apache.org) framework.

## Overview

Currently there exists no Clojure binding library atop the Apache Spark framework that operates well from a *user* standpoint. This project aims to resolve those issues by providing a series of files and functions that interoperate between traditional Clojure datasets as well as the Spark RDD objects.

For example, Spark has a concept of `map`, but wait, doesn't Clojure have one of those as well? I think so. So why should you, as a developer, worry about another library and namespace when both functions have the same expected outputs, merely operating on different objects? Well we don't have to anymore...

With `clj-spark` we looked at how the current Spark implementations were handling function serialization and their various methods, deciding there must be a better way. Instead of making custom versions of `map`, `reduce`, etc. `clj-spark` *overrides* the default `clojure.core` functionality and merely *adds* to it the ability to operate on Spark RDD objects as well!

### A Simple Example

TODO: Once the API has settled down.

## Caveats

Now, given we do things completely different from things like the older [clj-spark](https://github.com/TheClimateCorporation/clj-spark) library and the newer [flambo](https://github.com/yieldbot/flambo) there are a few things people should know about...

1. All projects must be AOT-compiled
2. All methods that accept a function cannot be passed an individual macro
  * For example: `(map count (parallelize [1 2 3 4]))` will cause an error because `count` is now a Clojure macro. Instead this line would have to be written as `(map (fn [x] (count x)) (parallelize [1 2 3 4]))`.

### Acknowledgements

TheClimateCorporation/clj-spark for their initial implementation of the library and giving me inspiration to move the concept forward.
