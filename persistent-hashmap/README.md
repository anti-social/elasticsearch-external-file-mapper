[![Build Status](https://travis-ci.org/anti-social/persistent-hashmap.svg?branch=master)](https://travis-ci.org/anti-social/persistent-hashmap)
[![Download](https://api.bintray.com/packages/evo/maven/persistent-hashmap/images/download.svg) ](https://bintray.com/evo/maven/persistent-hashmap/_latestVersion)

# Persistent hashmap

A hash map implementation that stores data on disk. The hash map can be shared by multiple processes. 
The only requirement: there can be a single writer. So it is possible to use it for interprocess communication.

Only primitive types are supported: `int` and `long` for keys; `short`, `int`, `long`, `float` and `double` for values.

At the moment there is no atomicity for multiple operations.
The only garantee is that every single operation (`put`, `get`, `remove`) is atomic.
