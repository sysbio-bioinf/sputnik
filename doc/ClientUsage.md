# Client Usage

The easiest approach to parallelize a job consisting of separate tasks with Sputnik using its client API
is outlined in the following.
The following code listings are based on the project [simple-example](../example-application/simple-example). 

```clojure
(ns simple-example.core
  (:require
    [sputnik.satellite.protocol :as p]
    [sputnik.satellite.client :as c]))

...

(defn distributed-calculation
  [config-url]
  ; create client from configuration file
  (with-open [^java.io.Closeable client (c/create-client-from-config config-url)]
    ; execute the job and wait for its results
    (let [task-results (c/compute-job client, (create-job param1, param2))]
      (println "Result:" (aggregate-results task-results))))
```

The above example shows how to create a Sputnik client from an URL to a [client configuration file](ConfigurationDeployment.md#client-configuration)
and submit a job to Sputnik server that the client connects to.
The function ```c/compute-job``` returns the task results synchronously.
The ```with-open``` macro ensures that the client is properly closed at the end or in case of exceptions.

A job with a given unique id ```some-job-id``` and given parameter lists for each task ```param-vector-list```
can be created as follows.
```clojure
(p/create-job some-job-id,  
  (mapv
    (fn [task-id, params] (apply p/create-task task-id, 'my.ns/calculate, params)
    (range)
    param-vector-list))
```
Notice that the function ```calculate``` from namespace ```my.ns``` that will perform the calculation is given as symbol.


The case where an asynchronous execution is desired such that unrelated computations can be performed while the distributed
computation is running can be implemented by specifying the option ```:async true``` like in the following example.
```clojure
(let [async-results (c/compute-job client, (create-job param1, param2), :async true),
      unrelated-computations-result (do-unrelated-computations)]  
  (aggregate-both-computations (deref async-results), unrelated-computations-result))
```
The form ```(deref async-results)``` blocks until all tasks of the job were completed.
In the example the unrelated computations can be performed during the distributed computation of the job.