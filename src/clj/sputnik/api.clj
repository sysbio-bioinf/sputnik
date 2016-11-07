; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.api
  "This namespace contains the main client API to submit jobs to a running Sputnik cluster (server + workers)."
  (:refer-clojure :exclude [future])
  (:require
    [clojure.options :refer [defn+opts, ->option-map]]
    (sputnik.satellite
      [client :as client]
      [protocol :as protocol]
      [executor :as exec]
      [worker :as worker])
    (sputnik.tools
      [error :as e]
      [format :as fmt]
      [logging :as log]))
  (:import
    java.io.Closeable
    (java.util.concurrent ExecutorService Executors)))



(defn create-client-from-config
  "Creates a Sputnik client for distributed computation using the configuration file given by the `config-url`."
  [config-url]
  (client/create-client-from-config config-url))


(defn create-task
  "Create a task with the given task-id, execution function and parameter data."
  [task-id, execute-fn, & param-data]
  (apply protocol/create-task task-id, execute-fn, param-data))


(defn create-job
  "Creates a job with the given job-id using the given tasks (created via 'create-task')."
  [job-id, tasks]
  (protocol/create-job job-id, tasks))


(defn time-job-id
  "Creates a job id with the given prefix and a timestamp as suffix.
  The timestamp is formatted such that string sorting of job ids leads to a chronological order."
  [prefix]
  (str prefix " " (fmt/datetime-filename-format (System/currentTimeMillis))))


(defn+opts execute-job
  "Submits the job to the client and waits until all tasks are completed.
  This function does not return anything - a handler function (fn [client, task-results] ...) has to be specified to get the task results.
  With the handler function you have access to the task definition and its result."
  [client, job | :as options]
  (client/execute-job client, job, options))


(defn+opts compute-job
  "Submits the job to the client, waits until all tasks are completed and returns the results of the task computations."
  [client, job | :as options]
  (client/compute-job client, job, options))




(deftype LocalExecutionClient [^ExecutorService thread-pool-executor] 
  
  client/ISputnikClient
  
  (submit-job [this, job-data, result-callback-fn]
    ; siwtch between multi-threaded with thread pool and single-threaded with direct execution
    (if thread-pool-executor
      ; preserve bindings
      (let [frame (clojure.lang.Var/cloneThreadBindingFrame)]
        ; pass tasks to thread pool executor
        (doseq [task (:tasks job-data)]
          (.execute thread-pool-executor
            (^:once fn* []
              (clojure.lang.Var/resetThreadBindingFrame frame)
              (result-callback-fn this, (vector (worker/start-execution nil, task))))))) ; nil instead of a node
      (doseq [task (:tasks job-data)]
        (result-callback-fn this, (vector (worker/start-execution nil, task)))))
    nil)
  
  (job-finished [this, job-data] #_nothingtodo)
  
  Closeable
  (close [this]
    (when thread-pool-executor
      ; shut down thread pool executor
      (.shutdownNow thread-pool-executor))
    nil))


(defn+opts create-local-execution-client
  "Creates a client for local computation using a specified number of threads."
  [| {thread-count 1}]
  (when-not (and (integer? thread-count) (<= 1 thread-count))
    (e/illegal-argument ":thread-count must be an integer >= 1 instead of %s" thread-count))
  (LocalExecutionClient. (Executors/newFixedThreadPool (int thread-count))))


(defn+opts ^java.io.Closeable create-client
  "Creates a client that for either distributed or local computation.
  (Convenience method for parameter dependent choice on call-site)
  <mode>Specifies the excution mode whether the Sputnik cluster, local parallel or sequential execution is used.</>
  <sputnik-config>Specifies a Sputnik configuration filename which is needed for the Sputnik mode.</>
  <thread-count>Specifies the number of threads to use when local parallel execution is used</>"
  [| {mode (choice :sputnik, :parallel, :sequential), sputnik-config nil, thread-count 2}]
  (case mode
    :sputnik    (create-client-from-config sputnik-config),
    :parallel   (create-local-execution-client :thread-count thread-count),
    :sequential (LocalExecutionClient. nil)))


(defn+opts configure-logging
  "Configures the logging environment with the given options."
  [| :as options]
  (log/configure-logging options))


(defn+opts ^java.io.Closeable create-executor
  [| :as options]
  (exec/create-executor (create-client options), options))


(def ^:dynamic *sputnik-executor* nil)

(defmacro with-sputnik
  "Creates a scope such that Sputnik futures that are calculated in parallel on a Sputnik cluster can be used within the given body.
  The parameters of the option-map refer to the ones accepted by `create-executor`."
  {:arglists '([{:keys [min-task-count, timeout, execution-batch-size, mode, sputnik-config, thread-count] :as option-map} & body])}
  [option-map & body]
  `(let [sputnik-executor# (create-executor (->option-map ~option-map))]
     ; manual `with-open` implementation to explicitely mark the executor as failed
     (try
       (binding [*sputnik-executor* sputnik-executor#]
         (let [result# (do ~@body)]
           (exec/await-termination sputnik-executor#)
           result#))
       (catch Throwable t#
         (exec/fail sputnik-executor#)
         (throw t#))
       (finally
         (.close ^java.io.Closeable sputnik-executor#)))))


(defn resolve-symbol
  [x]
  (if-let [^clojure.lang.Var x (resolve x)]
    (symbol (.. x ns name getName) (.. x sym getName))
    (e/illegal-argument "Unable to resolve symbol %s" x)))


(defn enqueue
  [f, args]
  (when-not (symbol? f)
    (e/illegal-argument "expected a symbol referring to a function but found %s" f))
  (exec/enqueue *sputnik-executor*, f, args))


(defmacro future
  {:arglists '([(f & args)])}
  [[f, & args]]
  (when-not (symbol? f)
    (e/illegal-argument "sputnik.api/future expects a list `(f arg1 arg2 ...)` starting with a symbol `f` referring to a function but found %s" f))
  `(enqueue '~(resolve-symbol f), ~(vec args)))