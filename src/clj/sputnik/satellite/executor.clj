; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.executor
  (:require
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.tools.logging :as log]
    [clojure.options :refer [defn+opts, ->option-map]]
    (sputnik.satellite
      [client :as c]
      [protocol :as protocol])
    [sputnik.tools.error :as e]
    [clojure.string :as str])
  (:import
    java.util.concurrent.TimeUnit))


(defn create-future-wrapper
  [task-promise]
  (let [target-unit (TimeUnit/MILLISECONDS)]
    (reify 
      clojure.lang.IDeref 
      
      (deref [_]
        (deref task-promise))
    
      clojure.lang.IBlockingDeref
      
      (deref [_ timeout-ms timeout-val]
        (deref task-promise timeout-ms timeout-val))
    
      clojure.lang.IPending
    
      (isRealized [_]
        (realized? task-promise))
    
      java.util.concurrent.Future
    
      (get [_]
        (deref task-promise))
    
      (get [_ timeout unit]
        (deref task-promise (.convert target-unit, timeout, unit), nil))
    
      (isCancelled [_]
        ; no cancellation implemented
        false)
      
      (isDone [_]
        (realized? task-promise))
      
      (cancel [_ interrupt?]
        ; no cancellation implemented
        false))))



(defprotocol ISputnikExecutor
  (enqueue-task [sputnik-executor, fn-symbol, args])
  (await-finshed-tasks [sputnik-executor])
  (fail [sputnik-executor]))


(defn create-id
  [uuid, counter]
  (str uuid "-" (swap! counter inc)))

(declare submit-job)

; task-counter (atom 0)
; job-counter (atom 0)
; enqueued-tasks (ref [])
; submitted-task-map (ref {}), task-id -> promise
(deftype SputnikExecutor [sputnik-client, execution-batch-size, uuid, task-counter, job-counter, enqueued-tasks, submitted-task-map, terminate?, scheduling-has-terminated?, all-tasks-finished?]
  
  ISputnikExecutor
  
  (enqueue-task [this, fn-symbol, args]
    (when @terminate?
      (e/exception "It is not possible to enqueue additional tasks because the Sputnik executor has already been told to terminate!"))
    (let [task-id (create-id uuid, task-counter),
          task-promise (dosync
                         (let [task-promise (promise),
                               task (protocol/create-task* task-id, fn-symbol, args)]
                           (alter enqueued-tasks conj task)
                           ; keep task promise which is used to create a future that is returned to the caller
                           (alter submitted-task-map assoc task-id task-promise)
                           task-promise))]
      (create-future-wrapper task-promise)))
  
  (await-finshed-tasks [this]
    (reset! terminate? true)
    (deref scheduling-has-terminated?)
    (when (pos? (count @enqueued-tasks))
      (deref all-tasks-finished?)))
  
  (fail [this]
    (reset! terminate? true)
    (deliver all-tasks-finished? false))
  
  java.io.Closeable
  
  (close [this]
    ; ensure that all tasks are finished
    (try
      (await-finshed-tasks this)
      (finally
        (.close ^java.io.Closeable sputnik-client)))))


(defn submit-jobs-periodically
  [^SputnikExecutor executor, timeout, min-task-count]
  (let [terminate? (.terminate? executor),
        enqueued-tasks (.enqueued-tasks executor),
        scheduling-has-terminated? (.scheduling-has-terminated? executor),
        maybe-submit (fn maybe-submit []
                       (try
                         (let [; retrieve enqueued tasks
                               tasks-to-submit (dosync
                                                 (let [tasks (ensure enqueued-tasks)]
                                                   ; when either no minimum task count specified, or at least that many tasks are available
                                                   (when (or (nil? min-task-count) (>= (count tasks) min-task-count))
                                                     ; then clear queue and return selected tasks
                                                     (ref-set enqueued-tasks [])
                                                     tasks)))]
                           (when (seq tasks-to-submit)
                             (submit-job executor, tasks-to-submit)))
                         (catch Throwable t
                           (log/errorf "Exception encountered in periodical job submission maybe-submit:\n%s"
                             (with-out-str (print-cause-trace t))))))]
    (log/debugf "Job submission thread started.")
    (loop []
      ; if timeout specified, wait for more tasks 
      (when timeout
        (try (Thread/sleep timeout) (catch InterruptedException e nil)))
      ; submit tasks
      (maybe-submit)      
      ; loop as long as nobody waits for termination
      (if @terminate?
        ; termination signaled submit remaining tasks
        (do
          (maybe-submit)
          (deliver scheduling-has-terminated? true)
          (log/debugf "Job submission thread terminates now."))
        ; continue
        (recur)))))


(defn tasks-finished
  [^SputnikExecutor sputnik-executor, client, finished-tasks]
  (let [task-map (deref (.submitted-task-map sputnik-executor)),
        task-id-vec (persistent!
                      (reduce
                        (fn [task-id-vec, {{:keys [task-id]} :task-data, {:keys [result-data]} :execution-data}]
                          (if-let [task-promise (get task-map task-id)]
                            (deliver task-promise result-data)
                            (log/errorf "No promise found for task %s in submitted-task-map!" task-id))
                          (conj! task-id-vec task-id))    
                        (transient [])
                        finished-tasks)),
        remaining-tasks (dosync (alter (.submitted-task-map sputnik-executor) #(apply dissoc % task-id-vec)))]
    (log/tracef "%s task finished - %s remaining tasks. teminate? %s" (count finished-tasks) (count remaining-tasks) (deref (.terminate? sputnik-executor)))
    ; when termination requested and no tasks remain, ...
    (when (and (deref (.terminate? sputnik-executor)) (zero? (count remaining-tasks)))
      (println "termination signaled!")
      ; ... signal termination
      (deliver (.all-tasks-finished? sputnik-executor) true))
    nil)) 


(defn submit-job
  [^SputnikExecutor sputnik-executor, tasks-to-submit]
  (let [sputnik-client (.sputnik-client sputnik-executor),
        execution-batch-size (.execution-batch-size sputnik-executor)
        job-id (create-id (.uuid sputnik-executor), (.job-counter sputnik-executor))]
    ; asynchronous job execution (otherwise submit-jobs-periodically would block until the job is finished)
    (c/compute-job sputnik-client, (protocol/create-job job-id, tasks-to-submit),
      :result-callback (partial tasks-finished sputnik-executor),
      :batch-size execution-batch-size,
      :check false,
      :async true,
      ; do not print progress since multiple jobs are running
      :print-progress false)))


(declare submit-batched-processing-job submit-remaining-tasks)

(deftype BlockingBatchedProcessing [sputnik-client, execution-batch-size, ^long job-task-count, uuid, task-counter, job-counter, current-job-finished?, enqueued-tasks, submitted-task-map, terminate?, all-tasks-finished?]

  ISputnikExecutor

  (enqueue-task [this, fn-symbol, args]
    (when @terminate?
      (e/exception "It is not possible to enqueue additional tasks because the BlockingBatchedProcessing executor has already been told to terminate!"))
    ; block until current job is done (only one job at a time)
    ; single threaded enqueuing blocks already in enqueue-task when calling submit-batched-processing-job
    ; for multithreaded enquing we need current-job-finished? for synchronisation
    (when-let [done? (deref current-job-finished?)]
      (deref done?))
    (let [task-id (create-id uuid, task-counter),
          task-promise (dosync
                         (let [task-promise (promise),
                               task (protocol/create-task* task-id, fn-symbol, args)]
                           (alter enqueued-tasks conj task)
                           ; keep task promise which is used to create a future that is returned to the caller
                           (alter submitted-task-map assoc task-id task-promise)
                           task-promise)),
          tasks-to-submit (dosync
                            (let [tasks (ensure enqueued-tasks)]
                              (when (>= (count tasks) job-task-count)
                                (let [tasks-to-submit (vec (take job-task-count tasks))]
                                  (alter enqueued-tasks (fn [task-set] (reduce disj task-set tasks-to-submit)))
                                  tasks-to-submit))))]
      (when (seq tasks-to-submit)
        (submit-batched-processing-job this, tasks-to-submit))
      (create-future-wrapper task-promise)))

  (await-finshed-tasks [this]
    (reset! terminate? true)

    (submit-remaining-tasks this)

    (when (pos? (count @enqueued-tasks))
      (deref all-tasks-finished?)))

  (fail [this]
    (reset! terminate? true)
    (deliver all-tasks-finished? false))

  java.io.Closeable

  (close [this]
    ; ensure that all tasks are finished
    (try
      (await-finshed-tasks this)
      (finally
        (.close ^java.io.Closeable sputnik-client)))))


(defn submit-remaining-tasks
  "Submits the remaining tasks that have not been submitted since there were too few compared to the specified job-task-count."
  [^BlockingBatchedProcessing executor]
  (let [tasks (dosync
                (let [tasks (deref (.enqueued-tasks executor))]
                  (ref-set (.enqueued-tasks executor) #{})
                  tasks))]
    (when (seq tasks)
      (when-let [done? (deref (.current-job-finished? executor))]
        (deref done?))
      (submit-batched-processing-job executor, tasks))))


(defn batched-processing-tasks-finished
  [^BlockingBatchedProcessing executor, client, finished-tasks]
  (let [task-map (deref (.submitted-task-map executor)),
        ; deliver task results to promises
        task-id-vec (persistent!
                      (reduce
                        (fn [task-id-vec, {{:keys [task-id]} :task-data, {:keys [result-data]} :execution-data}]
                          (if-let [task-promise (get task-map task-id)]
                            (deliver task-promise result-data)
                            (log/errorf "No promise found for task %s in submitted-task-map!" task-id))
                          (conj! task-id-vec task-id))
                        (transient [])
                        finished-tasks)),
        remaining-tasks (dosync (alter (.submitted-task-map executor) #(apply dissoc % task-id-vec)))]
    (log/tracef "%s task finished - %s remaining tasks. teminate? %s" (count finished-tasks) (count remaining-tasks) (deref (.terminate? executor)))
    ; when no tasks remain, ...
    (when (zero? (count remaining-tasks))
      ; mark current job as done
      (when-let [done? (deref (.current-job-finished? executor))]
        (deliver done? true)
        (reset! (.current-job-finished? executor) nil))
      ; when termination requested as well, ...
      (when (deref (.terminate? executor))
        ; ... signal termination
        (deliver (.all-tasks-finished? executor) true)))
    nil))


(defn submit-batched-processing-job
  "Submits job and blocks until it is finished."
  [^BlockingBatchedProcessing executor, tasks-to-submit]
  (let [sputnik-client (.sputnik-client executor),
        execution-batch-size (.execution-batch-size executor)
        current-job-finished? (.current-job-finished? executor)
        job-id (create-id (.uuid executor), (.job-counter executor))]
    (reset! current-job-finished? (promise))
    (c/execute-job sputnik-client, (protocol/create-job job-id, tasks-to-submit),
      :result-callback (partial batched-processing-tasks-finished executor),
      :batch-size execution-batch-size,
      :check false,
      :print-progress false)))


(defn+opts create-executor
  "Creates a Sputnik executor using the specified client.
  <min-task-count>Specifies the number of tasks (if any) that are bundled to a job. If not specified, a job is created after the given timeout.</>
  <timeout>Specifies the timeout after which all enqueued tasks are bundled to a job.</>"
  [sputnik-client | {min-task-count nil, timeout 100, execution-batch-size nil, executor-mode (choice :future :blocking), block-size 100}]
  (let [uuid (str (java.util.UUID/randomUUID)),
        executor (case executor-mode
                   :future   (SputnikExecutor. sputnik-client, execution-batch-size, uuid, (atom 0), (atom 0), (ref #{}), (ref {}), (atom false), (promise), (promise))
                   :blocking (BlockingBatchedProcessing. sputnik-client, execution-batch-size, block-size, uuid, (atom 0), (atom 0), (atom nil), (ref #{}), (ref {}), (atom false), (promise)))]
    (when (and timeout (not (and (integer? timeout) (pos? timeout))))
      (e/illegal-argument "timeout must be an positive integer specifying the timeout in milliseconds - found: %s" timeout))
    (when (and min-task-count (not (and (integer? min-task-count) (pos? min-task-count))))
      (e/illegal-argument "min-task-count must be an positive integer specifying the the minimum task cout to submit a job - found: %s" timeout))
    (future (submit-jobs-periodically executor, timeout, min-task-count))
    executor))


(defn enqueue
  [sputnik-executor, fn-symbol, args]
  (when-not (satisfies? ISputnikExecutor sputnik-executor)
    (e/illegal-argument "A Sputnik executor is required to submit computation tasks. Did you forget to use `sputnik.api/with-sputnik`?"))
  (enqueue-task sputnik-executor, fn-symbol, args))


(defn await-termination
  [sputnik-executor]
  (await-finshed-tasks sputnik-executor))