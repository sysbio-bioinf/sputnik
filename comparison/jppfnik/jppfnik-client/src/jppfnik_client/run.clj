; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-client.run
  (:import
    sputnik.client.SputnikTask
    java.util.concurrent.CountDownLatch
    (org.jppf.client JPPFClient JPPFJob JPPFResultCollector)
    (org.jppf.client.event TaskResultListener TaskResultEvent)
    org.jppf.server.protocol.JPPFExceptionResult)
  (:require
    [jppfnik-client.progress :as progress]
    [clojure.string :as string])
  (:use
    [jppfnik-tools.functions :only [resolve-fn]]
    [clojure.stacktrace :only [print-cause-trace]]
    [clojure.options :only [defn+opts]]))


(defmacro wrap-error
  "Wraps the given body in a try catch which logs potential exceptions and rethrows them.
  A string representation of the body is included in the log and the exception."
  [& body]
  (let [info (format "Exception during execution of %s !" (string/join " " body))]
   `(try
      ~@body
      (catch Throwable t#
        (println ~info) (print-cause-trace t#) (flush)
        (throw (Exception. (str ~info " -> " (.getMessage t#)), t#))))))


(def callback-agent (agent 0))


(defn create-task
  [{:keys [task-id, task-function, task-data]}, alternative-id]
  (SputnikTask.
    (if (string? task-id) 
      task-id
      (str "Task " alternative-id))
    task-function, 
    task-data))


(defn create-job-id
  [job-id, id]
  (if (string? job-id)
    job-id
    (str "Job " id)))


(defn create-jppf-job
  [{:keys [job-id, tasks]}, alternative-job-id, alternative-task-id]
  (let [job (JPPFJob.),
        tasks (if (vector? tasks) tasks (vec tasks)),
        n (count tasks),
        dummy-args (make-array Object 0)]
    ; set name of the job
    (.setName job, (create-job-id job-id, alternative-job-id))
    ; set non-blocking
    (.setBlocking job false)
    ; create and add the tasks
    (loop [i 0]
      (if (< i n)
        (let [task-data (nth tasks i)]
          ; create SputnikTask from task data
          (.addTask job (create-task task-data, (+ i alternative-task-id)) dummy-args)
          (recur (unchecked-inc i)))
        ; return job
        {:job job, :task-count n}))))


(defn create-jobs
  [job-fn, job-args]
  (let [job-fn (resolve-fn job-fn),
        job-args (read-string job-args)        
        dummy-args (make-array Object 0)]
    (when-let [job-data-coll (some->> (apply job-fn job-args) (filter (fn [{:keys [tasks]}] (seq tasks))) seq vec)]
      (let [job-count (count job-data-coll)]
        (loop [alternative-job-id 0, total-task-count 0, jobs (transient [])]
          (if (< alternative-job-id job-count)
            (let [job-data (nth job-data-coll alternative-job-id)
                  {:keys [job, task-count]} (create-jppf-job job-data, alternative-job-id, total-task-count)]
              (recur (unchecked-inc alternative-job-id), (unchecked-add total-task-count (long task-count)), (conj! jobs job)))
            {:jobs (persistent! jobs), :task-count total-task-count}))))))




(defn print-unknown-tasks-info
  [unknown-tasks]
  (when (seq unknown-tasks) 
    (binding [*print-length* 10, *print-level* 5]
      (println (count unknown-tasks) "unknown tasks from task result event:")
      (doseq [task (remove nil? unknown-tasks)]
        (println "Class:" (class task) "\n" task)))))


(definterface IWaitCompletion
  [^void waitForCompletion []])


(defn create-task-result-listener
  [task-count, result-fn]
  (let [latch (CountDownLatch. task-count),
        result-fn (resolve-fn result-fn)]
    ; create proxy of TaskResultListener and IWaitCompletion
    (proxy [TaskResultListener, IWaitCompletion] []
      
	    (resultsReceived [^TaskResultEvent event]
        (try
          (let [; group in sputnik tasks, exception-results and rest
                {tasks SputnikTask, exception-results JPPFExceptionResult :as grouped-tasks} (group-by class (.getTaskList event)),
                ; determine unknown tasks
                unknown-tasks (mapcat val (dissoc grouped-tasks SputnikTask JPPFExceptionResult)),
                results (keep #(.getResult ^SputnikTask %) tasks),
                t (.getThrowable event),
                throwables (concat (when t [t]) (map #(.getException ^JPPFExceptionResult %) exception-results))]
            (print-unknown-tasks-info unknown-tasks)
            (result-fn results)
            ; count down latch
            (dotimes [_ (+ (count tasks) (count exception-results))]
              (.countDown latch)))))
     
	    (waitForCompletion []
        (.await latch)))))


(defn run
  "Retrieves the jobs via the given job function and converts them to sputnik tasks
  that are executed via a JPPFClient."
  [{:keys [job-fn, job-args, result-fn, progress]}]
  (let [{:keys [jobs task-count]} (create-jobs job-fn, job-args)
        result-listener (create-task-result-listener task-count, result-fn)]
    (if (seq jobs)
      (do 
        (progress/init task-count :print-progress progress)
		    (with-open [client (JPPFClient.)]      
          (println "Task Count =" task-count)
          (doseq [^JPPFJob job jobs]
            (.setResultListener job result-listener)
            ; submit job          
            (.submit client job))
			      ; wait until all jobs have been processed
		        (println "waiting for tasks!")
			      (.waitForCompletion ^IWaitCompletion result-listener)
		        (println "Client finished!")))
      (println "WARNING: No jobs specified!"))))


(defn create-client-from-config
  [config]
  (System/setProperty "jppf.config" (str config))
  (JPPFClient.))


(defn jppf-tasks->result-maps
  [task-list]
  (persistent!
    (reduce
      (fn [res, task]
        (cond
          (instance? SputnikTask task)
            (conj! res (:result-data (.getResult ^SputnikTask task)))
          (instance? JPPFExceptionResult task)
            (print-cause-trace (.getException ^JPPFExceptionResult task))
          :else (throw (RuntimeException. (str "Unexpected return value from JPPF of type " (class task) " !")))))
      (transient [])
      task-list)))

(defn compute-job
  ""
  [^JPPFClient client, job]
  (let [{:keys [^JPPFJob job, task-count]} (create-jppf-job job, 0, 0),
        result-collector (JPPFResultCollector. job)]
    (println "Task Count =" task-count)
    (.setResultListener job, result-collector)      
    ; submit job          
    (.submit client job)
    ; wait until job has been completed
    (println "waiting for tasks!")
    (let [results (jppf-tasks->result-maps (.waitForResults result-collector))]
      (println "Job finished!")
      results)))