; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-client.task
  "Defines the class sputnik.client.SputnikTask which needs to be compiled so that JPPF can transfer this class 
  to the server and the nodes."
  (:gen-class
    :name sputnik.client.SputnikTask
    :extends org.jppf.server.protocol.JPPFTask
    :state taskData
    :init constructor
    :constructors {[String, Object, Object] []}    
    :prefix task-)
  (:require
    [clojure.string :as string])
  (:use
    [jppfnik-tools.functions :only [resolve-fn]]
    [clojure.stacktrace :only [print-cause-trace]])
  (:import
    (java.lang.management ManagementFactory ThreadMXBean)
    org.slf4j.LoggerFactory))



(let [thread-bean ^ThreadMXBean (ManagementFactory/getThreadMXBean)]
  (defn current-thread-cpu-time
    "Returns the total CPU time for the current thread in nanoseconds. The returned value is of nanoseconds precison but not necessarily nanoseconds accuracy. If the implementation distinguishes between user mode time and system mode time, the returned CPU time is the amount of time that the current thread has executed in user mode or system mode." 
    []
    (.getCurrentThreadCpuTime thread-bean)))

(defn current-thread-cpu-time-millis
  "Returns the total CPU time for the current thread in milliseconds."
  []
  (/ (current-thread-cpu-time) 1000000.0))


(defn task-constructor
  "Creates a sputnik task."
  [id, function, data]
  [[] {:task-id id, :function function, :data data}])



(defmacro wrap-error-log
  "Wraps the given body in a try catch which logs potential exceptions and rethrows them.
  A string representation of the body is included in the log and the exception."
  [log, & body]
  (let [info (format "Exception during execution of %s !" (string/join " " body))]
   `(try
      ~@body
      (catch Throwable t#
        (.error ~log (with-out-str (println ~info) (print-cause-trace t#)))
        (throw (Exception. (str ~info " -> " (.getMessage t#)), t#))))))


(defn task-run
  "Runs a sputnik task."
  [this]
  (let [log (LoggerFactory/getLogger sputnik.client.SputnikTask)]
    (.debug log "Task run started!")
		(try		  
		  (let [{:keys [function data]} (.taskData this),        
		        start-time (System/currentTimeMillis),
		        start-cpu-time (current-thread-cpu-time-millis),		        
		        f (wrap-error-log log, (resolve-fn function))
		        result (wrap-error-log log, (apply f data)),
		        end-cpu-time (current-thread-cpu-time-millis)
		        end-time (System/currentTimeMillis)]
		    (.debug log "Task result will be created next.")
		    (.setResult this
		      (assoc (.taskData this)
		        :start-time start-time,
		        :end-time end-time,
		        :cpu-time (- end-cpu-time start-cpu-time),
		        :result-data result)))     
		   (catch Throwable t
         (.debug log (with-out-str (println "Task run resulted in an exception:") (print-cause-trace t)))
		     (.setResult this
           (assoc (.taskData this)
             :exception t))))))