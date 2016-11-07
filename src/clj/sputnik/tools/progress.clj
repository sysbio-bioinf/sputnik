; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.tools.progress
  (:require
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [clojure.options :refer [defn+opts]]
    [sputnik.tools.format :as fmt]
    [sputnik.tools.performance-data :as pd]))


(def ^:const interval-count 5)

(defrecord ProgressData
  [period-performance-data, total-performance-data,
   completed-task-count, total-task-count, 
   start-time, output-format, print-timeout, last-print-time])



(defn- update-progress-data
  [progress-data, task-result, now]
  (try
    (if-let [ex (:exception task-result)]
      ; exception:
      (update-in progress-data [:completed-task-count] inc) ; TODO add exception to progress-data   
      ; regular result:
      (let [{:keys [start-time, end-time, cpu-time]} task-result,
            duration (- end-time start-time)] 
        (-> progress-data    
          (update-in [:period-performance-data] pd/add-task-data start-time end-time cpu-time)
          (update-in [:total-performance-data] pd/add-task-data start-time end-time cpu-time)
          (update-in [:completed-task-count] inc)))) 
   (catch Throwable t 
     (log/error (format "Error in update-progress-data:\n%s" (with-out-str (print-cause-trace t)))) 
     (throw t))))



(defn duration-estimation-string
  [data]
  (if data
    (format "%s   (finshing on %s - total: %s)"
      (fmt/duration-with-days-format (:estimated-duration data))
      (fmt/datetime-format (:estimated-end data))
      (fmt/duration-with-days-format (:estimated-total-duration data)))
    "N/A"))

(defn add-estimations
  [now, {:keys [completed-task-count, total-task-count, start-time] :as progress-data}, performance-data]
  (when performance-data
    (let [mean-speed (pd/performance-attribute performance-data, :mean-speed),
          estimated-duration (/ (- total-task-count completed-task-count) mean-speed)]
      (assoc (pd/computation-performance->map performance-data)
        :estimated-duration estimated-duration
        :estimated-total-duration (+ estimated-duration (- now start-time))
        :estimated-end (+ now estimated-duration)))))


(defn format-speed
  [time-unit, value]
  (->>
    (case time-unit
	    "sec"     1000
	    "min"  (* 1000 60)
	    "hour" (* 1000 60 60)
      "day"  (* 1000 60 60 24)
      "week" (* 1000 60 60 24 7))
    (* value)
    (format "%,.2f")))


(defn- print-current-progress
  [now, {:keys [period-performance-data, total-performance-data, completed-task-count, total-task-count,
                start-time, output-format]
         :as progress-data}]
  (let [total-spent-time (long (- now start-time)),
        last? (== completed-task-count total-task-count),
        period-data (add-estimations now, progress-data, (pd/computation-performance period-performance-data, interval-count)),
        total-data  (add-estimations now, progress-data, (pd/computation-performance total-performance-data))]
    (println     
	    (format
        output-format
	      completed-task-count
	      total-task-count
        (if last?
          100.0
          (* 100 (/ (double completed-task-count) total-task-count)))
        (fmt/datetime-format now)))  
    (println 
      (format "|Experiment duration:\n|RECENTLY ~ %s\n|ENTIRELY ~ %s"
        (or (some->> period-data :mean-duration double (format "%.3fms")) "N/A")
        (or (some->> total-data  :mean-duration double (format "%.3fms")) "N/A")))
    (println "|")
    (println (format "|Execution efficiency:\n|RECENTLY ~ %s\n|ENTIRELY ~ %s"
               (or (some->> period-data :mean-efficiency double (format "%.3f")) "N/A")
               (or (some->> total-data  :mean-efficiency double (format "%.3f")) "N/A")))
    (println "|")
    (println (format "|Total spent time:\n|SUM ~ %s" (fmt/duration-with-days-format total-spent-time)))
    (println "|")
    (when (not last?)
      (println 
        (format "|Estimated remaining time:\n|RECENTLY ~ %s\n|ENTIRELY ~ %s"
          (duration-estimation-string period-data)
          (duration-estimation-string total-data)))
      (println "|"))
    (println (format "|Estimated concurrency:\n|RECENTLY ~ %s\n|ENTIRELY ~ %s"
               (or (some->> period-data :mean-concurrency double (format "%.2f")) "N/A")
               (or (some->> total-data  :mean-concurrency double (format "%.2f")) "N/A")))
    (println "|")
    (println (format "|Estimated speed:\n|RECENTLY ~ %s Tasks/min\n|ENTIRELY ~ %s Tasks/min\n"
               (or (some->> period-data :mean-speed double (* 60 1000) (format "%.2f")) "N/A")
               (or (some->> total-data  :mean-speed double (* 60 1000) (format "%.2f")) "N/A")))
		(print \newline)))


(def ^:private ^:const exception-separator (format "\n%s\n" (apply str (repeat 40 "-"))))

(defn- update+print-progress
  [progress-data, results, exceptions]
  (log/trace (str (count results) " results reported!"))
  (try
	  (let [now (System/currentTimeMillis),         
	        ; updata progress data with task result
	        progress-data (reduce #(update-progress-data %, %2, now) progress-data results),
	        {:keys [print-timeout last-print-time completed-task-count total-task-count]} progress-data]
      (when (seq exceptions)
        (let [msg (format "%s exceptions during task execution:\n%s"
                    (count exceptions)
                    (str/join exception-separator exceptions))]
          (log/error msg)
          (println msg)
          (flush)))
	    (if (or (= completed-task-count total-task-count) (<= print-timeout (- now last-print-time)))
	      (do
	        ; [progress-data, start-time, end-time, now, experiment-cpu-time, process-cpu-time]          
	        (print-current-progress now, progress-data)
	        (flush)
	        ; return progress-data with now as last-print-time
	        (assoc progress-data :last-print-time now))
	      ; return progress data
	      progress-data))
   (catch Throwable t 
     (log/error (format "Error in update+print-progress:\n%s" (with-out-str (print-cause-trace t)))) 
     (throw t))))



(defn+opts create-progress-report
  "Returns a progress report function.
  <estimation-period>Estimation period (in minutes) for RECENTLY concurrency and runtime.</estimation-period>
  <print-timeout>Minimum time (in milliseconds) between two progress report prints to std-out.</print-timeout>
  <run-desc>Name of the single steps that the progress agent reports about, e.g. runs of an algorithm.</run-desc>
  <print-progress>Specifies whether the progress shall be printed.</print-progress>
  "
  [total-task-count | {estimation-period 30, run-desc "Task", print-timeout 1000, print-progress true}]
  (when print-progress
    (let [now (System/currentTimeMillis)
          progress-agent (agent 
                           (ProgressData.
                             ; period-performance-data,
                             (pd/create-period-task-computation-data (quot (* estimation-period 60 1000) interval-count), interval-count, now)
                             ; total-performance-data,
                             (pd/create-total-task-computation-data)
                             ; completed-task-count, 
                             0,
                             ; total-task-count,
                             total-task-count,
                             ; start-time,
                             now
                             ; output-format,
                             (str run-desc " %0" (-> total-task-count str count) "d/%d (%05.2f%%) - %s"),
                             ; print-timeout,
                             print-timeout
                             ; last-print-time
                             0))]
      (fn report-progress [results, exceptions]
        (send-off progress-agent, update+print-progress, results, exceptions)))))