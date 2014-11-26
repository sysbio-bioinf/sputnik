; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-client.progress
  (:use
    [clojure.stacktrace :only [print-cause-trace]]
    [clojure.options :only [defn+opts]]))




(defrecord ProgressData 
  [^double cpu-runtime-avg, ^double execution-efficiency-avg, ^long completed-task-count, ^long total-task-count, 
   ^Long start-time, output-format, ^long print-timeout, ^long last-print-time,
   ^long estimation-period, ^Long last-estimation-time, ^Double concurrency-ema, ^Double cpu-runtime-ema,
   ^long thread-cpu-time-sum, ^long thread-real-time-sum,
   ^boolean print-progress])


(def progress-agent (agent nil)) 


(defn+opts init
  "Initializes the progress report.
  <estimation-period>Estimation period (in minutes) for Exponential Moving Average of concurrency and runtime.</estimation-period>
  <print-timeout>Minimum time (in milliseconds) between two progress report prints to std-out.</print-timeout>
  <run-desc>Name of the single steps that the progress agent reports about, e.g. runs of an algorithm.</run-desc>
  <print-progress>Specifies whether the progress shall be printed.</print-progress>
  "
  [total-task-count | {estimation-period 1, run-desc "Task", print-timeout 1000, print-progress true}]
  (let [now (System/currentTimeMillis)] 
    (send-off progress-agent 
	    (constantly 
	      (ProgressData.
		      ; cpu-runtime-avg
		      0.0, 
		      ; execution-efficiency-avg
		      0.0,
		      ; completed-task-count
		      0, 
		      ; total-task-count
		      total-task-count,
		      ; start-time
		      now,
		      ; output-format
		      (str run-desc " %0" (-> total-task-count str count) "d/%d (%05.2f%%) - %s") ,
		      ; print-timeout
		      print-timeout,
		      ; last-print-time
		      0,
		      ; estimation-period
		      (* estimation-period 60000),
		      ; last-estimation-time
		      now,
		      ; concurrency-ema
		      nil,
		      ; cpu-runtime-ema
		      nil,
          ; thread-cpu-time-sum
          0,
          ; thread-real-time-sum
          0,
          ; print-progress
          (boolean print-progress))))))


(defn- update-avg 
  [avg, duration, n]
  (let [q (/ 1 (double (inc n)))]
    (+ 
      (* (- 1 q) avg)
      (* q duration)
    )
  )
)


(defn- update-ema
  [ema, value, delta-time, period]
  (if ema
	  (let [factor (Math/exp (- (/ (double delta-time) period)))]
	    (+
	      (* factor ema)
	      (* (- 1 factor) value)
	    )
	  )
    value
  )
)

(defn- estimate-total-concurrency
  [{:keys [thread-cpu-time-sum, start-time]}, now]
  (let [total-elapsed-time (- now start-time)] 
    (if (zero? total-elapsed-time)
      1.0
      (/ (double thread-cpu-time-sum) total-elapsed-time))))


(defn- update-concurrency-ema
  [progress-data, elapsed-time, estimation-period, now]
  (let [concurrency (estimate-total-concurrency progress-data, now)]
    (update-in progress-data [:concurrency-ema] update-ema concurrency, elapsed-time, estimation-period)))


(defn- update-progress-data
  [progress-data, task-result, now]
  (let [{:keys [start-time, end-time, cpu-time]} task-result,
        {:keys [completed-task-count, last-estimation-time, estimation-period]} progress-data,
        duration (- end-time start-time)
        efficiency (if (zero? duration) 1.0 (/ cpu-time duration)),
        elapsed-time (- now last-estimation-time)] 
	  (-> progress-data    
	    (update-in [:cpu-runtime-avg] update-avg cpu-time completed-task-count)
	    (update-in [:execution-efficiency-avg] update-avg efficiency completed-task-count)      
      (update-in [:thread-cpu-time-sum] + cpu-time)
      (update-in [:thread-real-time-sum] + duration)      
      (update-in [:cpu-runtime-ema] update-ema cpu-time, elapsed-time, estimation-period)
      ; thread time data has to be added in advance of the next calculation 
      (update-concurrency-ema elapsed-time, estimation-period, now)
	    (update-in [:completed-task-count] inc)
      (assoc :last-estimation-time now))))


(defn- duration-format
  [duration]
  (last
	  (reduce
	    (fn [[t, result], [div, space, delim]]
	      [(quot t div)
	       (format (str delim "%0" space "d%s") (mod t div) result)])
	    [duration ""]
	    [[1000 3 "."] [60 2 ":"] [60 2 ":"] [24 2 "d "] [365 3 ""]])))


(defn duration-estimation-string
  [remaining-runs, single-runtime, concurrency, total-spent-time]
  (let [concurrency (if (zero? concurrency) 1.0 concurrency),
        estimation (long (/ (* single-runtime remaining-runs) concurrency))]
    (format "%s   (=> %s)"
      (duration-format estimation)
      (duration-format (+ estimation total-spent-time)))))


(defn datetime-format
  [datetime]
  (format "%1$td.%1$tm.%1$tY %1$tH:%1$tM:%1$tS" datetime))


(defn- print-current-progress
  [now, {:keys [concurrency-ema, completed-task-count, total-task-count, start-time,
                output-format, cpu-runtime-avg, cpu-runtime-ema, execution-efficiency-avg,
                concurrency-ema]
         :as progress-data}]
  (let [concurrency (estimate-total-concurrency progress-data, now),        
        remaining-tasks (- total-task-count completed-task-count),
        total-spent-time (long (- now start-time)),
        last? (zero? remaining-tasks)]
    (println     
	    (format
        output-format
	      completed-task-count
	      total-task-count
        (if last?
          100.0
          (* 100 (/ (double completed-task-count) total-task-count)))
        (datetime-format now)))  
    (println 
      (format "|Experiment duration:\n|AVG ~ %.3fms\n|EMA ~ %.3fms"
        (double cpu-runtime-avg)
        (double cpu-runtime-ema)))
    (println "|")
    (println (format "|Execution efficiency:\n|AVG ~ %.3f" (double execution-efficiency-avg)))
    (println "|")
    (println (format "|Total spent time:\n|SUM ~ %s" (duration-format total-spent-time)))
    (println "|")
    (when (not last?)
      (println 
        (format "|Estimated remaining time:\n|AVG ~ %s\n|EMA ~ %s"
          (duration-estimation-string remaining-tasks, cpu-runtime-avg, concurrency, total-spent-time)
          (duration-estimation-string remaining-tasks, cpu-runtime-ema, concurrency-ema, total-spent-time)))
      (println "|"))
    (println (format "|Estimated concurrency:\n|AVG ~ %.2f\n|EMA ~ %.2f\n" concurrency concurrency-ema))
		(print \newline)))


(defn- update+print-progress
  [progress-data, results, throwables]
  (try
	  (when (seq throwables)
	    (println "Evil exceptions returned from task result event:")
	    (doseq [t throwables] 
	      (print-cause-trace t))
	    (flush))
  (let [now (System/currentTimeMillis), 
        exception-results (filter :exception results),
        results (remove :exception results),
        ; updata progress data for all task results
        progress-data (reduce #(update-progress-data %1, %2, now) progress-data results),
        {:keys [print-timeout last-print-time completed-task-count total-task-count]} progress-data,        
       ]
    (when (seq exception-results) 
      (println (format "%d exceptions in run method of the task:" (count exception-results)))
      (doseq [{:keys [exception]} exception-results]
        (print-cause-trace exception))
      (flush))
    (if (and (:print-progress progress-data)
             (or (= completed-task-count total-task-count) (<= print-timeout (- now last-print-time))))
      (do          
        (print-current-progress now, progress-data)
        ; return progress-data with now as last-print-time
        (assoc progress-data :last-print-time now)
      )
      ; return progress data
      progress-data
    )    
  )
  (catch Throwable t (print-cause-trace t) (throw t)))
)



(defn report
  [results, throwables]
  (send-off progress-agent update+print-progress results, throwables))