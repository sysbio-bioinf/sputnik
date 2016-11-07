; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.server.scheduling
  (:require
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [sputnik.satellite.messaging :as msg]
    [sputnik.satellite.server :as server]
    [sputnik.satellite.server.management :as mgmt]
    [sputnik.tools.performance-data :as pd]
    [sputnik.satellite.protocol :as protocol]))


(defn sort-vec
  ([v]
    (sort-vec compare v))
  ([^java.util.Comparator comp, v]
    (if (pos? (count v))
      (let [a (to-array v)]
        (java.util.Arrays/sort a, comp)
        (vec a))
      v)))


(defn sort-vec-by
  ([keyfn, v]
    (sort-vec-by keyfn compare v))
  ([keyfn ^java.util.Comparator comp v]
    (sort-vec
      (fn [x y] (.compare comp (keyfn x) (keyfn y)))
      v)))


(defn n-times-thread-count-tasks
  "Calculates the new tasks to assign as difference of the worker's thread count times n and the worker's
  already assigned tasks."
  [n, rated-worker-vec]
  (mapv
    (fn [{:keys [thread-count, assigned-task-count, worker-info] :as worker}]
      (if thread-count
        (let [max-task-count (long (* thread-count n)),
              new-task-count (max 0, (- max-task-count assigned-task-count))]
          (assoc worker
            :max-task-count max-task-count,
            :capacity new-task-count,
            :new-task-count new-task-count ))
        ; if no thread-count is known
        (do
          (log/warnf "Could not determine number of new tasks for worker %s because its thread count is unknown." worker-info)
          (assoc worker :max-task-count 0, :new-task-count 0))))
    rated-worker-vec))


(defn quarter-max-task-selection
  "Selects only workers that will get more tasks than a quarter of their maximum task number."
  [rated-worker-vec]
  (filterv
    (fn [{:keys [new-task-count, max-task-count]}]
      (>= new-task-count (quot max-task-count 4)))
    rated-worker-vec))


(defn half-thread-count-selection
  "Selects only workers that will get more tasks than half of their thread number."
  [rated-worker-vec]
  (filterv
    (fn [{:keys [new-task-count, thread-count]}]
      (>= new-task-count (quot thread-count 2)))
    rated-worker-vec))


(defn any-task-count-selection
  "Selects all workers that will get at least one task."
  [rated-worker-vec]
  (filterv
    (fn [{:keys [new-task-count]}]
      (pos? new-task-count))
    rated-worker-vec))


(defn new-task-ranking
  "Ranks the workers with the most new tasks to assign first."
  [rated-worker-vec]
  (sort-vec-by :new-task-count > rated-worker-vec))


(defn faster-worker?
  [{total-pd1 :total-performance-data, period-pd1 :maximal-period-performance-data, :as w1},
   {total-pd2 :total-performance-data, period-pd2 :maximal-period-performance-data, :as w2}]
  ; TODO: experiment use only total performance data and see whether tha solves the weird cases
  (let [w1-speed (when-let [pd1 total-pd1 #_(or period-pd1, total-pd1)] (pd/performance-attribute pd1, :mean-speed)),
        w2-speed (when-let [pd2 total-pd2 #_(or period-pd2, total-pd2)] (pd/performance-attribute pd2, :mean-speed))]
    (cond 
      (and w1-speed w2-speed) (> w1-speed w2-speed),
      w1-speed true,
      w2-speed false,
      :else (compare (:worker-info w1), (:worker-info w2)))))


(defn faster-worker-ranking
  "Ranks the faster workers first."
  [rated-worker-vec]
  (sort-vec faster-worker? rated-worker-vec))


; TODO: remove lazy filter with eager reduce-kv?
(defn- estimated-task-completion-at-worker
  "Returns an estimate how long the given task will need to run on the given worker.
  The estimation counts the tasks that were assigned to the worker before the given task (or at the same time)
  and calculated the estimated duration via the average computation speed of the worker."
  [mgr, {:keys [assigned-tasks, maximal-period-performance-data, total-performance-data] :as rated-worker-data}, assign-timestamp]
  (let [prev-task-count (->> assigned-tasks (filter #(-> % val (<= assign-timestamp))) count),        
        speed (when-let [pd (or maximal-period-performance-data total-performance-data)]
                (pd/performance-attribute pd :mean-speed))]    
    (cond
      ; there is no computation speed known (should only happen if the worker just started)
      ; hence if there are no previous tasks, assume this task is ready, else those are assumed to take forever.
      (nil? speed) (if (zero? prev-task-count) 0, Long/MAX_VALUE)
      (pos? speed) (/ (inc prev-task-count) speed)
      ; zero speed takes forever
      :else Long/MAX_VALUE)))


(defn estimated-completion-rating
  "Rates the task by its minimal estimated completion duration on the workers it is currently assigned to."
  [mgr, rated-worker-vec, worker-id-timestamp-map]
  ; determine minimum completion timestamp for the given workers
  (reduce
    (fn [result, {:keys [worker-id] :as rated-worker-data}]
      (if-let [task-assignment-timestamp (worker-id-timestamp-map worker-id)]
        (min
          result,
          (estimated-task-completion-at-worker mgr, rated-worker-data, task-assignment-timestamp))
        result))
    Long/MAX_VALUE
    rated-worker-vec))


(defn task-ratings->tasks
  "Selects at most `max-task-count` many tasks from task-rating-vec.
  Some tasks belonging to a given task key may be completed already then no task occurs in the result for that task key."
  [max-task-count, mgr, task-rating-vec]
  (let [n (count task-rating-vec),
        max-task-count (long max-task-count)]
    (loop [i 0, task-count 0, result-vec (transient [])]
      (if (and (< task-count max-task-count) (< i n))
        (let [{:keys [task-key]} (nth task-rating-vec i),
              ; multiple transactions: maybe the task gets done meanwhile
              task ((mgmt/unfinished-task-map mgr) task-key)]
          (if task
            (recur (unchecked-inc i), (unchecked-inc task-count), (conj! result-vec task))
            (recur (unchecked-inc i), task-count, result-vec)))
        ; return persistent result
        (persistent! result-vec)))))


(defn- ranked-tasks-for-worker
  "Ranks all uncompleted tasks by the given rating function"
  [max-task-count, rating-fn, mgr, rated-worker-vec, worker-id]
  (let [task-assignment-map (mgmt/task-assignment-map mgr)]
    (->> task-assignment-map 
	    (reduce-kv
	      (fn [result, task-key, worker-id-timestamp-map]
	        ; do not select a task that is already assigned to this worker
	        (if (contains? worker-id-timestamp-map worker-id)
	          result
            (conj! result {:task-key task-key, :rating [(count worker-id-timestamp-map) (rating-fn mgr, rated-worker-vec, worker-id-timestamp-map)]})))
	      (transient []))
	    persistent!
	    ; smallest assigned first, on ties biggest rating first (since bigger rating must mean completion at a later time)
	    (sort-vec-by :rating
        (fn [[assigned1, rating1], [assigned2, rating2]]
          (if (== assigned1 assigned2)
            (> rating1 rating2)
            (< assigned1 assigned2))))
	    (task-ratings->tasks max-task-count, mgr))))


(defn steal-estimated-longest-lasting-tasks
  [factor, mgr, rated-worker-vec, {:keys [assigned-task-count, thread-count, worker-id, worker-info] :as rated-worker}, new-task-count]
  (let [max-task-count (if (and factor thread-count)
                         (long (* factor thread-count))
                         0)]
    (when (pos? max-task-count)
      (let [steal-task-count (max 0, (- max-task-count assigned-task-count))]
        (when (pos? steal-task-count)
          (log/tracef "Task Scheduling: Trying to steal %d tasks for worker %s." steal-task-count worker-info)
          (ranked-tasks-for-worker steal-task-count, estimated-completion-rating, mgr, rated-worker-vec, worker-id))))))



(defn try-send+assign-tasks
  ([server-node, worker-node, tasks]
    (try-send+assign-tasks nil, server-node, worker-node, tasks))
  ([success-fn, server-node, worker-node, tasks]
	  (try
	    (let [now (System/currentTimeMillis)]
        (log/debugf "Task scheduling: Sending %s tasks to worker %s." (count tasks) (msg/address-str worker-node))
		    ; send message with tasks to the worker
		    (msg/send-message worker-node (protocol/task-distribution-message tasks))
		    ; successfull call the success function if given
		    (when success-fn
		      (success-fn))    
		    ; assign tasks to worker
		    (dosync
		      (doseq [t tasks]
		        (mgmt/assign-task (server/manager server-node), t, (server/get-worker-id server-node, (msg/id worker-node)), now))))
	    (catch Throwable t
	      (log/errorf "Task Scheduling: Sending tasks to worker %s failed!" (msg/address-str worker-node))
	      (throw t)))))


;  (cond 
;    ; compare speed in last rating period (current worker performance)
;    (and speed-period1 speed-period2) (> speed-period1 speed-period2),    
;    ; otherwise compare overall speed
;    (and speed1 speed2) (> speed1 speed2),
;    ; no speed of worker 2 known, worker 1 wins 
;    speed1 true,
;    ; no speed of worker 1 known, worker 2 wins
;    speed2 false,
;    ; no preference just compare worker info for an arbitrary but consistent winner
;    :else (compare (:worker-info w1), (:worker-info w2))))

(defn pop-from-queue
  [^long n, queue]
  (loop [i n, queue queue]
    (if (pos? i)
      (recur (unchecked-dec i), (pop queue))
      queue)))


(defn send-tasks
  [server-node, {:keys [worker-id, assigned-task-count] :as rated-worker}, new-task-count, task-queue-ref, task-stealing-fn]
  (if-let [worker-node (server/get-worker-endpoint server-node worker-id)]
    (let [queue-size (count @task-queue-ref)
          n (min new-task-count queue-size)]	     
      (log/tracef "Task Scheduling: Worker %s should get %d tasks (currently assigned: %d tasks). %d tasks can be take from the queue (total task count = %d).", 
        (msg/address-str worker-node), new-task-count, assigned-task-count, n, queue-size)
      (if (pos? n)              
	       (let [tasks (vec (take n @task-queue-ref))]
	         (try-send+assign-tasks
             ; success function: removes tasks from queue
             #(dosync (alter task-queue-ref (fn [task-queue] (pop-from-queue n, task-queue))))
             server-node, worker-node, tasks))
         (when task-stealing-fn
           (let [_ (log/tracef "Task Scheduling: No task in queue for worker %s. Trying to steal tasks now." (msg/address-str worker-node)),
                 tasks (task-stealing-fn rated-worker, new-task-count),
                 n (count tasks)]
             (log/tracef "Task Scheduling: Task Stealing selected %d tasks for worker %s." n (msg/address-str worker-node))
             (when (pos? n)
               (try-send+assign-tasks server-node, worker-node, tasks))))))
    (log/debugf "Task Scheduling: No worker with ID \"%s\" found!" worker-id)))


(defn fastest-max-task-scheduling
  [new-task-count-decision-fn, worker-task-selection-fn, worker-ranking-fn, task-stealing-fn, server-node]
  (try
    (let [scheduling-start-time (System/currentTimeMillis),
          mgr (server/manager server-node),
          rating-period-map (mgmt/rating-period-map mgr),
          max-period (reduce max (keys rating-period-map)),
          rated-worker-vec (mgmt/rated-workers mgr, max-period, true),
          selected-workers (-> rated-worker-vec
	                            new-task-count-decision-fn
	                            worker-task-selection-fn
	                            worker-ranking-fn),
          task-queue-ref (mgmt/unassigned-task-queue mgr),
          task-stealing-fn (partial task-stealing-fn mgr, rated-worker-vec)
          preparation-stop-time (System/currentTimeMillis)
          _ (log/tracef "Scheduling preparation took %s ms." (- preparation-stop-time scheduling-start-time))
          _ (doseq [{:keys [new-task-count, worker-id] :as rated-worker} selected-workers :when (pos? new-task-count)]
              (send-tasks server-node, rated-worker, new-task-count, task-queue-ref, task-stealing-fn)),
          scheduling-stop-time (System/currentTimeMillis)]
      (log/tracef "Complete scheduling took %s ms." (- scheduling-stop-time scheduling-start-time)))
    (catch Throwable t
      (log/errorf "Exception caught in the scheduling thread:\n%s" (with-out-str (print-cause-trace t)))))
  server-node)




(defn assign-task-count-equally
  [^long total-max-task-count, ^long total-capacity, ^long available-task-count, rated-worker-vec]
  (if (>= available-task-count total-capacity)
    (filterv #(pos? (:new-task-count %)) rated-worker-vec)
    (let [n (count rated-worker-vec),
          ; ratio of task that shall be assigned to each worker (approximately)
          ratio (/ (double available-task-count) total-max-task-count)]
      (loop [i 0, remaining-tasks available-task-count, result-vec (transient [])]
        (if (< i n)
          ; assign task count according to ratio and individual max-task-count
          (let [{:keys [max-task-count, assigned-task-count] :as worker} (nth rated-worker-vec i),
                new-task-count (if (< assigned-task-count max-task-count)
                                 ; there is capacity left, 
                                 (max 
                                    ; if no task is assigned, assign at least one task
                                    (if (zero? assigned-task-count) 1 0),
                                    ; ... or as many tasks needed to have (approx.) the same assignment ratio on all nodes (round down)
                                    (long (- (* ratio max-task-count) assigned-task-count)))
                                 ; otherwise no task is assigned
                                 0)]
            (recur
              (unchecked-inc i),
              (unchecked-subtract remaining-tasks new-task-count),
              (conj! result-vec (assoc worker :new-task-count new-task-count))))
          ; if there are tasks left due to rounding (at most (count result-vec) many remaining tasks)
          (if (and (pos? remaining-tasks) (pos? (count result-vec)))
            ; then assign remaining tasks
            (let [result-count (count result-vec)]
              (loop [i 0, remaining-tasks remaining-tasks, result-vec (persistent! result-vec)]
                (if (pos? remaining-tasks)
                  (let [{:keys [capacity, new-task-count]} (nth result-vec i),
                        i+1 (pd/inc-mod i, result-count)]
                    (if (< new-task-count capacity)
                      ; add one task
                      (recur
                        i+1,
                        (unchecked-dec remaining-tasks),
                        (update-in result-vec [i :new-task-count] inc))
                      (recur i+1, remaining-tasks, result-vec)))
                  result-vec)))            
            ; else just return workers with assignment
            (persistent! result-vec)))))))



(defn send-tasks-to-workers
  [server-node, task-queue-ref, rated-worker-vec]
  (when (seq rated-worker-vec)
    (log/tracef "Task Scheduling: %s", (str/join ", "
                                         (mapv #(str (:worker-info %) " " (:new-task-count %)) rated-worker-vec)))
    (doseq [{:keys [worker-id, worker-info, new-task-count] :as rated-worker} rated-worker-vec]
      (if-let [worker-node (server/get-worker-endpoint server-node worker-id)]
        (let [tasks (vec (take new-task-count @task-queue-ref))]
          (try-send+assign-tasks
            ; success function: removes tasks from queue
            #(dosync (alter task-queue-ref (fn [task-queue] (pop-from-queue new-task-count, task-queue))))
            server-node, worker-node, tasks))
        (log/warnf "Task Scheduling: Worker \"%s\" with ID \"%s\" is not connected anymore!", worker-info, worker-id)))))


(defn steal-tasks-for-workers
  [task-stealing-fn, server-node, mgr, rated-worker-vec]
  (doseq [{:keys [worker-id, worker-info, new-task-count] :as rated-worker} rated-worker-vec]    
    (if-let [worker-node (server/get-worker-endpoint server-node worker-id)]
      (let [tasks (task-stealing-fn mgr, rated-worker-vec, rated-worker, new-task-count)
            n (count tasks)]
        (log/tracef "Task Stealing: Stealing %s tasks for worker \"%s\"." n, worker-info)
        (when (pos? n)
          (try-send+assign-tasks server-node, worker-node, tasks)))
      (log/warnf "Task Stealing: Worker \"%s\" with ID \"%s\" is not connected anymore!", worker-info, worker-id))))



(defn equal-load-scheduling
  [new-task-count-decision-fn, task-stealing-fn, server-node]
  (try
    (let [scheduling-start-time (System/currentTimeMillis),
          mgr (server/manager server-node),
          ; determine number of available task
          task-queue-ref (mgmt/unassigned-task-queue mgr),
          available-task-count (count @task-queue-ref),
          rating-period-map (mgmt/rating-period-map mgr),
          max-period (reduce max (keys rating-period-map)),
          rated-worker-vec (new-task-count-decision-fn (mgmt/rated-workers mgr, max-period, true))]
      (if (pos? available-task-count)
        ; schedule remaining tasks
        (let [; rated workers with max-task-count, sorted by mean speed
              rated-worker-vec (faster-worker-ranking rated-worker-vec),
              ; determine sum of maximum task limits 
              total-max-task-count (reduce (fn [sum, worker] (+ sum (:max-task-count worker))) 0 rated-worker-vec),
              ; determine remaining capacity
              total-capacity (reduce (fn [sum, worker] (+ sum (:capacity worker))) 0 rated-worker-vec),
              ; calculate task count assignment and filter only those workers with new tasks
              rated-worker-vec (filterv
                                 #(pos? (:new-task-count %))
                                 (assign-task-count-equally total-max-task-count, total-capacity, available-task-count, rated-worker-vec)),
              preparation-stop-time (System/currentTimeMillis)
              _ (log/tracef "Scheduling preparation took %s ms." (- preparation-stop-time scheduling-start-time))
              _ (send-tasks-to-workers server-node, task-queue-ref, rated-worker-vec)
              scheduling-stop-time (System/currentTimeMillis)]
          (log/tracef "Complete scheduling took %s ms." (- scheduling-stop-time scheduling-start-time)))
        ; perform task stealing (if specified)
        (when task-stealing-fn
          (log/tracef "No tasks in queue - trying to steal tasks now.")
          (let [; steal from workers with fewer thread utilization first
                rated-workers-vec (sort-by 
                                    #(/ (double (:assigned-task-count %)) (:max-task-count %))
                                    (filterv
                                      #(pos? (:new-task-count %))
                                      (new-task-count-decision-fn (mgmt/rated-workers mgr, max-period, true))))]
            (if (seq rated-workers-vec)
              (let [_ (steal-tasks-for-workers task-stealing-fn, server-node, mgr, rated-worker-vec),
                    stealing-stop-time (System/currentTimeMillis)]
                (log/tracef "Complete stealing took %s ms." (- stealing-stop-time scheduling-start-time)))
              (log/tracef "No worker capacity for stealing tasks."))))))
    (catch Throwable t
      (log/errorf "Exception caught in the scheduling thread:\n%s" (with-out-str (print-cause-trace t))))))