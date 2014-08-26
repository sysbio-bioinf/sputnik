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
    [clojure.tools.logging :as log]
    [sputnik.satellite.node :as node]
    [sputnik.satellite.server :as server]
    [sputnik.satellite.server.management :as mgmt]
    [sputnik.satellite.protocol :as protocol]))


(defn n-times-thread-count-tasks
  "Calculates the new tasks to assign as difference of the worker's double thread count and the worker's
  already assigned tasks."
  [n, rated-workers]
  (mapv
    (fn [{:keys [thread-count, assigned-task-count, worker-info] :as worker}]
      (if thread-count
        (let [max-task-count (long (* thread-count n)),
              ; if assigned-task-count = 0 then just send thread-count many tasks
              new-task-count (max 0, (- max-task-count assigned-task-count) #_(if (== 0 assigned-task-count) thread-count (- max-task-count assigned-task-count)))]
          #_(when (== 0 assigned-task-count)
             (log/debugf "Worker %s gets only %d new tasks because he does not have any tasks right now." worker-info, new-task-count))
          (log/debugf "Worker %s  should get %d new tasks (max tasks = %d)." worker-info, new-task-count, max-task-count)
          (assoc worker
            :max-task-count max-task-count,
            :new-task-count new-task-count ))
        ; if no thread-count is known
        (do
          (log/debugf "Could not determine number of new tasks for worker %s because its thread count is unknown." worker-info)
          (assoc worker :max-task-count 0, :new-task-count 0))))
    rated-workers))


(defn quarter-max-task-selection
  "Selects only workers that will get more tasks than a quarter of their maximum task number."
  [rated-workers]
  (filterv
    (fn [{:keys [new-task-count, max-task-count]}]
      (>= new-task-count (quot max-task-count 4)))
    rated-workers))


(defn half-thread-count-selection
  "Selects only workers that will get more tasks than half of their thread number."
  [rated-workers]
  (filterv
    (fn [{:keys [new-task-count, thread-count]}]
      (>= new-task-count (quot thread-count 2)))
    rated-workers))


(defn any-task-count-selection
  "Selects all workers that will get at least one task."
  [rated-workers]
  (filterv
    (fn [{:keys [new-task-count]}]
      (pos? new-task-count))
    rated-workers))


(defn new-task-ranking
  "Ranks the workers with the most new tasks to assign first."
  [rated-workers]
  (sort-by :new-task-count > rated-workers))


(defn faster-worker?
  [{speed1 :avg-computation-speed, speed-period1 :avg-computation-speed-period, :as w1},
   {speed2 :avg-computation-speed, speed-period2 :avg-computation-speed-period, :as w2}]
  
  (let [w1-speed (or speed-period1, speed1),
        w2-speed (or speed-period2, speed2)]
    (cond 
      (and w1-speed w2-speed) (> w1-speed w2-speed),
      w1-speed true,
      w2-speed false,
      :else (compare (:worker-info w1), (:worker-info w2)))))


(defn faster-worker-ranking
  "Ranks the faster workers first."
  [rated-workers]
  (sort faster-worker? rated-workers))


(defn- estimated-task-completion-at-worker
  "Returns an estimate how long the given task will need to run on the given worker.
  The estimation counts the tasks that were assigned to the worker before the given task (or at the same time)
  and calculated the estimated duration via the average computation speed of the worker."
  [mgr, {:keys [assigned-tasks, avg-computation-speed, avg-computation-speed-period] :as rated-worker-data}, assign-timestamp]
  (let [prev-task-count (->> assigned-tasks (filter #(-> % val (<= assign-timestamp))) count),        
        speed (or avg-computation-speed-period avg-computation-speed)]    
    (cond
      ; there is no computation speed known (should only happen if the worker just started)
      ; hence if there are no previous tasks, assume this task is ready, else those are assumed to take forever.
      (nil? speed) (if (zero? prev-task-count) 0, Long/MAX_VALUE)
      (pos? speed) (/ (inc prev-task-count) speed)
      ; zero speed takes forever
      :else Long/MAX_VALUE)))


(defn estimated-completion-rating
  "Rates the task by its minimal estimated completion duration on the workers it is currently assigned to."
  [mgr, rated-worker-map, worker-id-timestamp-map]
  ; determine minimum completion timestamp for the given workers
  (reduce min
    (for [[worker-id timestamp] worker-id-timestamp-map]
      (estimated-task-completion-at-worker mgr, (rated-worker-map worker-id), timestamp))))


(defn task-ratings->tasks
  [mgr, task-rating-coll]
  (keep 
    (fn [{:keys [task-key]}]
      ; multiple transactions maybe the task gets done meanwhile
      ((mgmt/unfinished-task-map mgr) task-key))
   task-rating-coll))


(defn- ranked-tasks-for-worker
  "Ranks all uncompleted tasks by the given rating function"
  [rating-fn, mgr, rated-worker-map, worker-id]
  (let [task-assignment-map (mgmt/task-assignment-map mgr)]
    (->> task-assignment-map 
	    (reduce-kv
	      (fn [result, task-key, worker-id-timestamp-map]
	        ; do not select a task that is already assigned to this worker
	        (if (contains? worker-id-timestamp-map worker-id)
	          result
            (conj! result {:task-key task-key, :rating [(count worker-id-timestamp-map) (rating-fn mgr, rated-worker-map, worker-id-timestamp-map)]})))
	      (transient []))
	    persistent!
	    ; smallest assigned first, on ties biggest rating first (since bigger rating must mean completion at a later time)
	    (sort-by :rating
        (fn [[assigned1, rating1], [assigned2, rating2]]
          (if (== assigned1 assigned2)
            (> rating1 rating2)
            (< assigned1 assigned2))))
	    (task-ratings->tasks mgr))))


(defn steal-estimated-longest-lasting-tasks
  [factor, mgr, rated-worker-map, {:keys [assigned-task-count, thread-count, worker-id, worker-info] :as rated-worker}, new-task-count]
  (let [n (- (* factor thread-count) assigned-task-count)]
    (when (pos? n)
      (log/debugf "Task Scheduling: Trying to steal %d tasks for worker %s." n worker-info)
      (doall (take n (ranked-tasks-for-worker estimated-completion-rating, mgr, rated-worker-map, worker-id))))))


(defn no-task-stealing
  [mgr, rated-worker-map, rated-worker, new-task-count]
  nil)


(defn try-send+assign-tasks
  ([server-node, worker-node, tasks]
    (try-send+assign-tasks nil, server-node, worker-node, tasks))
  ([success-fn, server-node, worker-node, tasks]
	  (try
	    (let [now (System/currentTimeMillis)]
        (log/debugf "Task scheduling: Sending %s tasks to worker %s." (count tasks) (node/node-info worker-node))
		    ; send message with tasks to the worker
		    (node/send-message worker-node (protocol/task-distribution-message tasks))
		    ; successfull call the success function if given
		    (when success-fn
		      (success-fn))    
		    ; assign tasks to worker
		    (dosync
		      (doseq [t tasks]
		        (mgmt/assign-task (server/manager server-node), t, (node/node-id worker-node), now))))
	    (catch Throwable t
	      (log/errorf "Task Scheduling: Sending tasks to worker %s failed!" (node/node-info worker-node))	                  
	      ; mark worker as unreachable (logout handler will be called)
	      (node/remote-node-unreachable server-node (node/node-id worker-node))
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


(defn send-tasks
  [server-node, {:keys [worker-id] :as rated-worker}, new-task-count, task-queue-ref, task-stealing-fn]
  (if-let [worker-node (node/get-remote-node server-node worker-id)]
    (let [n (min new-task-count (count (dosync @task-queue-ref)))]	     
      (log/debugf "Task Scheduling: Worker %s should get %d tasks. Queue contains %d tasks.", (node/node-info worker-node), new-task-count, n)
      (if (pos? n)              
	       (let [tasks (take n @task-queue-ref)]
	         (try-send+assign-tasks
             ; success function: removes tasks from queue
             #(dosync (alter task-queue-ref (fn [task-queue] (reduce (fn [queue, _] (pop queue)) task-queue (range n)))))
             server-node, worker-node, tasks))
         (let [_ (log/debugf "Task Scheduling: No task in queue for worker %s. Trying to steal tasks now." (node/node-info worker-node)),
               tasks (task-stealing-fn rated-worker, new-task-count),
               n (count tasks)]
           (log/debugf "Task Scheduling: Task Stealing selected %d tasks for worker %s." n (node/node-info worker-node))
           (if (pos? n)
             (try-send+assign-tasks server-node, worker-node, tasks)
             (log/debugf "Task Scheduling: No task sent to worker %s.", (node/node-info worker-node))))))
    (log/debugf "Task Scheduling: No worker with ID \"%s\" found!" worker-id)))


(defn scheduling
  [new-task-decision-fn, worker-selection-fn, worker-ranking-fn, task-stealing-fn, rating-period, server-node]
  (try
    (let [start-time (System/currentTimeMillis),
          mgr (server/manager server-node),
          rated-worker-coll (mgmt/rated-workers mgr rating-period, true),
          rated-worker-map (zipmap (map :worker-id rated-worker-coll), rated-worker-coll),
          selected-workers (-> rated-worker-coll
	                            new-task-decision-fn
	                            worker-selection-fn
	                            worker-ranking-fn),
          task-queue-ref (mgmt/unassigned-task-queue mgr),
          task-stealing-fn (partial task-stealing-fn mgr, rated-worker-map)
          stop-time (System/currentTimeMillis)]
      (log/debugf "Scheduling preparation took %s ms." (- stop-time start-time))
      (doseq [{:keys [new-task-count, worker-id] :as rated-worker} selected-workers :when (pos? new-task-count)]
        (send-tasks server-node, rated-worker, new-task-count, task-queue-ref, task-stealing-fn)))
    (catch Throwable t
      (log/errorf "Exception caught in the scheduling thread:\n%s" (with-out-str (print-cause-trace t)))))
  server-node)


(def default-scheduling
  (partial scheduling 
           (partial n-times-thread-count-tasks 2),
           any-task-count-selection, ;quarter-max-task-selection,
           faster-worker-ranking,
           (partial steal-estimated-longest-lasting-tasks 1),
           (* 30 60 1000)))


(def scheduling-without-stealing
  (partial scheduling 
           (partial n-times-thread-count-tasks 2),
           any-task-count-selection,
           faster-worker-ranking,
           no-task-stealing,
           (* 30 60 1000)))