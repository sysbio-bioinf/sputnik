; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.server.management
  (:require
    [clojure.set :as set]
    [frost.quick-freeze :as qf]
    [sputnik.satellite.protocol :as protocol]))


(deftype TaskDuration [^long start, ^long end, ^long duration, ^double efficiency, ^long threadid])


(defn task-duration->map
  [^TaskDuration td]
  {:start (.start td), :end (.end td), :duration (.duration td), :efficiency (.efficiency td), :thread-id (.threadid td)})


(defprotocol IManagement
  (client-connected [this, client-id, client-info])
  (register-job [this, client-id, job-data])
  (client-disconnected [this, client-id])
  (worker-connected [this, worker-id, worker-info])
  (worker-thread-count [this, worker-id, thread-count])
  (worker-disconnected [this, worker-id])  
  (assign-task [this, task-key, worker-id, assignment-timestamp])
  (task-finished [this, task-key, worker-id, task-result])
  ; query operations
  (unassigned-task-queue [this])
  (task-assignment-map [this])
  (unfinished-task-map [this])
  (rated-workers [this, rating-period, connected-only?])
  (client-data-map [this])
  (worker+finished-tasks [this, finished-tasks])
  (remove-finished-job-data [this])
  (worker-task-durations [this]))



(defn- register-job*
  [client-job-data-map, unfinished-task-map, unassigned-task-queue, client-id, {:keys [job-id, tasks]}]
  (let [tasks (mapv #(assoc % :client-id client-id, :job-id job-id) tasks)
        unfinished-tasks (mapv (fn [t] [(protocol/create-task-key t) t]) tasks)]
    (dosync
      ; store job data per client
      (alter client-job-data-map
        assoc-in [client-id :jobs job-id] {:task-count (count tasks), :finished-task-count 0, :durations [], :exceptions [] :finished false})
      ; add tasks to the unfinished task map
      (alter unfinished-task-map into unfinished-tasks)
      ; add tasks to the unassigned task queue
      (alter unassigned-task-queue into tasks))))


(defn- remove-finished-jobs
  [data-map]
  (update-in data-map [:jobs]
    #(persistent!
       (reduce-kv
         (fn [res, job-id, job]
           (if (:finished job)
             res
             (assoc! res job-id job)))
         (transient {})
         %))))


(defn- remove-finished-job-data*
  [client-job-data-map]
  (dosync
    (alter client-job-data-map
      (fn [client-job-data-map]
        (persistent!
          (reduce-kv
            (fn [m, client-id, data-map]
              (if (:connected data-map)
                (assoc! m client-id (remove-finished-jobs data-map))
                m))            
            (transient {})
            client-job-data-map))))))


(defn- client-connected*
  [client-job-data-map, client-id, client-info]
  (dosync
    (alter client-job-data-map
      (fn [m]
        ; if the client was connected before, ...
        (if (contains? m client-id)
          ; ... then just mark it as connected again ...
          (-> m (assoc-in [client-id :connected] true) (assoc-in [client-id :disconnect-time] nil))
          ; ... else create a new map for it
          (assoc m client-id {:connected true, :client-info client-info, :jobs {}, :disconnect-time nil}))))))

(defn- client-disconnected*
  [client-job-data-map, client-id]
  (let [now (System/currentTimeMillis)]
    (dosync
      (alter client-job-data-map update-in [client-id] assoc :connected false, :disconnect-time now))))

(defn- worker-connected*
  [worker-task-data-map, worker-id, worker-info]
  (dosync
    (alter worker-task-data-map
	    (fn [m]
	      ; if the worker was connected before, ...
	      (if (contains? m worker-id)
	        ; ... then just mark it as connected again ...
	        (-> m (assoc-in [worker-id :connected] true) (assoc-in [worker-id :disconnect-time] nil))
	        ; ... else create a new map for it
	        (assoc m worker-id {:connected true, :worker-info worker-info, :assigned-tasks {}, :durations [], :disconnect-time nil}))))))
  
(defn- worker-thread-count*
  [worker-task-data-map, worker-id, thread-count]
  (dosync
    (alter worker-task-data-map assoc-in [worker-id :thread-count] thread-count)))

(defn- disj-nil
  "Removes the given element from the given collection but returns nil instead of the empty collection."
  [c e]
  (let [r (disj c e)]
    (when (seq r)
      r)))

(defn- dissoc-nil
  "Removes the given element from the given collection but returns nil instead of the empty collection."
  [c e]
  (let [r (dissoc c e)]
    (when (seq r)
      r)))


(defn- remove-task-assignment-of-worker
  "Removes the task assignment of the given task to the given worker from the task assignment map (if present)."
  [worker-id, task-assignment-map, task-key]
  ; remove assignment of the task to this worker: if there are assignments to other workers left, ...
  (if-let [worker-timestamp-map (dissoc-nil (task-assignment-map task-key) worker-id)]
    ; ... then put those assignments in the map ...
    (assoc task-assignment-map task-key worker-timestamp-map)
    ; ... else remove the task from the map
    (dissoc task-assignment-map task-key)))


(defn- worker-disconnected*
  [worker-task-data-map, unfinished-task-map, unassigned-task-queue, task-assignment-map, worker-id]
  (let [now (System/currentTimeMillis)]
    (dosync
	    (let [{:keys [assigned-tasks]} (get @worker-task-data-map worker-id)]
	      (when (seq assigned-tasks)
	        (let [assigned-task-keys (keys assigned-tasks),
                ; remove worker from the task assignment map
	              task-assignments (alter task-assignment-map
                                   #(reduce (partial remove-task-assignment-of-worker worker-id) % assigned-task-keys)),
	              ; select only tasks to requeue that are not assigned anymore and unfinished
	              requeue-tasks (->> assigned-task-keys
	                              (reduce
		                              (fn [result, task-key]
		                                (or
		                                  (when (nil? (get task-assignments task-key))
		                                    (when-let [task-data (get @unfinished-task-map task-key)]
		                                      (conj! result task-data)))
		                                  result))
		                              (transient []))
	                              persistent!)]
	          ; requeue tasks
	          (alter unassigned-task-queue into requeue-tasks)))
	      ; update worker state: no assigned task and not connected (but keep durations)
	      (alter worker-task-data-map update-in [worker-id] assoc :assigned-tasks {}, :connected false, :disconnect-time now)))))


(defn- assign-task*
  [worker-task-data-map, task-assignment-map, task-data, worker-id, assignment-timestamp]
  (let [task-key (protocol/create-task-key task-data)]
    (dosync
      (alter worker-task-data-map update-in [worker-id :assigned-tasks] assoc task-key assignment-timestamp)
      (alter task-assignment-map update-in [task-key] (fnil assoc {}) worker-id assignment-timestamp))))

; TODO: use cond->
(defn- update-job
  [job-data, task-id, task-duration, exception, exception-scope, duplicate?]
  (let [job-data (if duplicate?
                   job-data
                   (update-in job-data [:finished-task-count] inc)),
        job-data (if (or duplicate? (nil? task-duration))
                   job-data
                   (update-in job-data [:durations] conj task-duration)),
        job-data (if exception
                   (update-in job-data [:exceptions] conj [task-id exception, exception-scope])
                   job-data)]
    (if (== (:task-count job-data) (:finished-task-count job-data))
      (assoc job-data :finished true)
      job-data)))


(defn- determine-task-duration
  [{:keys [start-time, end-time, cpu-time, exception, exception-scope, duplicate?, thread-id] :as task-result}]
  (when (and start-time end-time (or (nil? exception) (= exception-scope :task)))
    (let [duration (max (- end-time start-time) 1)]
      (TaskDuration. (long start-time), (long end-time), (long duration), (/ (double cpu-time) duration), thread-id))))


(defn- task-finished*
  [client-job-data-map, unfinished-task-map, worker-task-data-map, task-assignment-map,
   {:keys [client-id, job-id, task-id] :as task-key}, worker-id, {:keys [exception, exception-scope, duplicate?] :as task-result}]
  (let [#_task-key #_(protocol/create-task-key task-data),
        task-duration (determine-task-duration task-result)]
    (dosync
	    (let [duplicate? (or (nil? (get @unfinished-task-map task-key)) duplicate?)]
        ; mark task as finished
        (alter unfinished-task-map dissoc task-key)
        ; update job data
        (alter client-job-data-map update-in [client-id, :jobs, job-id] update-job task-id, task-duration, exception, exception-scope, duplicate?)
        ; remove assignment from worker data
        (alter worker-task-data-map update-in [worker-id] #(let [worker-data (update-in % [:assigned-tasks] dissoc task-key)] 
                                                             (if (nil? task-duration)
                                                               worker-data
                                                               (update-in worker-data [:durations] conj task-duration))))        
        ; remove assignment from task-data
        (alter task-assignment-map #(remove-task-assignment-of-worker worker-id, %, task-key))
        ; return whether the task was NOT a duplicate
        (not duplicate?)))))


(defn duration-in-interval
  [^TaskDuration td, interval-start, interval-end]
  (if (<= interval-start (.start td))
    (if (<= (.end td) interval-end)
      (.duration td)
      (when (<= (.start td) interval-end)
        (- interval-end (.start td))))
    (when (<= interval-start (.end td) interval-end)
      (- (.end td) interval-start))))

(defn computation-statistics
  ([durations]
    (computation-statistics durations, 0, Long/MAX_VALUE))
  ([durations, interval-start, interval-end]
    (when (seq durations)
	    (let [n (count durations)]
	      (loop [i 0, tasks-in-interval 0, duration-sum 0.0, task-completion 0.0, min-start Long/MAX_VALUE, max-end 0, efficiency-sum 0.0, cputime-sum 0.0]
          (if (< i n)            
            (let [^TaskDuration td (durations i),
                  d (duration-in-interval td, interval-start, interval-end)]
              (if d
                (recur 
                  (inc i),
                  (inc tasks-in-interval),
                  (+ duration-sum d),
                  (+ task-completion (/ (double d) (.duration td))),
                  (min min-start (.start td)), (max max-end (.end td))
                  (+ efficiency-sum (.efficiency td)),
                  (+ cputime-sum (* (.efficiency td) d))) 
                (recur (inc i), tasks-in-interval, duration-sum, task-completion, min-start, max-end, efficiency-sum, cputime-sum)))
            (let [b (max min-start interval-start),
                  e (min max-end interval-end)]
              (when (< b e)
                {:interval-begin b,
                 :interval-end e,
                 :avg-concurrency (/ duration-sum (- e b)),
                 :avg-duration (/ duration-sum task-completion),
                 :avg-computation-speed (/ task-completion (- e b)),
                 :task-completion task-completion,
                 :avg-efficiency (/ efficiency-sum tasks-in-interval),
                 :tasks-in-interval tasks-in-interval,
                 :cputime-sum cputime-sum,
                 :avg-parallelism (/ cputime-sum (- e b))}))))))))

(defn- rated-workers*
  [worker-task-data-map, rating-period, connected-only?]
  (dosync
    (let [period-end (System/currentTimeMillis)
          period-begin (- period-end rating-period)]
	    (with-meta
        (->> worker-task-data-map
          deref
			    (reduce-kv
				    (fn [result, worker-id, {:keys [assigned-tasks, durations, connected, disconnect-time, thread-count, worker-info]}]
				      (if (or (not connected-only?) connected)
				        (let [{:keys [avg-concurrency, avg-duration, task-completion, avg-computation-speed, avg-efficiency, tasks-in-interval, 
                              cputime-sum, avg-parallelism, interval-begin, interval-end]}
                        (computation-statistics durations),
                      {avg-concurrency-period :avg-concurrency, avg-duration-period :avg-duration,
                       task-completion-period :task-completion, avg-computation-speed-period :avg-computation-speed,
                       avg-efficiency-period  :avg-efficiency,  tasks-in-interval-period :tasks-in-interval,
                       cputime-sum-period     :cputime-sum,     avg-parallelism-period :avg-parallelism}
                        (computation-statistics durations, period-begin, period-end)]
                  (conj! result
			              {:worker-id worker-id,
	                   :worker-info worker-info,
		                 :thread-count thread-count,
			               :assigned-task-count (count assigned-tasks),
                     :assigned-tasks assigned-tasks,
		                 :avg-concurrency 
                      avg-concurrency,
                     :avg-duration
                      avg-duration,
                     :task-completion
                      task-completion,
                     :avg-efficiency
                      avg-efficiency,
                     :tasks-in-interval
                      tasks-in-interval,
                     :cputime-sum
                      cputime-sum,
                     :avg-parallelism
                      avg-parallelism,
                     :avg-computation-speed
                      avg-computation-speed,
		                 :avg-concurrency-period
                      avg-concurrency-period,
                     :avg-duration-period
                      avg-duration-period,
                     :task-completion-period
                      task-completion-period,
                     :avg-efficiency-period
                      avg-efficiency-period,
                     :tasks-in-interval-period
                      tasks-in-interval-period,
                     :cputime-sum-period
                      cputime-sum-period,
                     :avg-parallelism-period
                      avg-parallelism-period,
	                   :avg-computation-speed-period
                      avg-computation-speed-period,
		                 :connected connected,
	                   :disconnect-time disconnect-time}))
				        result))
				    (transient []))
			    persistent!)
        {:rating-period rating-period}))))


(defn- worker+finished-tasks*
  "Returns a map that contains the finished tasks that are still assigned to workers."
  [finished-task-keys, worker-task-data-map]
  (let [worker-task-data-map (dosync @worker-task-data-map),
        finished-task-id-set (set finished-task-keys)]
    (persistent!
      (reduce-kv
        (fn [res, worker-id, {:keys [assigned-tasks]}]
          (let [finished-tasks (filterv finished-task-id-set (keys assigned-tasks))]
            (if (seq finished-tasks)
              (assoc! res worker-id finished-tasks)
              res)))
        (transient {})
        worker-task-data-map))))


(defn- worker-task-durations*
  [worker-task-data-map]
  (let [worker-task-data-map (dosync @worker-task-data-map)]
    (->> worker-task-data-map
      (reduce-kv
        (fn [m, worker-id, {:keys [durations]}]
          (assoc! m worker-id (mapv task-duration->map durations)))
        (transient {}))
      persistent!)))


; data layout:
;  * client-job-data-map
;     - type: (ref {})
;     - key-levels: client-id
;     - values: :connected (boolean), :jobs (map, see below)
;     - key-levels: client-id, :jobs, job-id
;     - values: :task-count (integer), :finished-task-count (integer), :durations (vector), :finished (boolean), :exceptions (vector of vectors: [task-id exception])
;
;  * unfinished-task-map
;     - type: (ref {})
;     - keys: task-key (= {:client-id #, :job-id #, :task-id #})
;     - values: task-data
;
;  * unassigned-task-queue
;     - type: (ref PersistentQueue/EMPTY)
;     - content: task-data
;
;  * worker-task-data-map
;     - type: (ref {})
;     - keys: worker-id
;     - values: :assigned-tasks (map of task-key -> timestamp of assignment), :durations (vector), :connected (boolean)
;
;  * task-assignment-map
;     - type: (ref {})
;     - keys: task-key (= {:client-id #, :job-id #, :task-id #})
;     - values: assigned workers (map of worker-id -> timestamp of assignment)
(deftype WorkerJobManager [client-job-data-map, unfinished-task-map, unassigned-task-queue, worker-task-data-map, task-assignment-map]  
  IManagement
  
  (client-connected [this, client-id, client-info]
    (client-connected* client-job-data-map, client-id, client-info))

  (register-job [this, client-id, job-data]
    (register-job* client-job-data-map, unfinished-task-map, unassigned-task-queue, client-id, job-data)
    this)  

  (client-disconnected [this, client-id]
    (client-disconnected* client-job-data-map, client-id))
  
  (worker-connected [this, worker-id, worker-info]
    (worker-connected* worker-task-data-map, worker-id, worker-info)
    this)
  
  (worker-thread-count [this, worker-id, thread-count]
    (worker-thread-count* worker-task-data-map, worker-id, thread-count)
    this)
  
  (worker-disconnected [this, worker-id]
    (worker-disconnected* worker-task-data-map, unfinished-task-map, unassigned-task-queue, task-assignment-map, worker-id)
    this)
  
  (assign-task [this, task-key, worker-id, assignment-timestamp]
    (assign-task* worker-task-data-map, task-assignment-map, task-key, worker-id, assignment-timestamp)
    this)
  
  (task-finished [this, task-key, worker-id, task-result]
    ; returns whether the task is not a duplicate (i.e. a result to this task was not already received from another worker. occurs when a task-stealing strategy is used.)
    (task-finished* client-job-data-map, unfinished-task-map, worker-task-data-map, task-assignment-map, task-key, worker-id, task-result))
  
  (unassigned-task-queue [this]
    ; return the ref since it is manipulated from the outside
    unassigned-task-queue)
  
  (task-assignment-map [this]
    (dosync @task-assignment-map))
  
  (unfinished-task-map [this]
    (dosync @unfinished-task-map))
  
  (rated-workers [this, rating-period, connected-only?]
    (rated-workers* worker-task-data-map, rating-period, connected-only?))
  
  (client-data-map [this]
    (dosync @client-job-data-map))
  
  (worker+finished-tasks [this, finished-tasks]
    (worker+finished-tasks* finished-tasks, worker-task-data-map))

  (remove-finished-job-data [this]
    (remove-finished-job-data* client-job-data-map))
  
  (worker-task-durations [this]
    (worker-task-durations* worker-task-data-map)))


;(defn- task-assignment-map-validator
;  [new-val]
;  (if (and
;        (map? new-val)
;        (every?
;          (fn [[task-key worker-timestamp-map]]
;            (and (map? task-key)))
;    
;          new-val))))

(defn create-worker-job-manager
  []
  (WorkerJobManager. (ref {} :min-history 15 :max-history 50), (ref {}), (ref clojure.lang.PersistentQueue/EMPTY), (ref {}), (ref {})))


(defn export-worker-task-durations
  [filename, manager]
  (qf/quick-file-freeze filename, (worker-task-durations manager)))