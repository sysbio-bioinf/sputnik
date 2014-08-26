; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.worker
  (:require
    [clojure.pprint :refer [pprint]]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [clojure.options :refer [defn+opts]]
    [sputnik.satellite.node :as node]
    [sputnik.satellite.protocol :as protocol]
    [sputnik.satellite.role-based-messages :as role]
    [sputnik.satellite.resolve :as r]
    [sputnik.tools.control-flow :as cf])
  (:import
    (java.util.concurrent ThreadPoolExecutor LinkedBlockingQueue TimeUnit)
    (java.lang.management ManagementFactory ThreadMXBean)))



(defn select-message-handler 
  [this-node, remote-node, msg] 
  (type msg))



(defmulti handle-message "Handles the messages to the client according to their type." #'select-message-handler)


(defmethod handle-message :default
  [this-node, remote-node, msg]
  (log/errorf "Node %s: Unknown message type \"%s\"!\nMessage:\n%s" (node/node-info remote-node) (type msg) msg))


(defmethod handle-message :error
  [this-node, remote-node, msg]
  (let [{:keys [kind reason message]} msg] 
    (log/errorf "Error from %s (type = %s reason = %s): %s" (node/node-info remote-node), kind, reason, message)))


(defprotocol IWorkerData
  (set-result-response-future [this, f]))

(deftype WorkerData [^ThreadPoolExecutor worker-thread-pool, worker-thread-count, task-result-queue, ^{:volatile-mutable true} result-response-future, finished-task-notification-set]
  IWorkerData
  (set-result-response-future [this, f]
    (set! result-response-future f)))


(defn+opts create-worker-data
  [ | {worker-thread-count nil}]
  (let [thread-count (or worker-thread-count (.availableProcessors (Runtime/getRuntime)))]
    (log/debugf "Option: worker-thread-count = %s - Starting worker with %s threads." worker-thread-count thread-count)
    (WorkerData. 
      (node/create-thread-pool :thread-count thread-count), 
      (atom thread-count),
      (LinkedBlockingQueue.),
      nil,
      (atom #{}))))


(defn get-worker-thread-count
  [worker-node]
  (-> worker-node ^WorkerData node/user-data .worker_thread_count deref))


(defn task-already-finished?
  [worker-node, task]
  (let [task-key (protocol/create-task-key task)
        finished-task-set (-> worker-node ^WorkerData node/user-data .finished_task_notification_set deref)]
    (finished-task-set task-key)))


(defn mark-task-finished!
  [worker-node, task]
  (let [task-key (protocol/create-task-key task)
        finished-task-set-atom (-> worker-node ^WorkerData node/user-data .finished_task_notification_set)]
    (swap! finished-task-set-atom disj task-key)
    nil))


(defn add-finished-tasks
  [worker-node, finished-task-keys]
  (let [finished-task-set-atom (-> worker-node ^WorkerData node/user-data .finished_task_notification_set)]
    (swap! finished-task-set-atom into finished-task-keys)
    nil))


(defn set-worker-thread-count
  [worker-node, thread-count]
  (let [worker-data ^WorkerData (node/user-data worker-node)]
    (doto ^ThreadPoolExecutor (.worker_thread_pool worker-data)
      (.setCorePoolSize thread-count)
      (.setMaximumPoolSize thread-count))
    (reset! (.worker_thread_count worker-data) thread-count)))


(defn ^LinkedBlockingQueue get-task-result-queue
  [worker-node]
  (-> worker-node ^WorkerData node/user-data .task_result_queue))


(defn enqueue-finished-task
  [worker-node, task-data]
  (let [task-result-queue (get-task-result-queue worker-node)]
    (.put task-result-queue task-data)))


(defn pop-task-results
  "Blocks until at least one result is available then waits for the specified timeout and pops all available results from the queue."
  [worker-node, timeout, max-results-to-send]
  (let [max-results-to-send (if max-results-to-send max-results-to-send java.lang.Integer/MAX_VALUE)
        task-result-queue (get-task-result-queue worker-node),
        first-task-result (.take task-result-queue)]
    (Thread/sleep timeout)
    (loop [result (transient [first-task-result])]
      (if (>= (count result) max-results-to-send)
        (persistent! result)
        (if-let [task-result (.poll task-result-queue)]
          (recur (conj! result task-result))
          (persistent! result))))))


(defn submit-work*
  [worker-node, work-fn]
  (let [thread-pool (-> worker-node ^WorkerData node/user-data .worker_thread_pool)]
    (.submit ^ThreadPoolExecutor thread-pool ^Callable work-fn))
  nil) 

(defmacro submit-work
  [worker-node, & work-body]
 `(submit-work* ~worker-node (^{:once true} fn* [] ~@work-body)))


(defn connect-as-worker
  "Connects the worker node to the server and requests the :worker role."
  [worker-node, server-hostname, server-nodename, server-port]
  (let [server-node (node/connect-to worker-node, server-hostname, server-nodename, server-port)]
    ; request :worker role (when granted the worker-thread-info will be sent)
    (node/send-message server-node (role/role-request-message :worker))
    nil))


(defn result-sending
  [worker-node, send-result-timeout, max-results-to-send, send-results-parallel]
  (log/debug "Result sending thread has started.")
  (loop []
    (try
      (let [task-results (pop-task-results worker-node, send-result-timeout, max-results-to-send),
            ; group by server node (will support multiserver layouts)
            ; actually groups by object identity.
            remote-groups (group-by :server-node task-results)]
        (doseq [[server-node task-results] remote-groups]
          (log/debugf "Result sending: Found %s task results for server %s.\nTask ids: %s"
            (count task-results), (node/node-info server-node), (str/join ", " (map (comp :task-id :task-data) task-results)))
          (try
            (cf/cond-wrap send-results-parallel, [node/safe-execute worker-node], 
              (node/send-message server-node (protocol/tasks-completed-message (map #(dissoc % :server-node) task-results))))
            (catch Throwable t
	            (log/errorf "Sending task results to server %s failed!" (node/node-info server-node))
	            ; TODO: recover somehow? store task-results on disk?
	            (throw t)))))
      (catch Throwable t
        (log/errorf "Exception caught in the result sending thread:\n%s" (with-out-str (print-cause-trace t)))))
    ; continue with next worker
    (recur)))


(defn+opts start-result-response-thread
  [worker-node | {send-result-timeout 1000, max-results-to-send nil, send-results-parallel true}]
  (let [f (future (result-sending worker-node, send-result-timeout, max-results-to-send, send-results-parallel))] 
    (doto worker-node
      (-> node/user-data (set-result-response-future f)))))


(defn+opts start-worker
  [hostname, nodename, port, server-hostname, server-nodename, server-port | :as options]
  (log/debugf "Worker %s@%s:%s started with options: %s", nodename, hostname, port, options)
  (log/debugf "Remote server is %s@%s:%s", server-nodename, server-hostname, server-port)
  (println (format "Worker %s@%s:%s starts. Corresponding server is %s@%s:%s.", nodename, hostname, port, server-nodename, server-hostname, server-port))
  (let [worker-node (node/start-node hostname, nodename, port, (partial role/handle-message-checked handle-message)
                      :user-data (create-worker-data options), options)]
    (doto worker-node
      (connect-as-worker server-hostname, server-nodename, server-port)
      (start-result-response-thread options))))


(defmethod handle-message :role-granted
  [this-node, remote-node, {:keys [role]}]
  (log/debugf "Server %s granted role %s." (node/node-info remote-node) role)
  ; :worker role was granted, now send worker-thread-info
  (node/send-message remote-node (protocol/worker-thread-info-message (get-worker-thread-count this-node))))


(defmacro wrap-error-log
  "Wraps the given body in a try catch which logs potential exceptions and rethrows them.
  A string representation of the body is included in the log and the exception."
  [& body]
  (let [info (format "Exception during execution of %s !" (str/join " " body))]
   `(try
      ~@body
      (catch Throwable t#
        (error (with-out-str (println ~info) (print-cause-trace t#)))
        (throw (Exception. (str ~info " -> " (.getMessage t#)), t#))))))



(let [thread-bean ^ThreadMXBean (ManagementFactory/getThreadMXBean)]
  (defn current-thread-cpu-time
    "Returns the total CPU time for the current thread in nanoseconds. The returned value is of nanoseconds precison but not necessarily nanoseconds accuracy. If the implementation distinguishes between user mode time and system mode time, the returned CPU time is the amount of time that the current thread has executed in user mode or system mode." 
    []
    (.getCurrentThreadCpuTime thread-bean)))

(defn current-thread-cpu-time-millis
  "Returns the total CPU time for the current thread in milliseconds."
  []
  (/ (current-thread-cpu-time) 1000000.0))



(defn task-finished
  [this-node, remote-node, task-data, execution-data]
  (try 
    (log/tracef "Task finished with:\n%s" (with-out-str (pprint execution-data)))
    (mark-task-finished! this-node, execution-data)
    (enqueue-finished-task this-node {:server-node remote-node, :task-data task-data, :execution-data execution-data})
    (catch Throwable t
      (log/errorf "Exception when enqueueing task \"%s\" of job \"%s\" of client \"%s\".\n%s"
        (:task-id task-data), (:job-id task-data), (:client-id task-data), (with-out-str (print-cause-trace t)))))
  nil)


(defn- resolve+execute
  [function, data]
  (let [f (try (r/resolve-fn function)
            (catch Throwable t
              (Exception. (format "Could not resolve function %s!" function), t)))]
    (if (instance? Exception f)
      f
      (try
        (apply f data)
        (catch Throwable t
          (Exception. (format "Task execution failed for task %s with data:\n%s!" 
                        f 
                        (str/join "\n"
                          (map-indexed
                            (fn [i, param]
                              (format "Param %d:\n%s" (inc i) (with-out-str (pprint param))))
                            data))), 
                      t))))))

(defn execute-task
  [this-node, remote-node, {:keys [task-id, function, data] :as task}]
  (->>
    (try
      (log/tracef "Next task:\n%s" (with-out-str (pprint task)))
      (log/debugf "Execution of task %s of job %s from client %s starts." task-id (:job-id task) (:client-id task))
		  (if (task-already-finished? this-node, task) 
        (do
          (log/debugf "Task %s of job %s from client %s has already been finished by another worker!" task-id (:job-id task) (:client-id task))
          [task {:duplicate? true}])
        (let [start-time (System/currentTimeMillis),
			         start-cpu-time (current-thread-cpu-time-millis),		        
	             result (resolve+execute function, data),
		           end-cpu-time (current-thread-cpu-time-millis),
	             end-time (System/currentTimeMillis)]
          (log/debugf "Execution of task %s of job %s from client %s finished." task-id (:job-id task) (:client-id task))
	        [task
	         (let [execution-data {:start-time start-time,
                                 :end-time end-time,
                                 :cpu-time (- end-cpu-time start-cpu-time),
                                 :thread-id (.getId (java.lang.Thread/currentThread))}]
             (if (instance? Exception result)
               (let [exception-msg (with-out-str (print-cause-trace result))]
                 (log/errorf "execute-task failed with the following exception:\n%s" exception-msg)
                 (assoc execution-data :exception exception-msg :exception-scope :task))
               (assoc execution-data :result-data result)))]))     
	   (catch Throwable t
	     (let [msg (with-out-str (println "Task run resulted in an exception:") (print-cause-trace t))]
        (log/debugf "Execution of task %s of job %s from client %s finished with an exception." task-id (:job-id task) (:client-id task))
        (log/error msg)
        [task {:exception msg :exception-scope :worker}])))
    ; send result back to remote-node
    (apply task-finished this-node, remote-node)))


(defmethod handle-message :task-distribution
  [this-node, remote-node, {:keys [tasks]}]
  (log/debugf "Received %s tasks from server %s" (count tasks) (node/node-info remote-node))
  (log/tracef "Received %s tasks from server %s:\n%s" (count tasks) (node/node-info remote-node) (with-out-str (pprint tasks)))
  (doseq [t tasks]    
    (submit-work this-node (execute-task this-node, remote-node, t))))


(defmethod handle-message :worker-thread-setup
  [this-node, remote-node, {:keys [thread-count]}]
  (set-worker-thread-count this-node, thread-count)
  (node/send-message remote-node (protocol/worker-thread-info-message (get-worker-thread-count this-node))))


(defmethod handle-message :finished-task-notfication
  [this-node, remote-node, {:keys [finished-task-keys]}]
  (log/debugf "Received notification about %d tasks that are already finished from server %s. Finished tasks: %s"
    (count finished-task-keys) (node/node-info remote-node) (str/join ", " finished-task-keys))
  (add-finished-tasks this-node, finished-task-keys))