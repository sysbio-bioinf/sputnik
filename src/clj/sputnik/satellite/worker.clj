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
    [clojure.java.io :as io]
    [clojure.java.shell :as sh]
    [clojure.tools.logging :as log]
    [clojure.options :refer [defn+opts]]
    [sputnik.satellite.messaging :as msg]
    [sputnik.satellite.protocol :as protocol]
    [sputnik.satellite.role-based-messages :as role]
    [sputnik.tools.resolve :as r]
    [sputnik.tools.threads :as threads]
    [sputnik.tools.control-flow :as cf])
  (:import
    (java.util.concurrent ThreadPoolExecutor LinkedBlockingQueue TimeUnit)
    (java.lang.management ManagementFactory ThreadMXBean)))



(defn select-message-handler 
  [this-node, remote-node, msg] 
  (type msg))



(defmulti handle-message "Handles the messages to the worker according to their type." #'select-message-handler)


(defmethod handle-message :default
  [this-node, remote-node, msg]
  (log/errorf "Node %s: Unknown message type \"%s\"!\nMessage:\n%s" (msg/address-str remote-node) (type msg) msg))


(defmethod handle-message :error
  [this-node, remote-node, msg]
  (let [{:keys [kind reason message]} msg] 
    (log/errorf "Error from %s (type = %s reason = %s): %s" (msg/address-str remote-node), kind, reason, message)))


(defprotocol IWorkerNode
  (set-result-response-future [this, f])
  (server-endpoint [this])
  (shutdown [this, now?]))

(deftype WorkerNode [message-client-atom, communication-thread-pool, role-map-atom, ^ThreadPoolExecutor worker-thread-pool, worker-thread-count, task-result-queue, ^{:volatile-mutable true} result-response-future, finished-task-notification-set, nickname, unique-id]
  
  IWorkerNode
  
  (set-result-response-future [this, f]
    (set! result-response-future f))
  
  (server-endpoint [this]
    (deref message-client-atom))
  
  (shutdown [this, now?]
    (threads/thread
      (when-let [message-client (deref message-client-atom)]
        (msg/shutdown message-client))
      (if now?
        (System/exit 0)
        (do
          (threads/shutdown-thread-pool worker-thread-pool, now?)
          (threads/shutdown-thread-pool communication-thread-pool, now?)
          (log/info "Worker shutdown complete."))))
    nil)
  
  role/IRoleStorage
  
  (role! [this, remote-node, role]
    (role/set-role* role-map-atom, remote-node, role)
    this)
  
  (role [this, remote-node]
    (role/get-role* role-map-atom, remote-node))
  
  threads/IExecutor
 
  (execute [this, action]
    (threads/submit-action communication-thread-pool, action)))



(defn get-worker-thread-count
  [^WorkerNode worker-node]
  (-> worker-node .worker-thread-count deref))


(defn task-already-finished?
  [^WorkerNode worker-node, task]
  (when worker-node
    (let [task-key (protocol/create-task-key task)
          finished-task-set (-> worker-node .finished-task-notification-set deref)]
      (finished-task-set task-key))))


(defn nickname
  [^WorkerNode worker-node]
  (.nickname worker-node))

(defn prev-unique-id
  [^WorkerNode worker-node]
  (.unique-id worker-node))


(defn mark-task-finished!
  [^WorkerNode worker-node, task]
  (let [task-key (protocol/create-task-key task)
        finished-task-set-atom (.finished-task-notification-set worker-node)]
    (swap! finished-task-set-atom disj task-key)
    nil))


(defn add-finished-tasks
  [^WorkerNode worker-node, finished-task-keys]
  (let [finished-task-set-atom (.finished-task-notification-set worker-node)]
    (swap! finished-task-set-atom into finished-task-keys)
    nil))


(defn set-worker-thread-count
  [^WorkerNode worker-node, thread-count]
  (doto ^ThreadPoolExecutor (.worker-thread-pool worker-node)
    (.setCorePoolSize thread-count)
    (.setMaximumPoolSize thread-count))
    (reset! (.worker-thread-count worker-node) thread-count))


(defn ^LinkedBlockingQueue get-task-result-queue
  [^WorkerNode worker-node]
  (.task-result-queue worker-node))


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
  [^WorkerNode worker-node, work-fn]
  (let [thread-pool (.worker-thread-pool worker-node)]
    (.submit ^ThreadPoolExecutor thread-pool ^Callable work-fn))
  nil) 

(defmacro submit-work
  [worker-node, & work-body]
 `(submit-work* ~worker-node (^{:once true} fn* [] ~@work-body)))


#_(defn connect-as-worker
   "Connects the worker node to the server and requests the :worker role."
   [worker-node, server-hostname, server-nodename, server-port]
   (let [server-node (node/connect-to worker-node, server-hostname, server-nodename, server-port)]
     ; request :worker role (when granted the worker-thread-info will be sent)
     (node/send-message server-node (role/role-request-message :worker))
     nil))


(defn prepare-task-results
  [task-results]
  (mapv
    (fn [{:keys [finished-task-map]}]
      (cond-> finished-task-map
        ; for batches the task-data is include in the individual tasks already (no duplicates!)
        (protocol/task-batch? (:task-data finished-task-map))        
        (update-in [:task-data] dissoc :tasks)))
    task-results))


(defn result-sending
  [worker-node, send-result-timeout, max-results-to-send, send-results-parallel]
  (log/debug "Result sending thread has started.")
  (loop []
    (try
      (let [task-results (pop-task-results worker-node, send-result-timeout, max-results-to-send),
            server-endpoint (server-endpoint worker-node)]
        (log/debugf "Result sending: Found %s task results for server %s.\nTask ids: %s"
          (count task-results), (msg/address-str server-endpoint), (str/join ", " (map (comp :task-id :task-data) task-results)))
          (try
            (cf/cond-wrap send-results-parallel, [threads/safe-execute worker-node], 
              (msg/send-message server-endpoint, (protocol/tasks-completed-message (prepare-task-results task-results))))
            (catch Throwable t
	            (log/errorf "Sending task results to server %s failed!" (msg/address-str server-endpoint))
	            ; TODO: recover somehow? store task-results on disk?
	            (throw t))))
      (catch Throwable t
        (log/errorf "Exception caught in the result sending thread:\n%s" (with-out-str (print-cause-trace t)))))
    ; continue with next worker
    (recur)))


(defn+opts start-result-response-thread
  [worker-node | {send-result-timeout 100, max-results-to-send nil, send-results-parallel true}]
  (let [f (future (result-sending worker-node, (or send-result-timeout 100), max-results-to-send, send-results-parallel))] 
    (doto worker-node
      (set-result-response-future f))))


(defn hostname
  "Try to get the hostname."
  []
  (or
    (System/getenv "HOSTNAME")
    (try
      (some->> (sh/sh "hostname") :out (re-find #"(.*)[\n]?") second)
      (catch Throwable t
        nil))
    (.getHostName (java.net.InetAddress/getLocalHost))))


(defn node-name
  [nickname, numa-id, cpu-id]
  (cond-> (or nickname (hostname))
    numa-id (str "-NUMA-NODE-" numa-id)
    cpu-id  (str "-NUMA-CPU-" cpu-id)))


(defonce ^:private ^:const worker-id-filename-fmt "%s-worker.id")

(defn save-worker-unique-id
  [init-nickname, unique-id]
  (spit (format worker-id-filename-fmt init-nickname) unique-id))

(defn load-worker-unique-id
  [init-nickname]
  (let [filename (format worker-id-filename-fmt init-nickname)]
    (when (.exists (io/file filename))
      (slurp filename))))


(defn+opts start-worker
  [server-hostname, server-port | {worker-thread-count nil, nickname nil, numa-id nil, cpu-id nil} :as options]
  (log/debugf "Worker started with options: %s", options)
  (log/debugf "Remote server is %s:%s", server-hostname, server-port)
  (println (format "Worker starts. Corresponding server is %s:%s.", server-hostname, server-port))
  (let [thread-count (or worker-thread-count (.availableProcessors (Runtime/getRuntime))),
        message-client-atom (atom nil),
        nickname (node-name nickname, numa-id, cpu-id)
        worker-node (WorkerNode. 
                      ; message client
                      message-client-atom                      
                      ; communication-thread-pool
                      (threads/create-thread-pool options)
                      ; role-map-atom
                      (atom {})
                      ; worker-thread-pool
                      (threads/create-thread-pool :thread-count thread-count), 
                      (atom thread-count),
                      (LinkedBlockingQueue.),
                      nil,
                      (atom #{}),
                      nickname,
                      (load-worker-unique-id nickname)),
        message-client (msg/create-client server-hostname, server-port,
                         (fn handle-messages-parallel [endpoint, message]
                           (threads/safe-execute worker-node, (role/handle-message-checked handle-message, worker-node, endpoint, message))),
                         ; try to reconnect worker on disconnect
                         :disconnect-handler (fn reconnect-worker [endpoint]
                                               (threads/safe-execute worker-node
                                                 (try
                                                   (log/warnf "Connection lost to server %s. Trying to reconnect." (msg/address-str endpoint))
                                                   ; try to reconnect
                                                   (msg/connect endpoint)
                                                   ; request :worker role (when granted the worker-thread-info will be sent)
                                                   (msg/send-message (server-endpoint worker-node), (role/role-request-message :worker))
                                                   (catch Throwable t
                                                     (log/errorf "Reconnecting worker to server %s failed due to:\n%s"
                                                       (msg/address-str endpoint), (with-out-str (print-cause-trace t))))))),
                         options)]
    (log/debugf "Option: worker-thread-count = %s - Starting worker with %s threads." worker-thread-count thread-count)
    (reset! message-client-atom message-client)
    (when (msg/connect message-client)
      (try
        ; request :worker role (when granted the worker-thread-info will be sent)
        (msg/send-message (server-endpoint worker-node), (role/role-request-message :worker))
        (start-result-response-thread worker-node, options)
        worker-node
        (catch Throwable t
          (log/errorf "Error during worker setup:\n%s" (with-out-str (print-cause-trace t)))
          nil)))))


(defmethod handle-message :role-granted
  [this-node, remote-node, {:keys [role]}]
  (log/debugf "Server %s granted role %s." (msg/address-str remote-node) role)
  ; TODO: unique-id is not stored in worker node for reconnect attempts (reconnect with same id works currently only in new processes)
  ; request worker-id for reconnection (on error)
  (msg/send-message remote-node (protocol/worker-id-request-message (nickname this-node), (prev-unique-id this-node)))
  ; :worker role was granted, now send worker-thread-info
  (msg/send-message remote-node (protocol/worker-thread-info-message (get-worker-thread-count this-node))))



(defmethod handle-message :worker-id-response
  [worker-node, remote-node, {:keys [unique-id, actual-nickname]}]
  (log/debugf "Server assigned unique id %s and uses the nickname %s for this worker." unique-id, actual-nickname)
  (save-worker-unique-id (nickname worker-node) unique-id))


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



(defn finished-task-map
  ([[task-data, execution-data]]
    (finished-task-map task-data, execution-data))
  ([task-data, execution-data]
    {:task-data task-data, :execution-data execution-data}))


(defn task-finished
  [this-node, remote-node, {:keys [task-data, execution-data] :as finished-task-map}]
  (try 
    (log/tracef "Task finished with:\n%s" (with-out-str (pprint execution-data)))
    (mark-task-finished! this-node, execution-data)
    ; enqueue finished task - remove input data since only the ids and the result data are needed
    (enqueue-finished-task this-node {:server-node remote-node, :finished-task-map (update-in finished-task-map [:task-data] dissoc :data)})
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
          (binding [*print-length* 50, *print-level* 3]
            (Exception. (format "Task execution failed for task %s with data:\n%s!"
                          function
                          (str/join "\n"
                            (map-indexed
                              (fn [i, param]
                                (format "Param %d:\n%s" (inc i) (with-out-str (pprint param))))
                              data))),
              t)))))))

(defn execution
  [this-node, execute-fn, finished-check?, task-description, {:keys [task-id] :as task}]
  ; wrap result in the map structure that is send back to server and client
  (finished-task-map
    (try
      (log/debugf "Execution of %s %s of job %s from client %s starts." task-description task-id (:job-id task) (:client-id task))
      (log/tracef "%s data:\n%s" task-description (with-out-str (pprint task)))
      (if (and finished-check? (task-already-finished? this-node, task)) 
        (do
          (log/debugf "%s %s of job %s from client %s has already been finished by another worker!" task-description task-id (:job-id task) (:client-id task))
          [task {:duplicate? true}])
        (let [start-time (System/currentTimeMillis),
              start-cpu-time (current-thread-cpu-time-millis),		        
              result (execute-fn this-node, task),
              end-cpu-time (current-thread-cpu-time-millis),
              end-time (System/currentTimeMillis)]
          (log/debugf "Execution of %s %s of job %s from client %s finished." task-description task-id (:job-id task) (:client-id task))
          [task
           (let [execution-data {:start-time start-time,
                                 :end-time end-time,
                                 :cpu-time (- end-cpu-time start-cpu-time),
                                 :thread-id (.getId (java.lang.Thread/currentThread))}]
             (if (instance? Exception result)
               (let [exception-msg (with-out-str (print-cause-trace result))]
                 (log/errorf "execution of %s %s failed with the following exception:\n%s" task-description task-id exception-msg)
                 (assoc execution-data :exception exception-msg :exception-scope :task))
               (assoc execution-data :result-data result)))]))     
      (catch Throwable t
        (let [msg (format "%s execution resulted in an exception:\n%s" task-description (with-out-str (print-cause-trace t)))]
          (log/debugf "Execution of %s %s of job %s from client %s failed with an exception." task-description task-id (:job-id task) (:client-id task))
          (log/error msg)
          [task {:exception msg :exception-scope :worker}])))))


(let [resolve+execute resolve+execute]
  (defn execute-task
    [this-node, {:keys [function, data] :as task}]
    (resolve+execute function, data)))


(defn failed-task?
  [{:keys [execution-data] :as finished-task-map}]
  (contains? execution-data :exception))


(let [execution execution,
      execute-task execute-task,
      failed-task? failed-task?]
  (defn execute-batch
    [this-node, {:keys [tasks] :as batch}]
    (let [finished-tasks-coll (->> tasks
                                (reduce
                                  (fn [results, task]
                                    (conj! results
                                      (execution this-node, execute-task, false, "Task", task)))
                                  (transient []))
                                persistent!),
          {failed-tasks true, completed-tasks false} (group-by failed-task? finished-tasks-coll)]
      {:failed-tasks failed-tasks, :completed-tasks completed-tasks})))


(let [execution execution,
      execute-task execute-task,
      execute-batch execute-batch]
  
  (defn start-execution
    [this-node, task]
    (if (protocol/task-batch? task)
      (execution this-node, execute-batch, true, "Batch", task)
      (execution this-node, execute-task, true, "Task", task)))
  
  (defn process-task
    [this-node, remote-node, {:keys [task-id] :as task}]
    (try     
      (log/debugf "Processing task %s of job %s from client %s starts." task-id (:job-id task) (:client-id task))
      (let [finished-task-map (start-execution this-node, task)]
        ; send result back to remote-node
        (task-finished this-node, remote-node, finished-task-map))
      (catch Throwable t
        (let [msg (with-out-str (println "Task processing resulted in an exception:") (print-cause-trace t))]
          (log/debugf "Processing task %s of job %s from client %s failed with an exception." task-id (:job-id task) (:client-id task))
          (log/error msg))))))


(defmethod handle-message :task-distribution
  [this-node, remote-node, {:keys [tasks]}]
  (log/debugf "Received %s tasks from server %s." (count tasks) (msg/address-str remote-node))
  (log/tracef "Received %s tasks from server %s:\n%s" (count tasks) (msg/address-str remote-node) (with-out-str (pprint tasks)))
  (doseq [t tasks]    
    (submit-work this-node (process-task this-node, remote-node, t))))


(defmethod handle-message :worker-thread-setup
  [this-node, remote-node, {:keys [thread-count]}]
  (log/infof "Server requests the usage of %s computation threads." thread-count)
  (set-worker-thread-count this-node, thread-count)
  (msg/send-message remote-node (protocol/worker-thread-info-message (get-worker-thread-count this-node))))


(defmethod handle-message :finished-task-notfication
  [this-node, remote-node, {:keys [finished-task-keys]}]
  (log/debugf "Received notification about %d tasks that are already finished from server %s. Finished tasks: %s"
    (count finished-task-keys) (msg/address-str remote-node) (str/join ", " finished-task-keys))
  (add-finished-tasks this-node, finished-task-keys))


(defmethod handle-message :worker-shutdown
  [worker-node, remote-node, {:keys [now?]}]
  (log/infof "Server requests this worker to shut down (now? = %s)." (boolean now?))
  (shutdown worker-node, now?))