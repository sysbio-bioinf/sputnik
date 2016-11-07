; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.server
  (:require
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.tools.logging :as log]
    [clojure.options :refer [defn+opts]]
    [sputnik.satellite.error :refer [send-error]]
    [sputnik.satellite.messaging :as msg]
    [sputnik.satellite.protocol :as protocol]
    [sputnik.satellite.role-based-messages :as role]
    [sputnik.satellite.server.management :as mgmt]
    [sputnik.tools.threads :as threads]
    [sputnik.tools.resolve :as r])
  (:import
    (java.util.concurrent LinkedBlockingQueue TimeUnit)
    java.util.UUID
    org.eclipse.jetty.server.Server))



(defn stop-jetty-server
  [^Server server]
  (.stop server))


(defprotocol IServerNode
  (set-jetty [this, j])
  (get-jetty [this])
  (scheduling-fn [this])
  (scheduling? [this])
  (stop-scheduling [this])
  (get-worker-id [this, endpoint-id])
  (get-worker-nickname [this, endpoint-id])
  (get-worker-endpoint [this, worker-id])
  (get-endpoint [this, endpoint-id])
  (worker-job-manager [this])
  (shutdown [this, now?]))

(defn- unwrap-promise
  [x]
  (if (instance? clojure.lang.IDeref x)
    @x
    x))

(deftype ServerNode [message-server-atom, thread-pool, role-map-atom, endpoint-id->worker-id-map, worker-id->endpoint-id-map, worker-job-manager, continue-scheduling?, scheduling-fn, ^{:volatile-mutable true} jetty]
  
  IServerNode
  
  (set-jetty [this, j]
    (set! jetty j))
  
  (get-jetty [this]
    jetty)
  
  (scheduling-fn [this]
    @scheduling-fn)
  
  (scheduling? [this]
    @continue-scheduling?)
  
  (stop-scheduling [this]
    (reset! continue-scheduling? false))
  
  (get-endpoint [this, endpoint-id]
    (when-let [message-server @message-server-atom]
      (msg/lookup-endpoint message-server, endpoint-id)))
  
  (get-worker-id [this, endpoint-id]
    (get @endpoint-id->worker-id-map endpoint-id))
  
  (get-worker-nickname [this, worker-id-or-endpoint-id]
    (when-let [worker-id (if (contains? @worker-id->endpoint-id-map worker-id-or-endpoint-id)
                           worker-id-or-endpoint-id
                           (get-worker-id this, worker-id-or-endpoint-id))]
      (mgmt/worker-nickname worker-job-manager, worker-id)))
  
  (get-worker-endpoint [this, worker-id]
    (when-let [message-server @message-server-atom]
      (if-let [endpoint-id (get @worker-id->endpoint-id-map worker-id)]
        (msg/lookup-endpoint message-server, endpoint-id)
        (do
          (log/errorf "There is no endpoint registered for worker \"%s\"!" worker-id)
          nil))))
  
  (worker-job-manager [this]
    worker-job-manager)
  
  (shutdown [this, now?]
    (threads/thread
      (threads/shutdown-thread-pool thread-pool, now?)
      (when-let [message-server (deref message-server-atom)]
        (msg/stop message-server))
      (stop-scheduling this)
      (when jetty
        (stop-jetty-server jetty)))
    nil)
  
  role/IRoleStorage
  
  (role! [this, remote-node, role]
    (role/set-role* role-map-atom, remote-node, role)
    this)
  
  (role [this, remote-node]
    (role/get-role* role-map-atom, remote-node))
  
  threads/IExecutor
 
  (execute [this, action]
    (threads/submit-action thread-pool, action)))
 

(defn start-scheduling
  [server-node, scheduling-timeout]
  (future
    (loop []
      (try
        (let [schedule (scheduling-fn server-node)]
          (schedule server-node)
          (Thread/sleep scheduling-timeout))
        (catch Throwable e
          (log/errorf "Exception in scheduling thread. Details:\n%s"
            (with-out-str (print-cause-trace e)))))
      (when (scheduling? server-node)
        (recur))))
  server-node)


(defn manager
  "Returns the worker-job-manager of the given server node."
  [server-node]
  (-> server-node worker-job-manager))


(defn set-web-server
  [server-node, ws]
  (-> server-node (set-jetty ws)))

(defn get-web-server
  [server-node]
  (-> server-node get-jetty))


(defn worker-finished-task-notification
  "Notifies all workers that have tasks assigned which are already finished."
  [server-node, finished-tasks]
  (let [worker-finished-task-map (mgmt/worker+finished-tasks (manager server-node),
                                   (mapv (comp protocol/create-task-key :task-data) finished-tasks))]
    (if (seq worker-finished-task-map)
      (log/debugf "Found %d tasks that are still assigned to workers (%d affected) but already completed."
        (count (apply concat (vals worker-finished-task-map))), (count (keys worker-finished-task-map)) )
      (log/debug "No tasks found that are still assigned to workers but already completed."))
    (doseq [[worker-id, finished-task-keys] worker-finished-task-map]
      (if-let [worker-node (get-worker-endpoint server-node, worker-id)]
        (do
          (log/debugf "Notifying worker %s of %d finished tasks.", (msg/address-str worker-node), (count finished-task-keys))
          (msg/send-message worker-node,
            (protocol/finished-task-notfication-message finished-task-keys)))
        (log/debugf "Worker %s is not connected anymore!" worker-id)))))


(defn tasks-finished
  "Informs the worker-job-manager about all finished tasks and returns a collection of those tasks
  that are completed for the first time, i.e. these task results are no duplicate for their tasks.
  Duplicates occur only when task stealing is used in scheduling."
  [server-node, worker-id, finished-tasks]
  (let [mgr (manager server-node)
        ; combine into a single transaction  (TODO: is this beneficial compared to multiple single transactions?)
        finished-tasks-no-duplicates (dosync      
                                       (doall
                                         (for [{:keys [task-data, execution-data] :as task} finished-tasks
                                               :let [no-duplicate? (mgmt/task-finished mgr (protocol/create-task-key task-data), worker-id, execution-data),
                                                     _ (when-not no-duplicate?
                                                         (log/debugf "Received duplicate for task \"%s\" of job \"%s\" from client \"%s\"." 
                                                           (:task-id task-data), (:job-id task-data), (:client-id task-data)))]
                                               :when no-duplicate?]
                                           task)))] 
    (worker-finished-task-notification server-node, finished-tasks-no-duplicates)
    finished-tasks-no-duplicates))


(defn select-message-handler 
  [this-node, remote-node, msg] 
  (type msg))


(defmulti handle-message "Handles the messages to the server according to their type." #'select-message-handler)


(defmethod handle-message :default
  [this-node, remote-node, msg]
  (log/errorf "Node %s: Unknown message type \"%s\"!\nMessage:\n%s", (msg/address-str remote-node), (type msg), (prn-str msg)))


(defmethod handle-message :error
  [this-node, remote-node, msg]
  (let [{:keys [kind reason message]} msg] 
    (log/errorf "Error from %s (type = %s reason = %s): %s" 
        (msg/address-str remote-node), kind, reason, message)))


(defn node-login
  [server-node, remote-node]
  ; request :server role
  (msg/send-message remote-node (role/role-request-message :server))
  (log/debugf "Node %s logged in.", (msg/address-str remote-node))
  nil)


(defn remove-worker-endpoint-mapping
  [^ServerNode server-node, endpoint-id]
  (let [eid->wid (.endpoint-id->worker-id-map server-node),
        wid->eid (.worker-id->endpoint-id-map server-node)]
    (dosync
      (let [worker-id (get (ensure eid->wid) endpoint-id)]
        (alter eid->wid dissoc endpoint-id)
        (alter wid->eid dissoc worker-id)
        worker-id))))


(defn node-logout
  [server-node, remote-node]
  (log/debugf "Node %s logged out." (msg/address-str remote-node))  
  (case (role/role server-node, remote-node)
    :worker
      (do
        ; in case a worker logs out, its assigned tasks need to be requeued
        (log/infof "Worker \"%s\" (%s) disconnected." (get-worker-nickname server-node (msg/id remote-node)), (msg/address-str remote-node))
        (log/infof "Requeueing current tasks of worker %s." (msg/address-str remote-node))
        (let [worker-id (remove-worker-endpoint-mapping server-node, (msg/id remote-node))]
          (mgmt/worker-disconnected (manager server-node) worker-id)))
    :client
      (do
        (log/infof "Client %s disconnected." (msg/address-str remote-node))
        (mgmt/client-disconnected (manager server-node) (msg/id remote-node)))
    nil)
  nil)


(defn handle-role-assigned
  [server-node, remote-node, role]
  (case role
    :client (mgmt/client-connected (manager server-node), (msg/id remote-node), (msg/address-str remote-node))
    nil))


(defn- resolve-function
  [info, fn-desc, default-value]
  (or    
    (when (some? fn-desc)
      (let [f (r/resolve-fn fn-desc, true)]
        (if (fn? f)
          f
          (log/errorf "%s: Identifier \"%s\" does not resolve to a function." info, fn-desc))))
    ; otherwise return default value
    (do
      (log/infof "%s: Using default value" info)
      default-value)))


(defn- check-number
  [info, x, default-value]
  (if (number? x)
    x
    (do
      (log/errorf "%s: Value \"%s\" is not a number! Using default: %s" info, x default-value)      
      default-value)))


(defmacro from-scheduling-ns
  "Avoid cyclic dependency with scheduling namespace by resolving functions at runtime."
  [symb]
  `(r/resolve-fn '~(->> symb name (symbol "sputnik.satellite.server.scheduling"))))


(defn+opts build-scheduling-fn
  "Creates a scheduling function from the given strategy parameters.
  Default values are used for undefined options.
  <max-task-count-factor>Specifies the factor that determines the number of maximum tasks of a worker (`max-task-count-factor` * thread-count).</>
  <worker-task-selection>Specifies the filter function that decides which workers get tasks in the current scheduling run.</>
  <worker-ranking>Specifies the function that ranks the workers - better ranked workers get new tasks first</>
  provided that the worker-task-selection decided to send them any tasks.</>
  <task-stealing>Specifies a function that selects already assigned tasks that are send to other workers. (No task stealing is an option as well.)</>
  <task-stealing-factor>Similar to the `max-task-count-factor` but applies only to task stealing.</>"
  [|{scheduling-strategy (choice :equal-load :fastest-max-tasks), max-task-count-factor nil, worker-task-selection nil, worker-ranking nil, task-stealing true, task-stealing-factor nil}]
  (let [worker-task-selection (resolve-function "worker-task-selection", worker-task-selection, (from-scheduling-ns any-task-count-selection)),
        worker-ranking        (resolve-function "worker-ranking",        worker-ranking,        (from-scheduling-ns faster-worker-ranking)),
        task-stealing-fn      (when task-stealing (from-scheduling-ns steal-estimated-longest-lasting-tasks)),
        max-task-count-factor (check-number "max-task-count-factor", max-task-count-factor, 2),
        task-stealing-factor  (check-number "task-stealing-factor", task-stealing-factor,  2)]
    
    (case scheduling-strategy
      :equal-load
        (partial (from-scheduling-ns equal-load-scheduling)
          (partial (from-scheduling-ns n-times-thread-count-tasks) max-task-count-factor)
          (when task-stealing-fn (partial task-stealing-fn task-stealing-factor)))
      :fastest-max-tasks
        (partial (from-scheduling-ns fastest-max-task-scheduling)
          (partial (from-scheduling-ns n-times-thread-count-tasks) max-task-count-factor),
          worker-task-selection,
          worker-ranking,
          (when task-stealing-fn (partial task-stealing-fn task-stealing-factor))))))


(defn+opts start-server
  "Starts a sputnik server with the given settings.
  <scheduling-timeout>Specifies the wait duration between scheduling runs in milliseconds.</>"
  [hostname, port | {scheduling-timeout 100} :as options]
  (log/debugf "Server %s:%s started with options: %s" hostname port (prn-str options))
  (println (format "Server %s:%s starts." hostname port))
  (let [init-scheduling-fn (build-scheduling-fn options),
        continue-scheduling?-atom (atom true),
        msg-server-atom (atom nil)
        server-node (ServerNode.
                      ; message server atom
                      msg-server-atom,
                      ; thread-pool
                      (threads/create-thread-pool options), 
                      ; role map
                      (atom {})
                      ; endpoint-id to worker-id map
                      (ref {})
                      ; worker-id to endpoint-id map
                      (ref {})
                      ; worker-job-manager
                      (mgmt/create-worker-job-manager options),
                      ; atom to shutdown the scheduling thread
                      continue-scheduling?-atom,
                      ; scheduling-fn
                      (atom init-scheduling-fn),
                      ; jetty
                      nil),
        msg-server (msg/create-server hostname, port,
                     (fn handle-messages-parallel [message-server, endpoint, message]
                       (threads/safe-execute server-node,
                         (role/handle-message-checked handle-message, server-node, endpoint, message, handle-role-assigned))),
                     :connect-handler
                     (fn node-connected [message-server, endpoint]
                       (threads/safe-execute server-node,
                         (node-login server-node, endpoint))),
                     :disconnect-handler
                     (fn node-disconnected [message-server, endpoint]
                       (threads/safe-execute server-node,
                         (node-logout server-node, endpoint))),
                     options)]
    (reset! msg-server-atom msg-server)
    (start-scheduling server-node, scheduling-timeout)
    (msg/start msg-server)
    server-node))


(defmethod handle-message :role-granted
  [this-node, remote-node, {:keys [role]}]
  (log/debugf "Node %s granted role %s." (msg/address-str remote-node) role))


(defn- unique-worker-id
  [^ServerNode server-node]
  (let [worker-id->endpoint-id-map (.worker-id->endpoint-id-map server-node)]
    (dosync
      (let [m (ensure worker-id->endpoint-id-map)]
        (loop []
          (let [uuid (str "W-" (UUID/randomUUID))]
            (if (contains? m uuid)
              (recur)
              uuid)))))))


(defn unique-worker-nickname
  [server-node, nickname]
  (let [mgr (manager server-node),
        used-nicknames (mgmt/worker-nicknames mgr)]
    (if (some #(= % nickname) used-nicknames)
      ; generate unique nickname by adding a number
      (let [pattern (re-pattern (str nickname " #(\\d+)"))
            max-nr (->> used-nicknames
                     (keep #(some->> % (re-find pattern) second Long/parseLong))
                     (reduce
                       max
                       0))]
        (format "%s #%02d" nickname (if (zero? max-nr) 2 (inc max-nr))))      
      nickname)))


(defmethod handle-message :worker-id-request
  [^ServerNode server-node, worker-node, {:keys [nickname, unique-id]}]
  (let [mgr (manager server-node)]
    (if (and unique-id (mgmt/worker-exists? mgr, unique-id) (not (mgmt/worker-connected? mgr, unique-id)))
      ; worker has been connected before
      (let [worker-id unique-id,
            endpoint-id (msg/id worker-node),
            nickname (mgmt/worker-nickname mgr, worker-id)]
        (dosync
          (alter (.endpoint-id->worker-id-map server-node) assoc endpoint-id worker-id)
          (alter (.worker-id->endpoint-id-map server-node) assoc worker-id endpoint-id))
        (mgmt/worker-reconnected mgr, worker-id)
        (msg/send-message worker-node (protocol/worker-id-response-message worker-id, nickname))
        (log/infof "Worker \"%s\" (%s, %s) reconnected." nickname, (msg/address-str worker-node), worker-id))
      ; worker connects for the first time
      (let [worker-id (unique-worker-id server-node),
            nickname (unique-worker-nickname server-node, nickname),
            endpoint-id (msg/id worker-node)]
        (dosync
          (alter (.endpoint-id->worker-id-map server-node) assoc endpoint-id worker-id)
          (alter (.worker-id->endpoint-id-map server-node) assoc worker-id endpoint-id))
        (mgmt/worker-connected mgr, worker-id, nickname)
        (msg/send-message worker-node (protocol/worker-id-response-message worker-id, nickname))
        (log/infof "Worker \"%s\" (%s, %s) connected." nickname, (msg/address-str worker-node), worker-id)))))


(defmethod handle-message :worker-thread-info
  [server-node, worker-node, msg]
  (let [{:keys [thread-count]} msg,
        worker-id (get-worker-id server-node, (msg/id worker-node))]
    (log/infof "Worker %s send :worker-thread-info with thread-count = %s." (msg/address-str worker-node) thread-count)
    (mgmt/worker-thread-count (manager server-node), worker-id, thread-count)))



(defmethod handle-message :job-submission
  [this-node, remote-node, msg]
  (log/infof "Received job %s with %d tasks from client %s." (-> msg :job :job-id) (-> msg :job :tasks count)  (msg/address-str remote-node))
  ; if the job has tasks, ...
  (if (-> msg :job :tasks seq)
    ; ... then register the job ...
    (let [client-id (msg/id remote-node)
          mgr (manager this-node)]
      (mgmt/register-job mgr client-id, (:job msg)))
    ; ... else send error to the remote node.
    (send-error this-node, remote-node, :invalid-job-data, :no-task-data, 
      (format "Job %s does not contain any tasks!" (-> msg :job :job-id)))))



(defmethod handle-message :tasks-completed
  [server-node, worker-node, {:keys [finished-tasks]}]
  (let [worker-id (get-worker-id server-node, (msg/id worker-node)),
        client-groups (group-by (comp :client-id :task-data) finished-tasks)] 
    (log/debugf "Received %s task results from worker %s." (count finished-tasks) (msg/address-str worker-node))
    ; for each client
    (doseq [[client-id finished-tasks] client-groups,
            :let [client-node (get-endpoint server-node client-id)]]      
      ; mark tasks as finished (updating manager state) and determine non-duplicate task results
      (let [non-duplicate-task-results (tasks-finished server-node, worker-id, finished-tasks)]
        (when (seq non-duplicate-task-results)
          (if client-node
	          ; send task results to the client
	          (msg/send-message client-node (protocol/tasks-completed-message non-duplicate-task-results))
	          ; error: client disconnected
	          (log/errorf "Tasks Completed: Client %s for completed tasks is not connected anymore!" client-id)))))))


(defn worker-thread-setup
  "Sends a setup message to the given worker node to adjust the thread count of that worker."
  [server-node, worker-id, thread-count]
  (if (pos? thread-count)
    (if-let [worker-node (get-worker-endpoint server-node worker-id)]
      (msg/send-message worker-node (protocol/worker-thread-setup-message thread-count))
      (log/debugf "Worker thread setup: No worker with id = %s found." worker-id))
    (log/debugf "Non-positive thread count %d given for worker with id = %s." thread-count worker-id)))


(defn request-worker-shutdown
  [server-node, worker-id]
  (if-let [worker-endpoint (get-worker-endpoint server-node, worker-id)]
    (do
      (log/infof "Request shutdown of worker %s." (get-worker-nickname  server-node, worker-id))
      (msg/send-message worker-endpoint, (protocol/worker-shutdown-message true)))
    (log/errorf "It is not possible to send a shutdown request to worker %s since there is no endpoint available." worker-id)))


(defn worker-shutdown
  [server-node, worker-id]
  (log/infof "Shutdown worker %s." worker-id)
  (request-worker-shutdown server-node, worker-id))


(defn shutdown-workers
  [server-node]
  (log/infof "Shutdown all workers.")
  (let [workers (mgmt/connected-workers (manager server-node))
        n (count workers)]
    (when (pos? n)
      (log/infof "Requesting shutdown of all %s workers." n)
      (doseq [{:keys [worker-id]} workers]
        (request-worker-shutdown server-node, worker-id)))))