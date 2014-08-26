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
    [sputnik.satellite.node :as node]
    [sputnik.satellite.protocol :as protocol]
    [sputnik.satellite.role-based-messages :as role]
    [sputnik.satellite.server.management :as mgmt])
  (:import
    (java.util.concurrent LinkedBlockingQueue TimeUnit)
    java.util.Collection))



(defprotocol IServerData
  (set-node-check-future [this, f])
  (set-jetty [this, j])
  (trigger-scheduling [this])
  (worker-job-manager [this]))

(defn- unwrap-promise
  [x]
  (if (instance? clojure.lang.IDeref x)
    @x
    x))

(deftype ServerData [worker-job-manager, scheduling-agent, scheduling-fn, ^{:volatile-mutable true} jetty]
  IServerData
  (set-jetty [this, j]
    (set! jetty j))
  (trigger-scheduling [this]
    ; unwrap promise the first time it is found as the agents value (for snychronisation, since scheduling needs the server node)
    (send scheduling-agent (comp @scheduling-fn unwrap-promise)))
  (worker-job-manager [this]
    worker-job-manager))
 

(defn create-server-data
  [init-scheduling-fn, server-node-promise]
  (ServerData.
    ; worker-job-manager
    (mgmt/create-worker-job-manager),
    ; scheduling-agent
    (agent server-node-promise),
    ; scheduling-fn
    (atom init-scheduling-fn),
    ; jetty
    nil))


(defn manager
  "Returns the worker-job-manager of the given server node."
  [server-node]
  (-> server-node node/user-data worker-job-manager))


(defn scheduling!
  "Triggers scheduling for the given server node."
  [server-node]
  (-> server-node node/user-data trigger-scheduling))


(defn set-web-server
  [server-node, ws]
  (-> server-node node/user-data (set-jetty ws)))


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
      (if-let [worker-node (node/get-remote-node server-node, worker-id)]
        (do
          (log/debugf "Notifying worker %s of %d finished tasks.", (node/node-info worker-node), (count finished-task-keys))
          (node/send-message worker-node,
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
  (log/errorf "Node %s: Unknown message type \"%s\"!\nMessage:\n%s", (node/node-info remote-node), (type msg), (prn-str msg)))


(defmethod handle-message :error
  [this-node, remote-node, msg]
  (let [{:keys [kind reason message]} msg] 
    (log/errorf "Error from %s (type = %s reason = %s): %s" 
        (node/node-info remote-node), kind, reason, message)))


(defn node-login
  [this-node, remote-node]
  ; request :server role
  (node/send-message remote-node (role/role-request-message :server))
  (log/debugf "Node %s logged in.", (node/node-info remote-node))
  nil)


(defn node-logout
  [server-node, remote-node, reason]
  (log/debugf "Node %s logged out (reason = %s)." (node/node-info remote-node) reason)  
  (case (node/get-data remote-node :role)
    :worker
      (do
        ; in case a worker logs out, its assigned tasks need to be requeued
        (log/infof "Requeueing current tasks of worker %s." (node/node-info remote-node))
        (mgmt/worker-disconnected (manager server-node) (node/node-id remote-node))
        (scheduling! server-node))
    :client
      (mgmt/client-disconnected (manager server-node) (node/node-id remote-node))
    nil)
  nil)


(defn handle-role-assigned
  [server-node, remote-node, role]
  (case role
    :worker (mgmt/worker-connected (manager server-node), (node/node-id remote-node), (node/short-node-info remote-node))
    :client (mgmt/client-connected (manager server-node), (node/node-id remote-node), (node/short-node-info remote-node))
    nil))


(defn+opts start-server
  [hostname, nodename, port, init-scheduling-fn | :as options]
  (log/debugf "Server %s@%s:%s started with options: %s" nodename hostname port (prn-str options))
  (println (format "Server %s@%s:%s starts." nodename hostname port))
  (let [; usage of a promise for server node so that the schedule agent is guaranteed to wait for a server node
        server-node-promise (promise),
        server-node (node/start-node hostname, nodename, port, #(role/handle-message-checked handle-message, %1, %2, %3, handle-role-assigned), 
                      :login-handler node-login, :logout-handler node-logout, :user-data (create-server-data init-scheduling-fn, server-node-promise), options)]
    (deliver server-node-promise server-node)
    server-node))


(defmethod handle-message :role-granted
  [this-node, remote-node, {:keys [role]}]
  (log/debugf "Node %s granted role %s." (node/node-info remote-node) role))


(defmethod handle-message :worker-thread-info
  [this-node, worker-node, msg]
  (let [{:keys [thread-count]} msg,
        worker-id (node/node-id worker-node)]
    (log/debugf "Worker %s send :worker-thread-info with thread-count = %s." (node/node-info worker-node) thread-count)
    (mgmt/worker-thread-count (manager this-node), worker-id, thread-count)    
    (scheduling! this-node)))



(defmethod handle-message :job-submission
  [this-node, remote-node, msg]
  (log/debugf "Received job %s with %d tasks from client %s." (-> msg :job :job-id) (-> msg :job :tasks count)  (node/node-info remote-node))
  ; if the job has tasks, ...
  (if (-> msg :job :tasks seq)
    ; ... then register the job and trigger scheduling ...
    (let [client-id (node/node-id remote-node)
          mgr (manager this-node)]
      (mgmt/register-job mgr client-id, (:job msg))
      (scheduling! this-node))
    ; ... else send error to the remote node.
    (send-error this-node, remote-node, :invalid-job-data, :no-task-data, 
	    (format "Job %s does not contain any tasks!" (-> msg :job :job-id)))))



(defmethod handle-message :tasks-completed
  [this-node, worker-node, {:keys [finished-tasks]}]
  (let [worker-id (node/node-id worker-node),
        client-groups (group-by (comp :client-id :task-data) finished-tasks)] 
    (log/debugf "Received %s task results from worker %s." (count finished-tasks) (node/node-info worker-node))
    ; for each client
    (doseq [[client-id finished-tasks] client-groups,
            :let [client-node (node/get-remote-node this-node client-id)]]      
      ; mark tasks as finished (updating manager state) and determine non-duplicate task results
      (let [non-duplicate-task-results (tasks-finished this-node, worker-id, finished-tasks)]
        (when (seq non-duplicate-task-results)
          (if client-node
	          ; send task results to the client
	          (node/send-message client-node (protocol/tasks-completed-message non-duplicate-task-results))
	          ; error: client disconnected
	          (log/errorf "Tasks Completed: Client %s for completed tasks is not connected anymore!" client-id)))))
    ; trigger scheduling
    (scheduling! this-node)))


(defn worker-thread-setup
  "Sends a setup message to the given worker node to adjust the thread count of that worker."
  [server-node, worker-id, thread-count]
  (if (pos? thread-count)
    (if-let [worker-node (node/get-remote-node server-node worker-id)]
      (node/send-message worker-node (protocol/worker-thread-setup-message thread-count))
      (log/debugf "Worker thread setup: No worker with id = %s found." worker-id))
    (log/debugf "Non-positive thread count %d given for worker with id = %s." thread-count worker-id)))