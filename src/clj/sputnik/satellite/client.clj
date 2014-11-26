; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.client
  (:import
    java.util.concurrent.CountDownLatch
    java.io.Closeable)
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.pprint :refer [pprint]]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.tools.logging :as log]
    [clojure.options :refer [defn+opts, ->option-map]]
    [sputnik.satellite.node :as node]
    [sputnik.satellite.protocol :as protocol]
    [sputnik.satellite.role-based-messages :as role]
    [sputnik.satellite.progress :as progress]
    [sputnik.config.api :as cfg]
    [sputnik.tools.file-system :as fs]
    [sputnik.tools.error :as e]))



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



; job-registry:
;  job: {:job-id int, :job-data data, :result-callback function}
(deftype ClientData [job-registry, connected-promise-atom, additional-data])


(defn create-client-data
  []
  (ClientData. (atom {}), (atom nil), (atom nil)))


(defn register-job
  [client-node, job-data, result-callback-fn]
  (if-let [job-id (:job-id job-data)]
    (let [job-reg (-> client-node ^ClientData node/user-data .job_registry)] 
      (if (contains? @job-reg job-id)
        (throw (IllegalArgumentException. (format "The :job-id %s is already used!" job-id)))
        (do
          (swap! job-reg assoc job-id {:job-data job-data, :result-callback result-callback-fn})
          nil)))
    (throw (IllegalArgumentException. "The given job has no :job-id!"))))


(defn remove-job
  [client-node, job-data]
  (let [job-reg (-> client-node ^ClientData node/user-data .job_registry)]
    (swap! job-reg dissoc (:job-id job-data))))


(defn get-job-result-callback
  [client-node, job-id]
  (some-> client-node ^ClientData node/user-data .job_registry deref (get-in [job-id :result-callback])))


(defn setup-connected-sync
  [client-node]
  (let [connected-promise (promise)] 
    (-> client-node ^ClientData node/user-data .connected_promise_atom (reset! connected-promise))
    connected-promise))

(defn notify-connected
  [client-node]
  (-> client-node ^ClientData node/user-data .connected_promise_atom deref (deliver true)))


; result-callback-fn: [client, finished-tasks] -> void
(defprotocol ISputnikClient
  (connect [this, hostname, nodename, port])
  (submit-job [this, job-data, result-callback-fn])
  (job-finished [this, job-data]))


(deftype SputnikClient [client-node, ^{:volatile-mutable true} server-node]
  ISputnikClient
  (connect [this, hostname, nodename, port]
    (log/debugf "Client tries to connect to server %s@%s:%s ..." nodename hostname port)
    (let [remote-node (node/connect-to client-node, hostname, nodename, port)]
      (log/debugf "Client connected to server %s@%s:%s" nodename hostname port)
      (set! server-node remote-node)        
      (let [sync-promise (setup-connected-sync client-node)]
        (node/send-message remote-node (role/role-request-message :client))  
        @sync-promise)))
  (submit-job [this, job-data, result-callback-fn]
    (register-job client-node, job-data, (partial result-callback-fn this))
    (log/tracef "Sending job data to server %s:\n%s" (node/node-info server-node) (with-out-str (pprint job-data)))
    (node/send-message server-node (protocol/job-submission-message job-data))
    nil)
  (job-finished [this, job-data]
    (remove-job client-node, job-data)
    nil)
  Closeable
  (close [this]
    (node/shutdown client-node, false)
    nil))




(defn+opts start-client
  "Starts a client providing it with its identifying information."
  [hostname, nodename, port | :as options]
  (log/debugf "Client %s@%s:%s started with options: %s" nodename hostname port options)
  (let [client-node (node/start-node hostname, nodename, port, (partial role/handle-message-checked handle-message)
                      :user-data (create-client-data), options)]
    (SputnikClient. client-node, nil)))


(defmethod handle-message :role-granted
  [this-node, remote-node, {:keys [role]}]
  (log/debugf "Server %s granted role %s." (node/node-info remote-node) role)
  ; notify the client that the server granted the role and is thus fully connected
  (notify-connected this-node))


(defmethod handle-message :tasks-completed
  [this-node, remote-node, {:keys [finished-tasks]}]
  (let [job-group (group-by (comp :job-id :task-data) finished-tasks)] 
    (doseq [[job-id finished-tasks] job-group] 
      (when-let [result-callback (get-job-result-callback this-node, job-id)]
        (result-callback finished-tasks)))))



(defn maybe-fix-job-id-or-task-ids
  [job]
  (let [tasks (:tasks job),
        task-id-freq (frequencies (mapv :task-id tasks))
        duplicate-map (when-let [duplicate-task-ids (seq (filter #(> (val %) 1) task-id-freq))]
                        (into {} duplicate-task-ids))
        missing-task-ids? (contains? task-id-freq nil)
        missing-job-id? (-> job :job-id nil?)
        job-id (when missing-job-id? (str (java.util.UUID/randomUUID)))]
    (when (or duplicate-map missing-task-ids?)
      (let [msg (format
                  "Some tasks of job \"%s\" have %s task ids. All task ids are replaced. Either fix the task ids or do not rely on the task ids for further processing.%s"
                  (:job-id job)
                  (cond 
                    (and duplicate-map missing-task-ids?) "no or duplicate"
                    duplicate-map "duplicate"
                    missing-task-ids? "no")
                  (if duplicate-map
                    (->> duplicate-map (sort-by key >) (take 3) (map (fn [[id n]] (str n "x " id))) (str/join ", ") (str "\n"))
                    ""))]
        (log/errorf msg)
        (println "\nERROR:\n" msg "\n")))
    (when missing-job-id?
      (let [msg (format "The submitted job has no job id. Assigning new job id \"%s\"." job-id)]
        (log/errorf msg)
        (println "\nERROR:\n" msg "\n")))
    (cond-> job
      (or duplicate-map missing-task-ids?)
        (assoc :tasks (mapv (fn [id, task] (assoc task :task-id id)) (range 1 (inc (count tasks))) tasks))
      missing-job-id?
        (assoc :job-id job-id))))


(defn create-batched-job
  [{:keys [job-id, tasks] :as job}, batch-size]
  (when-not (pos? batch-size)
    (e/illegal-argument "Batch size must be a positive integer! %s was specified." batch-size))
  (protocol/create-job
    job-id,
    (->> tasks
      (partition-all batch-size)
      (mapv #(protocol/create-task-batch (inc %1), %2) (range)))))



(defn unwrap-batched-tasks
  [finished-tasks]
  (persistent!
    (reduce
      (fn [results, batch-task]
        (let [{:keys [failed-tasks, completed-tasks]} (get-in batch-task [:execution-data, :result-data])]
          (reduce conj! (reduce conj! results completed-tasks) failed-tasks)))
      (transient [])
      finished-tasks)))


(defn extract-exceptions
  [batched?, finished-tasks]
  (remove nil?
    (mapcat
      (fn [task]
        (let [exception (get-in task [:execution-data, :exception])
              nested-exceptions (when batched?
                                  (seq
                                    ; extract exception from each failed task
                                    (keep :exception
                                      ; get failed tasks
                                      (get-in task [:execution-data, :result-data, :failed-tasks]))))]
          ; if task batch, then list the root exception first and the nested exceptions subsequently
          (cond->> nested-exceptions
            exception
            (list* exception))))
      finished-tasks)))


(defn progress-callback
  [batched?, report-progress, result-callback, count-down, client, finished-tasks]
  (when report-progress
    ; progress report based on batched tasks (if batched) but with all exceptions (-> extract from batch)
    (report-progress (map :execution-data finished-tasks), (extract-exceptions batched?, finished-tasks)))
  (when result-callback
    (try
      (result-callback client, (cond-> finished-tasks batched? unwrap-batched-tasks))
      (catch Throwable t
        (log/error (str "Exception caught when calling the result callback:\n" (with-out-str (print-cause-trace t)))))))
  (dotimes [_ (count finished-tasks)] 
    (count-down)))


(defn+opts execute-job
  "Submits the job to the client and waits until all tasks are completed.
  <result-callback>Specifies a callback function that is invoked with the `client` and the `finished-tasks` when a new list of task results is received.</>
  <job-finished-callback>Specifies a callback function that is invoked with the `client` when the all tasks of the job are finished.</>
  <check>Specifies whether the job id and the task ids are checked to ensure that Sputnik can manage the tasks.
  Provided you ensure unique task ids you can switch this off to save time.</>
  <batch-size>When specified, the tasks are grouped to batches of `batch-size` many tasks that are evaluated together on one worker.</>"
  [client, job | {result-callback nil, job-finished-callback nil, check true, batch-size nil} :as options]
  (let [job (cond-> job
              check maybe-fix-job-id-or-task-ids
              batch-size (create-batched-job batch-size)),
        task-count (count (:tasks job)),
        latch (CountDownLatch. task-count)]
	    (log/infof "Starting a job with %s tasks." task-count)    
	    (submit-job client, job, 
	      (partial progress-callback
          (boolean batch-size),
	        (progress/create-progress-report task-count, options), 
	        result-callback,
	        #(.countDown latch)))
	    (.await latch)
	    (when job-finished-callback
	      (job-finished-callback client))
      (job-finished client, job)))


(defn result-data
  "Extracts the result data from the given list of finshed tasks."
  [finished-tasks]
  (mapv #(get-in % [:execution-data :result-data]) finished-tasks))


(defn+opts compute-job
  "Submits the job to the client, waits until all tasks are completed and returns the results.
  <async>Specifies whether this function returns immediately (true) and returns a dereferencable value or blocks until the job was completed.</>"
  [client, job | {async false} :as options]
  (let [results-atom (atom []),
        job-execution (future
                        (try
                          (execute-job client, job,
                            :result-callback
                            (fn [_, finished-tasks]
                              (swap! results-atom into (result-data finished-tasks))),
                            options)
                          (catch Throwable e
                            (log/errorf "An exception occured during the execution of job \"%s\". Details:\n%s"
                              (:job-id job), (with-out-str (print-cause-trace e)))
                            (throw e)))),
        deref-future #'clojure.core/deref-future,
        result-ref (reify
                     clojure.lang.IDeref 
                     (deref [_]
                       (deref-future job-execution)
                       (deref results-atom))
                     clojure.lang.IBlockingDeref
                     (deref [_ timeout-ms timeout-val]
                       (let [val (deref-future job-execution timeout-ms ::timed-out)]
                         (if (= val ::timed-out)
                           timeout-val
                           (deref results-atom))))
                     clojure.lang.IPending
                     (isRealized [_]
                       (realized? job-execution)))]     
    (if async
      result-ref
      (deref result-ref))))


(defn+opts start-automatic-client
  "Start an automatic client that executes a job. This is intended for automatically running a client remotely.
  The job is retrieved by calling the `job-setup-fn`. The automatic client exits as soon as the job is completed."
  [hostname, nodename, port, server-hostname, server-nodename, server-port, job-setup-fn | :as options]
  (log/infof "Automatic client started as %s@%s:%s to be connected to server %s@%s:%s.",
    nodename, hostname, port,
    server-nodename, server-hostname, server-port)
  (with-open [^Closeable client (doto (start-client hostname, nodename, port, options) 
                                  (connect server-hostname, server-nodename, server-port))]
    (let [{:keys [job, result-callback, job-finished-callback]} (job-setup-fn)]
	    (execute-job client, job,
       :result-callback result-callback,
       :job-finished-callback job-finished-callback,
       options))))


(defn test-result-callback
  [client, finished-tasks]
  (println (format "Received task results!\n%s" (pr-str finished-tasks))))


(defn additional-data
  [^SputnikClient sputnik-client]
  (-> sputnik-client .client-node ^ClientData node/user-data .additional-data deref))

(defn reset-additional-data
  [^SputnikClient sputnik-client, value]
  (-> sputnik-client .client-node ^ClientData node/user-data .additional-data (reset! value)))

(defn update-additional-data
  [^SputnikClient sputnik-client, f & values]
  (-> sputnik-client .client-node ^ClientData node/user-data .additional-data (apply swap! f values)))



(defn read-client-config
  "Read client configuration from file."
  [config-url]
  (->>
    (with-open [w (some-> config-url fs/find-url io/reader java.io.PushbackReader.)]
      (read w))
    (cfg/node-options :client)))


(defn create-client-from-config
  "Creates a client using the configuration file given by the `config-url`."
  [config-url]
  (if (fs/exists? config-url)
    (let [{:keys [hostname, nodename, registry-port, server-hostname, server-nodename, server-registry-port] :as options} (read-client-config config-url)]
               (doto (start-client hostname, nodename, registry-port, (->option-map options))
                 (connect server-hostname, server-nodename, server-registry-port)))
    (throw (java.io.FileNotFoundException. (format "Configuration file %s does not exist!" config-url)))))