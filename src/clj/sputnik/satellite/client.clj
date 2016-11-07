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
    [sputnik.satellite.messaging :as msg]
    [sputnik.satellite.protocol :as protocol]
    [sputnik.satellite.role-based-messages :as role]
    [sputnik.tools.threads :as threads]
    [sputnik.tools.progress :as progress]
    [sputnik.config.api :as cfg]
    [sputnik.tools.file-system :as fs]
    [sputnik.tools.error :as e]))



(defn select-message-handler 
  [this-node, remote-node, msg] 
  (type msg))



(defmulti handle-message "Handles the messages to the client according to their type." #'select-message-handler)


(defmethod handle-message :default
  [this-node, remote-node, msg]
  (log/errorf "Node %s: Unknown message type \"%s\"!\nMessage:\n%s" (msg/address-str remote-node) (type msg) msg))


(defmethod handle-message :error
  [this-node, remote-node, msg]
  (let [{:keys [kind reason message]} msg] 
    (log/errorf "Error from %s (type = %s reason = %s): %s" (msg/address-str remote-node), kind, reason, message)))


(defprotocol IConnectable
  (connect [this])
  (disconnect [this]))


(defprotocol IClientNode
  (server-endpoint [this])
  (shutdown [this, now?]))


; job-registry:
;  job: {:job-id int, :job-data data, :result-callback function}
(deftype ClientNode [message-client-atom, thread-pool, role-map-atom, job-registry, connected-promise-atom, additional-data]
  
  IClientNode
  
  (server-endpoint [this]
    (deref message-client-atom))
  
  (shutdown [this, now?]    
    (threads/shutdown-thread-pool thread-pool, now?)
    (disconnect this))
  
  IConnectable
  
  (connect [this]
    (when-let [message-client (deref message-client-atom)]
      (if (msg/connect message-client)
        this
        (throw (Exception. (format "Failed to connecto to %s." (msg/address-str message-client)))))))
  
  (disconnect [this]
    (when-let [message-client (deref message-client-atom)]
      (msg/disconnect message-client)))
  
  role/IRoleStorage
  
  (role! [this, remote-node, role]
    (role/set-role* role-map-atom, remote-node, role)
    this)
  
  (role [this, remote-node]
    (role/get-role* role-map-atom, remote-node))
  
  threads/IExecutor
 
  (execute [this, action]
    (threads/submit-action thread-pool, action)))


(defn job-with-task-map
  [{:keys [tasks] :as job-data}]
  (-> job-data
    (dissoc :tasks)
    (assoc
      :task-map
      (persistent!
        (reduce
          (fn [task-map, task]
            (assoc! task-map (:task-id task) task))
          (transient {})
          tasks)))))


(defn register-job
  [^ClientNode client-node, job-data, result-callback-fn]
  (if-let [job-id (:job-id job-data)]
    (let [job-reg (.job-registry client-node)] 
      (if (contains? @job-reg job-id)
        (throw (IllegalArgumentException. (format "The :job-id %s is already used!" job-id)))
        (do
          (swap! job-reg assoc job-id {:job-data (job-with-task-map job-data), :result-callback result-callback-fn})
          nil)))
    (throw (IllegalArgumentException. "The given job has no :job-id!"))))


(defn get-job-map
  [^ClientNode client-node]
  (let [job-reg (.job-registry client-node)]
    (deref job-reg)))


(defn get-job
  [^ClientNode client-node, job-id]
  (let [job-reg (.job-registry client-node)]
    (get (deref job-reg) job-id)))


(defn remove-job
  [^ClientNode client-node, job-id]
  (let [job-reg (.job-registry client-node)]
    (swap! job-reg dissoc job-id)))


(defn get-job-result-callback
  [^ClientNode client-node, job-id]
  (some-> client-node .job-registry deref (get-in [job-id :result-callback])))


(defn setup-connected-sync
  [^ClientNode client-node]
  (let [connected-promise (promise)] 
    (-> client-node .connected-promise-atom (reset! connected-promise))
    connected-promise))

(defn notify-connected
  [^ClientNode client-node]
  (-> client-node .connected-promise-atom deref (deliver true)))


; result-callback-fn: [client, finished-tasks] -> void
(defprotocol ISputnikClient
  (submit-job [this, job-data, result-callback-fn])
  (job-finished [this, job-id]))


(deftype SputnikClient [client-node]
  
  IConnectable
  
  (connect [this]
    (let [server-endpoint (server-endpoint client-node)]
      (log/debugf "SputnikClient tries to connect to server %s ..." (msg/address-str server-endpoint))
      (connect client-node)
      (log/debugf "SputnikClient connected to server %s" (msg/address-str server-endpoint))
      (let [sync-promise (setup-connected-sync client-node)]
        (msg/send-message server-endpoint (role/role-request-message :client))  
        @sync-promise))
    this)
  
  (disconnect [this]
    (disconnect client-node)
    this)
  
  ISputnikClient
  
  (submit-job [this, job-data, result-callback-fn]
    (register-job client-node, job-data, (partial result-callback-fn this))
    (log/tracef "Sending job data to server %s:\n%s" (server-endpoint client-node) (with-out-str (pprint job-data)))
    (msg/send-message (server-endpoint client-node) (protocol/job-submission-message job-data))
    this)
  
  (job-finished [this, job-id]
    (remove-job client-node, job-id)
    this)
  
  Closeable
  
  (close [this]
    (shutdown client-node, false)
    this))




(defn+opts start-client
  "Starts a client providing it with its identifying information."
  [hostname, port | :as options]
  (log/debugf "Client for server %s:%s started with options: %s" hostname port options)
  (let [message-client-atom (atom nil)
        client-node  (ClientNode.
                       message-client-atom,
                       (threads/create-thread-pool options),
                       (atom {})
                       (atom {}),
                       (atom nil),
                       (atom nil)),
        message-client (msg/create-client hostname, port,
                         (fn handle-messages-parallel [endpoint, message]
                           (threads/safe-execute client-node, (role/handle-message-checked handle-message, client-node, endpoint, message))),
                         options)]
    (reset! message-client-atom message-client)
    (doto (SputnikClient. client-node)
      (connect))))


(defn merge-task-data
  "Add task :data to finished tasks (:data is not sent back by the worker)."
  [^SputnikClient client, finished-tasks]
  (let [job-map (get-job-map (.client-node client))]
    (mapv
      (fn [{:keys [task-data] :as finished-task}]
        (let [{:keys [job-id, task-id]} task-data,
              task (get-in job-map [job-id, :job-data, :task-map, task-id])]
          (assoc-in finished-task [:task-data :data] (:data task))))
      finished-tasks)))



(defmethod handle-message :role-granted
  [this-node, remote-node, {:keys [role]}]
  (log/debugf "Server %s granted role %s." (msg/address-str remote-node) role)
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
      (result-callback client, (merge-task-data client, (cond-> finished-tasks batched? unwrap-batched-tasks)))
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
      (job-finished client, (:job-id job))))


(defn result-data
  "Extracts the result data from the given list of finshed tasks."
  [finished-tasks]
  (mapv
    (fn [{:keys [execution-data]}]
      (or
        (when-let [ex (:exception execution-data)]
          (RuntimeException. ^String ex))
        (:result-data execution-data)))
    finished-tasks))


(defn find-first
  "Find first element in collection that matches the given predicate."
  [pred, coll]
  (reduce
    (fn [_, x]
      (when (pred x)
        (reduced x)))
    nil
    coll))


(defn- deref-results
  [rethrow, results-atom]
  (let [results (deref results-atom)]
    (if rethrow
      ; then check for exception and throw the first found execption
      (if-let [ex (find-first #(instance? Throwable %) results)]
        (throw ex)
        results)
      ; else just return results unchecked
      results)))


(defn+opts compute-job
  "Submits the job to the client, waits until all tasks are completed and returns the results.
  <async>Specifies whether this function returns immediately (true) and returns a dereferencable value or blocks until the job was completed.</>"
  [client, job | {async false, rethrow true} :as options]
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
        deref-future #'clojure.core/deref-future]     
    (if async
      (reify
        clojure.lang.IDeref 
          (deref [_]
            (deref-future job-execution)
            (deref-results rethrow, results-atom))
        clojure.lang.IBlockingDeref
          (deref [_ timeout-ms timeout-val]
            (let [val (deref-future job-execution timeout-ms ::timed-out)]
              (if (= val ::timed-out)
                timeout-val
                (deref-results rethrow, results-atom))))
        clojure.lang.IPending
          (isRealized [_]
            (realized? job-execution)))
      (do
        (deref-future job-execution)
        (deref-results rethrow, results-atom)))))


(defn+opts start-automatic-client
  "Start an automatic client that executes a job. This is intended for automatically running a client remotely.
  The job is retrieved by calling the `job-setup-fn`. The automatic client exits as soon as the job is completed."
  [server-hostname, server-port, job-setup-fn | :as options]
  (log/infof "Automatic client for server %s:%s started.", server-hostname, server-port)
  (with-open [^Closeable client (start-client server-hostname, server-port, options)]
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
  (let [^ClientNode client-node (.client-node sputnik-client)]
    (-> client-node .additional-data deref)))

(defn reset-additional-data
  [^SputnikClient sputnik-client, value]
  (let [^ClientNode client-node (.client-node sputnik-client)]
    (-> client-node .additional-data (reset! value))))

(defn update-additional-data
  [^SputnikClient sputnik-client, f & values]
  (let [^ClientNode client-node (.client-node sputnik-client)]
    (-> client-node .additional-data (apply swap! f values))))



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
    (let [{:keys [server-hostname, server-port] :as options} (read-client-config config-url)]
      (start-client server-hostname, server-port, (->option-map options)))
    (throw (java.io.FileNotFoundException. (format "Configuration file %s does not exist!" config-url)))))