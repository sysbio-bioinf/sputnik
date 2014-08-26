; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.rest-client
  (:require
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.pprint :refer [pprint]]
    [clojure.options :refer [defn+opts]]
    [clojure.tools.logging :refer [infof, debugf, errorf, tracef]]
    (compojure
      [core :as c :refer [GET, POST]]
      [route :as r]
      [handler :as h])
    [ring.middleware.stacktrace :as st]
    [ring.adapter.jetty :as jetty]
    [ring.util.response :as resp]
    [sputnik.satellite.client :as client]
    [sputnik.satellite.protocol :as proto])
  (:import
    java.io.Closeable))



(defn extract-job
  [client]
  (let [{:keys [task-management]} (client/additional-data client)]
    (dosync
      (let [{:keys [new-tasks, next-job-id]} @task-management]
        (when (seq new-tasks)
          (let [taskid-promise-pairs (map (juxt :task-id :result-promise) new-tasks)
                job (proto/create-job next-job-id (map #(dissoc % :result-promise) new-tasks))]
            ; increase next-job-id, clear new-task vector, add promises to task-result-map
            (alter task-management
              #(-> %
                 (update-in [:next-job-id] inc)
                 (update-in [:new-tasks] (constantly []))
                 (update-in [:task-result-map] into taskid-promise-pairs)))
            job))))))


(defn handle-task-results
  [client, finished-tasks]
  (let [{:keys [task-management]} (client/additional-data client),
        {:keys [task-result-map]} (dosync @task-management)]
    (doseq [{:keys [task-data execution-data] :as task} finished-tasks,
            :let [{:keys [task-id]} task-data,
                  result-promise (task-result-map task-id)]]
      (tracef "Result for task %d received:\n%s" task-id)
      (if result-promise
        (do
          (deliver result-promise execution-data)
          (alter task-management update-in [:task-result-map] dissoc task-id))
        (errorf "Task result promise for task %d not found!" task-id)))))


(defn job-submission
  [client, job-submission-timeout]
  (debugf "Job submission thread has started.")
  (loop []
    (try 
      (when-let [job (extract-job client)]
        (debugf "Job %d with %d tasks created." (:job-id job) (-> job :tasks count))
        (tracef "Job content:\n" (with-out-str (pprint job)))
        (client/submit-job client, job,
          (fn [_, finished-tasks]
            (handle-task-results client, finished-tasks))))
      (catch Throwable t
        (errorf "Exception caught in the job submission thread:\n%s" (with-out-str (print-cause-trace t)))))
    ; timeout to gather some tasks
    (Thread/sleep job-submission-timeout)
    ; continue with next worker
    (recur)))




(defn submit-task
  [client, request]
  (debugf "Received request with the following parameters:\n%s" (with-out-str (pprint (:params request))))
  (try
    (let [{:keys [task-management]} (client/additional-data client),
         {:keys [execute-fn] :as params} (:params request),
         params (dissoc params :execute-fn),
         result-promise (promise)]
     ; create and add task
     (dosync
       (let [{:keys [next-task-id]} @task-management,
             task (-> (proto/create-task next-task-id (symbol execute-fn), params)
                    (assoc :result-promise result-promise))]
         (alter task-management
           #(-> %
              (update-in [:next-task-id] inc)
              (update-in [:new-tasks] conj task)))))
     ; outside of dosync: wait for the computation result
     (let [{:keys [result, error]} (deref result-promise)]
       (cond-> (if error error result)
         true resp/response
         true (resp/content-type "text/plain")
         error (resp/status 400))))
    (catch Throwable t
      (-> (resp/response (with-out-str (print-cause-trace t)))
        (resp/content-type "text/plain")
        (resp/status 400)))))


(defn main-routes
  [client]
  (c/routes
    (GET "/" [] "Sputnik REST client\n")
    (POST "/submit" request (submit-task client, request) "submitted!")    
    (r/not-found "Not found!")))


(defn+opts client-api
  [client-node]     
  (h/api
    (st/wrap-stacktrace-web
      (main-routes client-node)
      #_(friend/authenticate 
	       (if ssl-port
	         (friend/requires-scheme (main-routes client-node) :https {:https ssl-port})
	         (main-routes client-node))
	       {:credential-fn (partial creds/bcrypt-credential-fn (create-user-map admin-user, admin-password))
	        :workflows [(workflows/interactive-form)]}))))


(defn+opts ^org.eclipse.jetty.server.Server create-jetty-instance
  [client-node, port | {min-threads 10, max-threads 1000, max-idle-time (* 5 60 1000)}]
  (jetty/run-jetty (client-api client-node) 
	                   {:port port,
                      :host "localhost",
                      :min-threads min-threads,
                      :max-threads max-threads,
                      :max-idle-time max-idle-time,
                      ; do not block
                      :join? false}))


(defn+opts start-rest-client
  [hostname, nodename, port, rest-port, server-hostname, server-nodename, server-port | {job-submission-timeout 1000} :as options]
  (infof "REST client started as %s@%s:%s to be connected to server %s@%s:%s providing the REST service on port %s.",
    nodename, hostname, port,
    server-nodename, server-hostname, server-port,
    rest-port)
  (with-open [^Closeable client (doto (client/start-client hostname, nodename, port, options) 
                                  #_(client/connect server-hostname, server-nodename, server-port))]
    (let [finished? (promise),
          server (create-jetty-instance client, rest-port)]
      (client/reset-additional-data client,
        {:finished finished?, :server server,
         :task-management (ref {:task-result-map {}, :new-tasks [], :next-task-id 1, :next-job-id 1}),
         :job-submission-thread (future (job-submission client, job-submission-timeout)), })
      (deref finished?)
      (.stop server))))