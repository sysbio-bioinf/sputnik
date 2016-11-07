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
    [clojure.string :as str]
    [clojure.options :refer [defn+opts]]
    [clojure.tools.logging :as log]
    (compojure
      [core :as c :refer [GET, POST, DELETE]]
      [route :as r]
      [handler :as h])
    [ring.middleware.stacktrace :as st]
    [ring.adapter.jetty :as jetty]
    [ring.util.response :as resp]
    [sputnik.version :as v]
    [sputnik.satellite.client :as client]
    [sputnik.satellite.protocol :as proto]
    [sputnik.tools.error :as e]
    [ring.middleware.keyword-params :as kw-params]
    [ring.middleware.params :as params]
    [ring.middleware.json :as json]
    [cemerick.friend :as friend]
    (cemerick.friend
      [workflows :as workflows]
      [credentials :as creds]))
  (:use
    (hiccup core page form util element))
  (:import
    java.io.Closeable
    java.util.UUID))



(defn extract-task-results
  [finished-tasks]
  (->> finished-tasks
    (reduce
      (fn [result-map, {:keys [task-data execution-data]}]
        (let [{:keys [job-id, task-info]} task-data
              {:keys [list-index]} task-info,
              task-result (-> execution-data
                            (dissoc :thread-id)
                            (assoc :list-index list-index))]
          (assoc! result-map
            job-id
            (conj (get result-map job-id []) task-result))))
      (transient {}))
    persistent!))


(defn handle-task-results
  [client, finished-tasks]
  (try
    (let [{:keys [job-management]} (client/additional-data client),
          task-results-by-job-id (extract-task-results finished-tasks)]
      (dosync
        (alter job-management
          (fn [jm]
            (update-in jm [:job-result-map]            
              #(reduce-kv
                 (fn [result-map, job-id, task-results]
                   (let [n (count task-results)]
                     (log/debugf "%s results for job %s received:" n, job-id)
                     (if (contains? result-map job-id)                     
                       (-> result-map
                         (update-in [job-id, :task-results] into task-results)
                         (update-in [job-id, :finished-task-count] + n))
                       (do
                         (log/infof
                           "Task results ignored because job \"%s\" does not exist (never submitted or already deleted)."
                           job-id)
                         result-map))))
                 %
                 task-results-by-job-id))))))
    (catch Throwable t
      (log/errorf "The following exception occured in handle-task-result:\n%s"
        (with-out-str (print-cause-trace t))))))


(defn json->task
  [fn, task-id, task-map]
  (let [{:strs [list-index, instance, parameters]} task-map]
    (proto/create-task-with-info
      task-id,
      {:list-index list-index},
      fn,
      {:instance instance, :parameters parameters})))


(defn json->job
  [job-data]
  (let [{:strs [fn, task-data-list]} job-data,
        fn-symbol (if (string? fn)
                    (symbol fn)
                    (e/illegal-argument "The value bound to \"fn\" (%s) must be a string refering to a clojure function!" (pr-str fn)))]
    (when (zero? (count task-data-list))
      (e/illegal-argument "No task data given! \"task-data-list\" must contain at least one map with task data!"))
    (proto/create-job
      (UUID/randomUUID)
      (mapv (partial json->task fn-symbol) (range) task-data-list))))



(defn register-job
  [client, {:keys [job-id, tasks] :as job}]
  (let [{:keys [job-management]} (client/additional-data client)]
    (dosync
      (alter job-management
        (fn [jm]
          (-> jm
            (assoc-in [:job-map job-id] job)
            (assoc-in [:job-result-map job-id]
              {:task-count (count tasks),
               :finished-task-count 0,
               :task-results []})))))))


(defn submission-agent
  [client]
  (:job-submission-agent (client/additional-data client)))


(defn submit-job
  [client, job]
  (send
    (submission-agent client)
    (fn [agent-state]
      (try
        (log/debugf "Submitting job %s." (:job-id job))
        (client/submit-job client, job,
          (fn [_, finished-tasks]
            (handle-task-results client, finished-tasks)))
        (inc agent-state)
        (catch Throwable t
          (log/errorf "Exception caught during submit-job:\n%s" (with-out-str (print-cause-trace t))))))))


(defmacro wrap-exception-response
  [context & body]
  `(try
     ~@body
     (catch Exception e#
       (let [ex-trace# (with-out-str (print-cause-trace e#))
             context#  (str ~context)]
         (log/errorf "The following exception occured in the context of \"%s\":\n%s" context#, ex-trace#)
         (resp/status 
           (resp/response {"exception" ex-trace#, :context context#})
           400)))))


(defn submission
  [client, request]
  (log/debugf "Received request with the following body:\n%s" (with-out-str (pprint (:body request))))
  (wrap-exception-response "job submission"
    (let [job (json->job (:body request))]
      (log/debugf "Job %s with %s tasks created." (:job-id job) (-> job :tasks count))
      (log/tracef "Job content:\n" (with-out-str (pprint job)))
      (register-job client job)
      (submit-job  client, job)
      (resp/status (resp/response {"job-id" (str (:job-id job))}) 200))))


(defn ->uuid
  [x]
  (cond
    (instance? UUID x) x,
    (string? x) (UUID/fromString x)))

(defn uuid?
  [s]
  (boolean (re-matches #"^[\da-f]{8,8}(-[\da-f]{4,4}){3,3}-[\da-f]{12,12}$" s)))


(defn job-exists?
  [client, job-id]
  (let [job-id (->uuid job-id)]
    (some-> (client/additional-data client)
      :job-management
      deref
      :job-map
      (contains? job-id))))


(defn check-job-id
  "Returns error message (JSON) when errors are detected"
  [client, job-id]
  (when-let [error (cond
                     (not (uuid? job-id)) 
                       {:exception (format "%s is not a valid job id (UUID)!" job-id)}
                     (not (job-exists? client, job-id))
                       {:exception (format "Job with id %s does not exist!" job-id)})]
    (-> error
      resp/response
      (resp/status 400))))


(defn job-result-map
  [client, job-id]
  (let [job-id (->uuid job-id)]
    (some-> (client/additional-data client)
      :job-management
      deref
      (get-in [:job-result-map job-id]))))


(defn job-progress
  [client, job-id]
  (log/debugf "Job progress of job \"%s\" requested." job-id)
  (wrap-exception-response "job progress"
    (let [{:keys [task-count, finished-task-count]} (job-result-map client, job-id)]
      (resp/response
        {:finished (== task-count finished-task-count),
         :progress (/ (double finished-task-count) task-count)}))))


(defn job-results
  [client, job-id]
  (log/debugf "Job results of job \"%s\" requested." job-id)
  (wrap-exception-response "job results"
    (let [{:keys [task-results, task-count, finished-task-count]} (job-result-map client, job-id)]
      (resp/response
        {:finished (== task-count finished-task-count),
         :results task-results}))))


(defn delete-job
  [client, job-id]
  (log/debugf "Deletion of job \"%s\" requested." job-id)
  (wrap-exception-response "delete job"
    (let [job-id (->uuid job-id),
          {:keys [job-management]} (client/additional-data client)]
      (client/job-finished client, job-id)
      (dosync
        (alter job-management
          (fn [jm]
            (-> jm
              (update-in [:job-map] dissoc job-id)
              (update-in [:job-result-map] dissoc job-id)))))
    (resp/response {:deleted (str job-id)}))))




(def irace-data-format
  (str
    "<h2>JSON format for POST /irace/submit</h2>\n"
    "<h3>Request</h3>\n"
    "The request body contains a function given as string in <b>\"fn\"</b> and a list of task data in <b>\"task-data-list\"</b>.<br>\n"
    "The task data contains the following keys:<br>\n"
    "<b>\"list-index\"</b> ... index of the candidate (parameter configuration) within the list provided by irace (roundtrip id to assign the result to the correct candidate)<br>\n"
    "<b>\"instance\"</b> ... the url to the problem instance for the run<br>\n"
    "<b>\"parameters\"</b> ... the selected parameter configuration (e.g. as command line options, if the given \"fn\" supports that)\n"
    "<h3>Response</h3>\n"
    "A map containing a <b>\"job-id\"</b>.\n"
    "<h2>JSON format for GET /irace/:job-id/progress</h2>\n"
    "<h3>Request</h3>\n"
    "There is no data required in the request body.\n"
    "<h3>Response</h3>\n"
    "A map containing the \"finished\" flag and the fraction of finished tasks in \"progress\".\n"
    "<h2>JSON format for GET /irace/:job-id/results</h2>\n"
    "<h3>Request</h3>\n"
    "There is no data required in the request body.\n"
    "<h3>Response</h3>\n"
    "A map containing the \"finished\" flag and the result data of the evaluated tasks in \"results\"."
    "<h2>JSON format for DELETE /irace/:job-id/results</h2>\n"
    "<h3>Request</h3>\n"
    "There is no data required in the request body.\n"
    "<h3>Response</h3>\n"
    "A map containing the id of the deleted job in \"deleted\"."))


(defn irace-routes
  [client]
  (c/routes
    (POST   "/submit" request (submission client, request))
    (GET    "/:job-id/progress" [job-id] (or (check-job-id client, job-id) (job-progress client, job-id)))
    (GET    "/:job-id/results"  [job-id] (or (check-job-id client, job-id) (job-results  client, job-id)))
    (DELETE "/:job-id"          [job-id] (or (check-job-id client, job-id) (delete-job   client, job-id)))))


(defn main-routes
  [client]
  (c/routes
    (GET "/" [] (html
                  (format "<h1>Sputnik REST client (Sputnik %s)</h1>\n%s\n"
                    (v/sputnik-version)
                    irace-data-format)))
    (c/context "/irace" request (friend/wrap-authorize (irace-routes client) #{::user}))
    (r/not-found "Not found!")))


(defn create-user-map
  [service-user, service-password]
  {service-user {:username service-user
                 :password (creds/hash-bcrypt service-password)
                 :roles #{::user}}})


(defn+opts client-api
  [client-node, port, ssl-enabled | {service-user nil, service-password nil}]
  (json/wrap-json-response
    (json/wrap-json-body
      (params/wrap-params
        (kw-params/wrap-keyword-params
          (friend/authenticate
            (if ssl-enabled
              (friend/requires-scheme (main-routes client-node) :https {:https port})
              (main-routes client-node))
            {:allow-anon? false,
             :unauthenticated-handler #(workflows/http-basic-deny "Sputnik REST service" %),
             :workflows [(workflows/http-basic
                           :credential-fn (partial creds/bcrypt-credential-fn (create-user-map service-user, service-password))
                           :realm "Sputnik REST service")]}))))))


(defn+opts ^org.eclipse.jetty.server.Server create-jetty-instance
  [client-node, port | {ssl-enabled false, keystore nil, keystore-password nil,  min-threads 10, max-threads 1000, max-idle-time (* 5 60 1000)} :as options]
  (binding [friend/*default-scheme-ports* {:http  (if ssl-enabled (dec port) port)
                                           :https (if ssl-enabled port (inc port))}]
    (jetty/run-jetty
      (client-api client-node, port, ssl-enabled, options) 
      {:port (if ssl-enabled (dec port) port),
        :host "localhost",
        :min-threads min-threads,
        :max-threads max-threads,
        :max-idle-time max-idle-time,
        :ssl? ssl-enabled
        :ssl-port (when ssl-enabled port)
        :keystore (when ssl-enabled keystore)
        :key-password (when ssl-enabled keystore-password)
        ; do not block
        :join? false})))


(defn+opts start-rest-client
  [rest-port, server-hostname, server-port | :as options]
  (log/infof "REST client for server %s:%s started providing the REST service on port %s.",
     server-hostname, server-port, rest-port)
  (with-open [^Closeable client (client/start-client server-hostname, server-port options)]
    (let [finished? (promise),
          server (create-jetty-instance client, rest-port, options)]
      (client/reset-additional-data client,
        {:finished finished?, :server server,
         :job-management (ref {:job-map {}, :job-result-map {}}),
         :job-submission-agent (agent 0), })
      (deref finished?)
      (.stop server))))