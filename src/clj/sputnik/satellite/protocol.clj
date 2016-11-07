; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.protocol
  (:require
    [sputnik.config.meta :as meta]))


(defn- attribute-symbol
  [x]
  (if (vector? x) (first x) x))

(defn- attribute-keyword
  [x]
  (-> (if (vector? x) (first x) x)
    name
    keyword))

(defn- attribute-value
  [x]
  (if (vector? x) (second x) x))

(defmacro defmessage
  "
  roles is either a list of roles or :all"
  [msg-name, roles & attributes]
 (let [msg-keyword (-> msg-name name keyword)] 
  `(do
     (swap! message-permissions assoc 
       ~msg-keyword 
       ~(cond
          (= roles :all) `(constantly true)
          (vector? roles)`(fn [role#] (~(set roles) role#))
          :else (fn [role#] `(~#{roles} role#))))
     (defn ~(with-meta (-> msg-name name (str "-message") symbol) (meta msg-name))
       [~@(map attribute-symbol attributes)]
       (meta/with-type ~msg-keyword
         (hash-map 
           ~@(interleave 
               (map attribute-keyword attributes) 
               (map attribute-value   attributes))))))))


(def message-permissions (atom {}))

(defn message-allowed?
  [role, msg]
  (boolean
    (when-let [f (get @message-permissions (type msg))]
      (f role))))

(defn message-type?
  [msg-type, msg]
  (= msg-type (type msg)))



; task & job

(defn create-task*
  ([task-id, execute-fn, param-data-list]
    {:task-id task-id, :task-type :single-task, :function execute-fn, :data (vec param-data-list)}))

(defn create-task
  ([task-id, execute-fn, & param-data]
    {:task-id task-id, :task-type :single-task, :function execute-fn, :data (vec param-data)}))

(defn create-task-with-info
  ([task-id, task-info, execute-fn, & param-data]
   {:task-id task-id, :task-type :single-task, :task-info task-info, :function execute-fn, :data (vec param-data)}))

(defn conj-task-data
  ([task, data-1]
    (update-in task [:data] conj data-1))
  ([task, data-1 & additional-data]
    (update-in task [:data] into (list* data-1 additional-data))))

(defn create-task-batch
  ([task-id, task-coll]
    {:task-id task-id, :task-type :task-batch, :tasks (vec task-coll)}))

(defn task-batch?
  [task]
  (= (:task-type task) :task-batch))

(defn create-job
  [job-id, tasks]
  {:job-id job-id, :tasks tasks})


(defn create-task-key
  [task]
  (select-keys task [:client-id, :job-id, :task-id]))


; common


; client

(defmessage job-submission [:client] job)

; server

(defmessage worker-id-response [:server] unique-id actual-nickname)
(defmessage task-distribution [:server] tasks)
(defmessage worker-thread-setup [:server] thread-count)
(defmessage finished-task-notfication [:server] finished-task-keys)
(defmessage worker-shutdown [:server] now?)

; worker

(defmessage worker-thread-info [:worker] thread-count)
(defmessage worker-id-request [:worker] nickname, unique-id)

; worker & server
(defmessage tasks-completed [:worker :server] finished-tasks)