; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-client.protocol)

(defn create-task
  "Creates a task data map for usage in run/compute-job."
  [task-id, execute-fn, & param-data]
  {:task-id task-id, :task-function execute-fn, :task-data (vec param-data)})

(defn conj-task-data
  ([task, data-1]
    (update-in task [:task-data] conj data-1))
  ([task, data-1 & additional-data]
    (update-in task [:task-data] into (list* data-1 additional-data))))

(defn create-job
  "Creates a job data map for usage in run/compute-job."
  [job-id, tasks]
  {:job-id job-id, :tasks tasks})