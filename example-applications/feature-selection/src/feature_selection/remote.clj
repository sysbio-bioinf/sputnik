; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns feature-selection.remote
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.stacktrace :as st]
    [clojure.pprint :as pp]
    [clojure.tools.logging :as log]
    [clojure.options :refer [defn+opts, ->option-map]]
    [sputnik.satellite.client :as c]
    [sputnik.satellite.protocol :as p]
    [sputnik.tools.resolve :as r]
    [sputnik.tools.format :as fmt]
    [sputnik.tools.file-system :as fs]
    [sputnik.config.api :as cfg]
    [feature-selection.tools :as t]
    [feature-selection.bitset :as b]
    [feature-selection.fitness :as f])
  (:import
    java.util.concurrent.CountDownLatch))


(defn active-sleeping
  "Wait for the specified amount of time by calculating nonsence for cpu usage."
  [duration]
  (let [begin (System/currentTimeMillis)
        end (+ begin duration)]
    (loop []
      (when (< (System/currentTimeMillis) end)
        (reduce * (repeat 10000 1.001))
        (recur)))))


(defn batch-evaluation
  ([load-datasets-fn, file-list, sample-list, individual-data-list]
    (batch-evaluation load-datasets-fn, file-list, sample-list, individual-data-list, nil))
  ([load-datasets-fn, file-list, sample-list, individual-data-list, sleep-duration]
    (let [result (persistent!
                   (reduce
                     (fn [res, {:keys [individual-id, selected-genes-array]}]
                       (conj! res
                         (assoc (f/evaluate-feature-selection load-datasets-fn, file-list, sample-list, selected-genes-array)
                           :individual-id individual-id)))
                     (transient [])
                     individual-data-list))]
      ; when specified then sleep for the specified duration
      (when sleep-duration
        (active-sleeping sleep-duration))
      result)))


(defn prepare-remote-evaluation-tasks-batched
  [sample-list, load-datasets-fn, file-list, population, batch-size]
  (let [individual-data-list (mapv
               (fn [i, individual]
                 {:individual-id i, :individual individual})
               (range)
               population),
        individuals-map (->> individual-data-list (map (juxt :individual-id :individual)) distinct (into {}))
        batches (vec (partition-all batch-size individual-data-list))
        n (count batches)]
    (loop [id 0, task-coll (transient [])]
      (if (< id n)
        (let [batch-data (nth batches id),
              task (p/create-task id,
                     'feature-selection.remote/batch-evaluation,
                     load-datasets-fn, file-list, sample-list,
                     (mapv
                       (fn [individual-data]
                         (-> individual-data
                           (select-keys [:individual-id])
                           (assoc :selected-genes-array (-> individual-data :individual :selected-genes b/to-long-array))))
                       batch-data))]
          (recur (inc id), (conj! task-coll task)))
        {:tasks (persistent! task-coll),
         :individuals-map individuals-map}))))


(defn merge-individuals-with-results
  [individuals-map, batched-results]
  (let [results (apply concat batched-results)]
    (persistent!
      (reduce
        (fn [res, {:keys [individual-id] :as result-data}]
          (let [individual (individuals-map individual-id)]
            (conj! res (merge individual (dissoc result-data :individual-id)))))
        (transient [])
        results))))


(defn maybe-add-sleep-duration
  [sleep-duration, sleep-frequency, tasks]
  (if (and sleep-duration sleep-frequency)
    (mapv
      (fn [i, task]
        (cond-> task
          (zero? (mod i sleep-frequency))
          (p/conj-task-data sleep-duration)))
      (range)
      tasks)
    tasks))


(defn distributed-evaluation
  [sleep-duration, sleep-frequency, client, batch-size, jobcounter-atom, load-datasets-fn, file-list, sample-list, population]
  (let [{:keys [tasks, individuals-map]} (prepare-remote-evaluation-tasks-batched sample-list, load-datasets-fn, file-list, population, batch-size),
        tasks (maybe-add-sleep-duration sleep-duration, sleep-frequency, tasks)
        job-id (swap! jobcounter-atom inc),
        job (p/create-job (format "%s - %03d" (fmt/datetime-filename-format (java.lang.System/currentTimeMillis)), job-id), tasks),
        _ (log/debugf "Starting computation of job %s:\n%s" (:job-id job) (with-out-str (pp/pprint job))),
        begin-time (System/currentTimeMillis),
        results (c/compute-job client, job, :print-progress false),
        end-time (System/currentTimeMillis)]
    (with-meta (merge-individuals-with-results individuals-map, results)
       {:duration (- end-time begin-time)})))



(defn batch-test
  [train-instance-coords, test-instance-coords, load-datasets-fn, file-list, individual-data-list]
  (persistent!
    (reduce
      (fn [res, {:keys [individual-id, selected-genes-array]}]
        (conj! res
          (assoc (f/test-feature-selection train-instance-coords, test-instance-coords, load-datasets-fn, file-list, selected-genes-array)
            :individual-id individual-id)))
      (transient [])
      individual-data-list)))


(defn prepare-remote-test-tasks-batched
  [train-instance-coords, test-instance-coords, load-datasets-fn, file-list, population, batch-size]
  (let [individuals (mapv
                      (fn [i, individual]
                        {:individual-id i, :individual individual})
                      (range)
                      population),
        individuals-map (->> individuals (map (juxt :individual-id :individual)) distinct (into {})),
        batches (vec (partition-all batch-size individuals))
        n (count batches)]
    (loop [id 0, task-coll (transient [])]
      (if (< id n)
        (let [batch-data (nth batches id),
              task (p/create-task id,
                     'feature-selection.remote/batch-test,
                     train-instance-coords, test-instance-coords, load-datasets-fn, file-list,
                     (mapv
                       (fn [individual-data]
                         (-> individual-data
                           (select-keys [:individual-id])
                           (assoc :selected-genes-array (-> individual-data :individual :selected-genes b/to-long-array))))
                       batch-data))]
          (recur (inc id), (conj! task-coll task)))
        {:tasks (persistent! task-coll),
         :individuals-map individuals-map}))))



(defn distributed-test
  [client, batch-size, load-datasets-fn, file-list, train-instance-coords, test-instance-coords, population]
  (let [{:keys [tasks, individuals-map]} (prepare-remote-test-tasks-batched train-instance-coords, test-instance-coords, load-datasets-fn, file-list, population, batch-size),
        job (p/create-job (format "%s - test" (fmt/datetime-filename-format (java.lang.System/currentTimeMillis))), tasks),
        _ (log/debugf "Starting computation of job %s:\n%s" (:job-id job) (with-out-str (pp/pprint job)))
        begin-time (System/currentTimeMillis),
        results (c/compute-job client, job, :print-progress false),
        end-time (System/currentTimeMillis)]
    (with-meta (merge-individuals-with-results individuals-map, results)
       {:duration (- end-time begin-time)})))