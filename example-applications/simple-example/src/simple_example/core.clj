; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns simple-example.core
  (:require
    [clojure.java.io :as io]
    [clojure.options :refer [defn+opts]]
    [sputnik.api :as s]))


(defn in-unit-circle?
  "Is the point given by (x,y) located within the unit circle?"
  [^double x, ^double y]
  (<= (+ (* x x) (* y y)) 1.0))


(defn rand-coordinate
  "Returns a random number in the interval [-1.0,1.0)."
  ^double [^java.util.Random rnd]
  (- (* 2.0 (.nextDouble rnd)) 1.0))


(defn sample-points
  "Randomly generate the given number of points and count the points contained in the unit circle."
  [seed, point-count]
  (let [rnd (java.util.Random. (long seed))]
    (loop [i 0, in-circle 0]
      (if (< i point-count)
        (let [x (rand-coordinate rnd) #_(.nextDouble rnd),
              y (rand-coordinate rnd) #_(.nextDouble rnd)]
          (recur
            (unchecked-inc i),
            (if (in-unit-circle? x, y)
              (unchecked-inc in-circle)
              in-circle)))
        {:in-circle in-circle, :point-count point-count}))))


(defn sample-points-list
  [seed, point-count]
  (let [rnd (java.util.Random. (long seed))]
    (loop [i 0, points (transient [])]
      (if (< i point-count)
        (let [x (rand-coordinate rnd),
              y (rand-coordinate rnd)]
          (recur
            (unchecked-inc i),
            (conj! points [x, y, (in-unit-circle? x, y)])))
        (persistent! points)))))


(defn export-sample-points-tex
  [filename, seed, point-count]
  (with-open [f (io/writer filename)]
    (binding [*out* f]
      (doseq [[x, y, in-circle?] (sample-points-list seed, point-count)]
        (println (format "\\node[point%s] at (%.3f,%.3f) {};" (if in-circle? ",incircle" "") x, y))))))



(defn create-job
  "Creates a job consisting of the given number of tasks.
  Each task will randomly generate the given number of points."
  [seed, task-count, point-count-per-task]
  (let [rnd (java.util.Random. (long seed))]
    (s/create-job (System/currentTimeMillis),
      (mapv
        #(s/create-task %, `sample-points, (.nextLong rnd), point-count-per-task)
        (range task-count)))))



(defn+opts estimate-pi
  [seed, task-count, point-count-per-task | {progress? false} :as options]
  ; create client from configuration file
  (with-open [^java.io.Closeable client (s/create-client options)]
    ; execute the pi estimation job and wait for its results
    (let [task-results (s/compute-job client, (create-job seed, task-count, point-count-per-task), :print-progress progress?),
          in-circle-total (reduce + (mapv :in-circle task-results))]
      (when progress?
        (Thread/sleep 1000)
        (shutdown-agents))
      (println "Estimate of pi:" (/ (* 4.0 in-circle-total) (* task-count point-count-per-task)))
      (flush))))



(defn+opts estimate-pi-new
  [seed, task-count, point-count-per-task | {progress? false} :as options]
  ; create client from configuration file
  (let [rnd (java.util.Random. (long seed)),
        in-circle-total (->> (repeatedly task-count #(.nextLong rnd))
                          (mapv
                            (fn [seed]
                              (s/future (sample-points seed, point-count-per-task))))
                          (reduce
                            (fn [in-circle-total, result]
                              (+ in-circle-total (:in-circle (deref result))))
                            0))]
    (println "Estimate of pi:" (/ (* 4.0 in-circle-total) (* task-count point-count-per-task)))))