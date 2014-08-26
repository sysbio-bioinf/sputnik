; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns simple-example.core
  (:require
    [sputnik.satellite.protocol :as p]
    [sputnik.satellite.client :as c]))


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
        (let [x (.nextDouble rnd),
              y (.nextDouble rnd)]
          (recur
            (unchecked-inc i),
            (if (in-unit-circle? x, y)
              (unchecked-inc in-circle)
              in-circle)))
        {:in-circle in-circle, :point-count point-count}))))




(defn create-job
  "Creates a job consisting of the given number of tasks.
  Each task will randomly generate the given number of points."
  [seed, task-count, point-count-per-task]
  (let [rnd (java.util.Random. (long seed))]
    (p/create-job (System/currentTimeMillis),
      (mapv
        #(p/create-task %, `sample-points, (.nextLong rnd), point-count-per-task)
        (range task-count)))))



(defn estimate-pi
  [config-url, seed, task-count, point-count-per-task, progress?]
  ; create client from configuration file
  (with-open [^java.io.Closeable client (c/create-client-from-config config-url)]
    ; execute the pi estimation job and wait for its results
    (let [task-results (c/compute-job client, (create-job seed, task-count, point-count-per-task), :print-progress progress?),
          in-circle-total (reduce + (mapv :in-circle task-results))]
      (when progress?
        (Thread/sleep 1000)
        (shutdown-agents))
      (println "Estimate of pi:" (/ (* 4.0 in-circle-total) (* task-count point-count-per-task)))
      (flush))))