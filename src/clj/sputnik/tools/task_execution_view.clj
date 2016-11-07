; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.tools.task-execution-view
  "Visualization of task execution for use on REPL with stored task execution duration data."
  (:require
    [quil.core :as q]))

; Note that the export of task durations has been removed, since the data is not collected at the server anymore
; since the introduction of sputnik.tools.performance-data!
; This code is still included since the task durations could be gathered at client side.

(defn find-extreme-value
  [selection-fn, attribute-key, coll]
  (let [vals (map #(get % attribute-key) coll)]
    (when (seq vals)
      (reduce selection-fn (first vals) (rest vals)))))


(defn determine-duration-interval
  [worker-task-durations-map]
  (let [task-durations (mapcat val worker-task-durations-map)]
    {:earliest-start (find-extreme-value min, :start, task-durations),
     :latest-end (find-extreme-value max, :end, task-durations)}))



(def task-height 20)
(def task-height+sep (+ task-height 5))

(def fill-color [200 255 200])

(defn draw-executions-per-worker
  [{:keys [earliest-start] :as interval}, pixel-per-millisecs, y, durations]
  (let [durations-per-thread (vals (group-by :thread-id durations))]
    (->
      (reduce
        (fn [y, thread-durations]
          (->
            (reduce
              (fn [y, {:keys [start, end, duration, efficiency]}]
                (let [x (+ task-height+sep (* (- start earliest-start) pixel-per-millisecs))]
                  (q/with-fill fill-color
                    (q/with-stroke [0]
                      (q/rect
                        x
                        y
                        (* duration pixel-per-millisecs)
                        task-height)))
                  (q/with-fill [0]
                    (q/text (format "%d" (long (quot duration 1000))) (+ 2 x) (+ y (* 0.75 task-height)))))
                y)
              y
              thread-durations)
            (+ task-height+sep)))
        y
        durations-per-thread)
      (+ task-height+sep))))


(defn draw-task-execution
  [worker-task-durations-map, interval, pixel-per-millisecs]
  (reduce #(draw-executions-per-worker interval, pixel-per-millisecs, %1, %2) task-height+sep (vals worker-task-durations-map)))


(defn determine-thread-count
  [worker-task-durations-map]
  (->> worker-task-durations-map
    vals
    (map #(count (group-by :thread-id %)))
    (reduce +)))


(defn task-execution-view
  ([worker-task-durations-map, pixel-per-millisecs]
    (task-execution-view worker-task-durations-map, pixel-per-millisecs, true))
  ([worker-task-durations-map, pixel-per-millisecs, show?]
    (let [{:keys [earliest-start, latest-end] :as interval} (determine-duration-interval worker-task-durations-map),
          worker-count (count worker-task-durations-map),
          thread-count (determine-thread-count worker-task-durations-map)]
      (q/sketch
        :title "Task execution view"
        :setup (fn [] (q/background 255))
        :draw (fn [] (#'draw-task-execution worker-task-durations-map, interval, pixel-per-millisecs))
        :size [(+ (* 2 task-height+sep) (* (- latest-end earliest-start) pixel-per-millisecs))
               (+ task-height+sep (* thread-count task-height+sep) (* worker-count task-height+sep))]
        :target (if show? :frame :none)))))


(defn speed-up
  [worker-task-durations-map-1, worker-task-durations-map-2]
  (let [dinterval-1 (determine-duration-interval worker-task-durations-map-1),
        dinterval-2 (determine-duration-interval worker-task-durations-map-2)]
    (/
      (double (- (:latest-end dinterval-1) (:earliest-start dinterval-1)))
      (double (- (:latest-end dinterval-2) (:earliest-start dinterval-2))))))


(defn speed
  [worker-task-durations-map]
  (let [{:keys [earliest-start, latest-end]} (determine-duration-interval worker-task-durations-map),
        duration-sum (->> worker-task-durations-map
                       (mapcat val)
                       (map :duration)
                       (reduce + 0.0))]
    (/ duration-sum (- latest-end earliest-start))))