; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.server.performance-data
  (:require
    [clojure.string :as str]
    [sputnik.tools.error :as e])
  (:import
    java.util.concurrent.ConcurrentHashMap))


(defn now
  {:inline (fn [] `(System/currentTimeMillis))}
  ^long []
  (System/currentTimeMillis))


(defn long-mod
  {:inline (fn [num, div]
             `(let [num# ~num,
                    div# ~div,
                    m# (rem num# div#)] 
                (if (or (zero? m#) (= (pos? num#) (pos? div#)))
                  m#
                  (+ m# div#))))}
  ^long [^long num, ^long div]
  (let [m (rem num div)] 
    (if (or (zero? m) (= (pos? num) (pos? div)))
      m
      (+ m div))))


(defn inc-mod
  {:inline (fn [x, n]
             `(long-mod (unchecked-inc ~x), ~n))}
  ^long [^long x, ^long n]
  (long-mod (unchecked-inc x), n))


(defn dec-mod
  {:inline (fn [x, n]
             `(long-mod (unchecked-dec ~x), ~n))}
  ^long [^long x, ^long n]
  (long-mod (unchecked-dec x), n))


(defn add-mod
  {:inline (fn [x, y, n]
             `(long-mod (unchecked-add ~x ~y), ~n))}
  ^long [^long x, ^long y, ^long n]
  (long-mod (unchecked-add x y), n))


(defn sub-mod
  {:inline (fn [x, y, n]
             `(long-mod (unchecked-subtract ~x ~y), ~n))}
  ^long [^long x, ^long y, ^long n]
  (long-mod (unchecked-subtract x y), n))


(defn bucket-offset
  ^long [^long bucket-time-period, ^long active-bucket-start-time, ^long timestamp]
  ; using `quot` is not possible since we need to round towards negativ infinity
  (long (Math/floor (/ (double (- timestamp active-bucket-start-time)) bucket-time-period))))



(defn update-bucket
  [^doubles mean-value-buckets, ^longs value-count-buckets, ^long i, ^double value]
  (let [mean (aget mean-value-buckets i),
        cnt  (aget value-count-buckets i),
        cnt+1 (inc cnt)]
    (aset-double mean-value-buckets, i,
      (+
        (* (/ (double cnt) cnt+1) mean)
        (/ value cnt+1)))
    (aset-long value-count-buckets, i, cnt+1)))


(defn begin-in-bucket
  ^long [^long bucket-start-time, ^long bucket-time-period, ^long task-begin-timestamp, ^long task-end-timestamp]
  (let [bucket-end-time (+ bucket-start-time bucket-time-period)]
    (if (< task-begin-timestamp bucket-start-time)
      ; task begin before bucket begin
      (if (> task-end-timestamp bucket-start-time)
        ; task end after bucket begin
        bucket-start-time
        ; whole task before bucket
        Long/MAX_VALUE)
      ; task begin after bucket begin
      (if (< task-begin-timestamp bucket-end-time)
        ; task begin within bucket
        task-begin-timestamp
        ; whole task after bucket
        Long/MAX_VALUE))))


(defn end-in-bucket
  ^long [^long bucket-start-time, ^long bucket-time-period, ^long task-begin-timestamp, ^long task-end-timestamp]
  (let [bucket-end-time (+ bucket-start-time bucket-time-period)]
    (if (> task-end-timestamp bucket-end-time)
      ; task end after bucket end
      (if (< task-begin-timestamp bucket-end-time)
        ; task start before bucket end
        bucket-end-time
        ; whole task after bucket
        Long/MIN_VALUE)
      ; task end befor bucket end
      (if (> task-end-timestamp bucket-start-time)
        ; task end after bucket begin
        task-end-timestamp
        ; whole task before bucket
        Long/MIN_VALUE))))


(def ^:const epsilon "Precision for equality comparison of floating point numbers" 1.0E-9)

(defn equal-doubles?
  {:inline (fn [x y]
             `(< (Math/abs (- ~x ~y)) ~epsilon))}
  [^double x, ^double y]
  (< (Math/abs (- x y)) epsilon))


(deftype TaskComputationPerformance
  [^double mean-duration, ^double mean-concurrency, ^double mean-speed, ^double mean-efficiency,
   ^double total-completion,^long total-duration, ^long total-cpu-time, ^long measure-period,
   ^long first-task-begin-timestamp, ^long last-update-timestamp]
  
  Object
  
  (toString [_]
    (format
      (str
        "AVG duration = %.2f, AVG concurrency = %.2f, AVG speed = %.2f, AVG efficiency = %.3f, "
        "TOTAL completion = %.2f, TOTAL duration = %d, TOTAL cpu time = %d, measure period = %d")
      mean-duration, mean-concurrency, mean-speed, mean-efficiency,
      total-completion, total-duration, total-cpu-time, measure-period))
  
  (equals [_, other]
    (and
      (instance? TaskComputationPerformance other)
      (let [^TaskComputationPerformance other other]
        (and
          (equal-doubles? mean-duration,    (.mean-duration    other))
          (equal-doubles? mean-concurrency, (.mean-concurrency other))
          (equal-doubles? mean-speed,       (.mean-speed       other))
          #_(equal-doubles? mean-efficiency,  (.mean-efficiency  other)) ; exact match of efficiency no that important in tests
          (equal-doubles? total-completion, (.total-completion other))
          (== measure-period (.measure-period other)))))))


(defn computation-performance->map
  [^TaskComputationPerformance cp-data]
  (when cp-data
    {:mean-duration              (.mean-duration              cp-data),
     :mean-concurrency           (.mean-concurrency           cp-data),
     :mean-speed                 (.mean-speed                 cp-data),
     :mean-efficiency            (.mean-efficiency            cp-data),
     :total-completion           (.total-completion           cp-data),
     :total-duration             (.total-duration             cp-data),
     :total-cpu-time             (.total-cpu-time             cp-data),
     :measure-period             (.measure-period             cp-data),
     :first-task-begin-timestamp (.first-task-begin-timestamp cp-data),
     :last-update-timestamp      (.last-update-timestamp      cp-data)}))


(defmacro performance-attribute
  "Direct acces to attributes of TaskComputationPerformance without importing the class."
  [task-computation-performance, attribute]
  `(let [^TaskComputationPerformance tcp# ~task-computation-performance]
     (. tcp# ~(symbol (name attribute)))))


(defn aggregate-performance-data
  "Aggregates a collection (vector, sequence) of computation performance data."
  [performance-data-coll]
  (let [performance-data-coll (if (vector? performance-data-coll) performance-data-coll (vec performance-data-coll)),
        n (count performance-data-coll)]
    (loop [i 0, pd-count 0, mean-duration-sum 0.0, mean-efficiency-sum 0.0, total-mean-concurrency 0.0, total-mean-speed 0.0,
           total-completion 0.0, total-duration 0, total-cpu-time 0]
      (if (< i n)
        (if-let [pd (nth performance-data-coll i)]
          (recur
            (unchecked-inc i),
            (unchecked-inc pd-count),
            (+ mean-duration-sum      (performance-attribute pd, :mean-duration))
            (+ mean-efficiency-sum    (performance-attribute pd, :mean-efficiency))
            (+ total-mean-concurrency (performance-attribute pd, :mean-concurrency))
            (+ total-mean-speed       (performance-attribute pd, :mean-speed))
            (+ total-completion       (performance-attribute pd, :total-completion))
            (+ total-duration         (performance-attribute pd, :total-duration))
            (+ total-cpu-time         (performance-attribute pd, :total-cpu-time)))
          (recur
            (unchecked-inc i),
            pd-count,
            mean-duration-sum,
            mean-efficiency-sum,
            total-mean-concurrency,
            total-mean-speed,
            total-completion,
            total-duration,
            total-cpu-time))
        {:mean-duration   (when (pos? pd-count) (/ mean-duration-sum   pd-count)),
         :mean-efficiency (when (pos? pd-count) (/ mean-efficiency-sum pd-count)),
         :total-mean-concurrency total-mean-concurrency,
         :total-mean-speed total-mean-speed,
         :total-completion total-completion,
         :total-duration total-duration,
         :total-cpu-time total-cpu-time}))))


(definterface ITaskComputationData
  ; period count n - periods are numbered: 0,...,n-1
  (^long period_count [])
  (^long period_duration [])  
  (^long period_duration [^long until-period])
  (add_task_data [^long begin-timestamp, ^long end-timestamp, ^long cpu-time])
  (computation_performance [])  
  (computation_performance [^long until-period]))


(defn computation-performance
  {:inline (fn
             ([tpm]
               `(let [^ITaskComputationData tpm# ~tpm]
                  (.computation-performance tpm#)))
             ([tpm, until-period]
               `(let [^ITaskComputationData tpm# ~tpm]
                  (.computation-performance tpm#, ~until-period))))
   :inline-arities #{1 2}}
  ([^ITaskComputationData tpm]
    (.computation-performance tpm))
  ([^ITaskComputationData tpm, ^long until-period]
    (.computation-performance tpm, until-period)))


(defn add-task-data
  {:inline (fn [wtd, begin-timestamp, end-timestamp, cpu-time]
             `(let [^ITaskComputationData wtd# ~wtd]
                (.add-task-data wtd#, ~begin-timestamp, ~end-timestamp, ~cpu-time)))}
  [^ITaskComputationData wtd, ^long begin-timestamp, ^long end-timestamp, ^long cpu-time]
  (.add-task-data wtd, begin-timestamp, end-timestamp, cpu-time))


; private interface for modifications within `locking`
(definterface IPeriodTaskComputationDataModification
  (move_active_bucket [^long active-bucket-id, ^long bucket-start-time])
  (mark_first_task_begin [^long task-begin])
  (mark_update [^long timestamp]))

(deftype PeriodTaskComputationData
  [^longs duration-sum-buckets,
   ^longs cpu-time-sum-buckets
   ^doubles completion-sum-buckets,
   ^long bucket-time-period,
   ^long bucket-count,   
   ^:volatile-mutable ^long active-bucket,
   ^:volatile-mutable ^long active-bucket-start-time,
   ^long initial-timestamp,
   ^:volatile-mutable ^long first-task-begin-time,
   ^:volatile-mutable ^long last-update-time]
  
  IPeriodTaskComputationDataModification
  
  (move-active-bucket [_, ^long active-bucket-id, ^long bucket-start-time]
    (set! active-bucket active-bucket-id)
    (set! active-bucket-start-time bucket-start-time))
  
  (mark-update [_, timestamp]
    (set! last-update-time (max timestamp last-update-time)))
  
  (mark-first-task-begin [_, task-begin]
    (when (== first-task-begin-time -1)
      (set! first-task-begin-time task-begin)))
  
  ITaskComputationData
  
  (period-count [_]
    (dec bucket-count))
  
  (period-duration [this]
    (.period-duration this, (dec bucket-count)))
  
  (period-duration [_, period-count]
    (when-not (< 0 period-count (inc bucket-count))
      (e/illegal-argument "The `until-period` must be one of 1,...,%d but %d was given!" bucket-count, period-count))
    (* bucket-time-period (inc period-count)))  
  
  (computation-performance [this]
    (.computation-performance this, bucket-count))
  
  (computation-performance [this, period-count]
    (locking this
      (when-not (< 0 period-count (inc bucket-count))
        (e/illegal-argument "The `until-period` must be one of 1,...,%d but %d was given!" bucket-count, period-count))
      ; get current time stamp to approximate the real world duration in the active bucket
      (let [now (System/currentTimeMillis)] 
        ; start at active bucket for consistency with total computation performance
        (loop [i active-bucket, steps 0, bucket-start-time active-bucket-start-time,
               total-duration 0, total-completion 0.0, total-cpu-time 0, total-bucket-duration 0]          
          (if (< steps period-count)
            ; process current bucket i
            (let [duration     (aget duration-sum-buckets   i),
                  completion   (aget completion-sum-buckets i),
                  cpu-time     (aget cpu-time-sum-buckets   i),              
                  total-duration        (unchecked-add total-duration        duration),
                  total-completion      (unchecked-add total-completion      completion)
                  total-cpu-time        (unchecked-add total-cpu-time        cpu-time)
                  time-period-in-bucket (if (or (< now bucket-start-time) (< bucket-start-time initial-timestamp))
                                            0
                                          (if (< now (+ bucket-start-time bucket-time-period))
                                            (unchecked-subtract now bucket-start-time)
                                            bucket-time-period))
                  total-bucket-duration (unchecked-add total-bucket-duration time-period-in-bucket)]
              (recur
                (dec-mod i, bucket-count),
                (unchecked-inc steps),
                (- bucket-start-time bucket-time-period),
                total-duration,
                total-completion,
                total-cpu-time,
                total-bucket-duration))
            (when (pos? total-completion)
              (TaskComputationPerformance.
                ; mean duration
                (/ (double total-duration) total-completion),
                ; mean concurrency
                (/ (double total-duration) total-bucket-duration)
                ; mean speed
                (/ (double total-completion) total-bucket-duration)
                ; mean efficiency
                (/ (double total-cpu-time) total-duration)
                ; total completion
                total-completion
                ; total duration
                total-duration
                ; total cpu time
                total-cpu-time
                ; measure-period
                total-bucket-duration,
                ; first task begin time
                first-task-begin-time,
                ; last update time
                last-update-time)))))))
    
  (add-task-data [this, begin-timestamp, end-timestamp, cpu-time]
    (locking this
      (.mark-first-task-begin this, begin-timestamp)
      (.mark-update this, end-timestamp)
      (let [offset (bucket-offset bucket-time-period, active-bucket-start-time, (dec end-timestamp))]
        ; move to new active bucket when end-timestamp lies outside the active bucket
        (when (pos? offset)
          ; move active bucket
          (let [step-count (min offset, bucket-count),
                new-active-bucket (add-mod active-bucket, offset, bucket-count),
                new-start-time (+ active-bucket-start-time (* offset bucket-time-period))]
            (loop [i active-bucket, steps 0]
              ; move as far as the offset 
              (when (< steps step-count)
                (let [i+1 (inc-mod i, bucket-count)]
                  (aset-long duration-sum-buckets   i+1 0)
                  (aset-long cpu-time-sum-buckets   i+1 0)
                  (aset-double completion-sum-buckets i+1 0.0)
                  (recur i+1, (inc steps)))))
            (.move-active-bucket this, new-active-bucket, new-start-time))))
      ; update buckets
      (let [max-shift (- (dec bucket-count))
            task-begin-offset (-> (bucket-offset bucket-time-period, active-bucket-start-time, begin-timestamp)
                                (max max-shift))
            task-end-offset   (-> (bucket-offset bucket-time-period, active-bucket-start-time, (dec end-timestamp))
                                (max max-shift)),
            n (add-mod active-bucket, task-end-offset, bucket-count),
            task-duration (max (- end-timestamp begin-timestamp) 0)]
        (loop [i (add-mod active-bucket, task-begin-offset, bucket-count),
               ; note: task-begin-offset is signed
               bucket-start-time (+ active-bucket-start-time (* task-begin-offset bucket-time-period)),
               cpu-time-psum 0.0,
               cpu-time-rounded-psum 0]
          (let [begin (begin-in-bucket bucket-start-time, bucket-time-period, begin-timestamp, end-timestamp),
                end   (end-in-bucket   bucket-start-time, bucket-time-period, begin-timestamp, end-timestamp)]
            (when (and (< begin Long/MAX_VALUE) (> end Long/MIN_VALUE)),
              (let [bucket-duration (- end begin),
                    ; task completion is the relative portion of the task within the bucket
                    bucket-completion (/ (double bucket-duration) task-duration),
                    ; assumption: cpu-time is equally distributed over the complete real timespan                  
                    bucket-cpu-time (* bucket-completion cpu-time)
                    ; round cpu time (cascade rounding)
                    cpu-time-psum (+ cpu-time-psum bucket-cpu-time)
                    rounded-bucket-cpu-time (long (- cpu-time-psum cpu-time-rounded-psum))]      ;(println bucket-completion bucket-cpu-time rounded-bucket-cpu-time)          
                (aset-long duration-sum-buckets i (+ bucket-duration (aget duration-sum-buckets i)))
                (aset-long cpu-time-sum-buckets i (+ rounded-bucket-cpu-time (aget cpu-time-sum-buckets i)))
                (aset-double completion-sum-buckets i (+ bucket-completion (aget completion-sum-buckets i)))
                (when-not (== i n)
                  (recur
                    (inc-mod i, bucket-count),
                    (+ bucket-start-time bucket-time-period),
                    cpu-time-psum,
                    (+ cpu-time-rounded-psum rounded-bucket-cpu-time)))))))))
    this)
  
  Object
  (toString [this]
    (locking this
      (format "%02d (of %02d) @ %d :\n%s"
        active-bucket bucket-count active-bucket-start-time
        (str/join ",\n"
          (map
            (fn [i]
              (let [bucket-start-time (- active-bucket-start-time (* bucket-time-period (sub-mod active-bucket i bucket-count)))]
                (format "%d: [%d, %d), DUR: %7.2f, CPU: %7.2f, COMPL: %7.2f"
                  i
                  bucket-start-time, (+ bucket-start-time bucket-time-period),
                  (aget duration-sum-buckets i), (aget cpu-time-sum-buckets i), (aget completion-sum-buckets i))))
            (range 0 bucket-count)))))))


(defn create-period-task-computation-data
  [^long bucket-time-period, ^long interval-count, ^long initial-timestamp]
  ; one more bucket than intervals
  (PeriodTaskComputationData.
    (long-array interval-count),
    (long-array interval-count),
    (double-array interval-count),
    bucket-time-period, interval-count,
    0, initial-timestamp, initial-timestamp, -1, -1))


; private interface for modifications within `locking`
(definterface ITotalTaskComputationDataModification
  (mark_first_task_begin [^long timestamp])
  (mark_update [^long timestamp])
  (add_task_duration [^long value])
  (add_cpu_time [^long value])
  (count_task []))


(deftype TotalTaskComputationData
  [^:volatile-mutable ^long first-task-begin-time,
   ^:volatile-mutable ^long last-update-time,
   ^:volatile-mutable ^long total-task-duration,
   ^:volatile-mutable ^long total-cpu-time,
   ^:volatile-mutable ^long total-task-count]
  
  ITotalTaskComputationDataModification
  
  (mark-first-task-begin [_, timestamp]
    (when (== first-task-begin-time -1)
      (set! first-task-begin-time timestamp)))
  
  (mark-update [_, timestamp]
    (set! last-update-time (max last-update-time, timestamp)))
  
  (add-task-duration [_, value]
    (set! total-task-duration (+ total-task-duration value)))
  
  (add-cpu-time [_, value]
    (set! total-cpu-time (+ total-cpu-time value)))
  
  (count-task [_]
    (set! total-task-count (inc total-task-count)))
  
  ITaskComputationData
  
  (period-count [_]
    1)
  
  (period-duration [this]
    (locking this
      (- last-update-time first-task-begin-time)))
  
  (period-duration [this, until-period]
    (.period-duration this))  
  
  (computation-performance [this]
    (locking this
      (when (pos? total-task-count)
        (let [duration (- last-update-time first-task-begin-time)]
          (when (pos? duration)
            (TaskComputationPerformance.
              ; mean duration
              (/ (double total-task-duration) total-task-count),
              ; mean concurrency
              (/ (double total-task-duration) duration)
              ; mean speed
              (/ (double total-task-count) duration)
              ; mean efficiency
              (/ (double total-cpu-time) total-task-duration)
              ; total completion
              total-task-count
              ; total duration
              total-task-duration
              ; total cpu time
              total-cpu-time
              ; measure-period
              duration,
              ; first task begin time
              first-task-begin-time,
              ; last update time
              last-update-time))))))

  (computation-performance [this, until-period]
    (.computation-performance this))
    
  (add-task-data [this, begin-timestamp, end-timestamp, cpu-time]
    (locking this
      (.add-task-duration this, (max (- end-timestamp begin-timestamp), 0))
      (.add-cpu-time this, cpu-time)
      (.count-task this)
      (.mark-first-task-begin this, begin-timestamp)
      (.mark-update this, end-timestamp))
    this)
  
  Object
  (toString [this]
    (locking this
      (format "[%d, %d] TASK DUR: %d, CPU: %d, #TASKS: %d",
        first-task-begin-time, last-update-time,
        total-task-duration, total-cpu-time, total-task-count))))


(defn create-total-task-computation-data
  [^long bucket-time-period, ^long interval-count, ^long initial-timestamp] ; initial-timestamp is only included for interface compatibility with `create-period-task-computation-data`
  ; one more bucket than intervals
  (let [bucket-count (inc interval-count)]
    (TotalTaskComputationData.
       -1, -1, 0, 0, 0)))



(definterface IEntityPerformanceData
  (add_task_data_for [id, ^long task-begin, ^long task-end, ^long cpu-time])
  (total_computation_performance_of [id])
  (period_computation_performance_of [id])
  (period_computation_performance_of [id, ^long period])
  (total_computation_performance_map [])
  (period_computation_performance_map [])
  (period_computation_performance_map [^long period])
  ; returns duration for each period as map (period id -> period duration)
  (periods []))



(defmacro update-data-inline
  [create-fn, performance-data-map, bucket-time-period, interval-count, initial-timestamp, id, task-begin, task-end, cpu-time]
  `(let [performance-data-map# ~performance-data-map,
         id# ~id,         
         performance-data# (or
                             (.get performance-data-map#, id#)
                             (let [bucket-time-period# ~bucket-time-period,
                                   task-begin# ~task-begin,
                                   begin-delta# (mod (- task-begin# ~initial-timestamp) bucket-time-period#),
                                   first-bucket-begin# (- task-begin# begin-delta#),
                                   pd# (~create-fn bucket-time-period#, ~interval-count, first-bucket-begin#)]
                               (.put performance-data-map#, id#, pd#)
                               pd#))]
     (add-task-data performance-data#, ~task-begin, ~task-end, ~cpu-time)
     performance-data-map#))

; for both entities: workers and jobs (initial time stamp is used to have the same bucket layout wrt. begin time stamps for every entity)
(deftype EntityPerformanceData [^ConcurrentHashMap period-performance-data-map, ^ConcurrentHashMap total-performance-data-map, ^long bucket-time-period, ^long interval-count, ^long initial-timestamp]

  IEntityPerformanceData
  
  (add_task_data_for [this, id, ^long task-begin, ^long task-end, ^long cpu-time]
    ; period data
    (update-data-inline
      create-period-task-computation-data, period-performance-data-map, bucket-time-period, interval-count, initial-timestamp,
      id, task-begin, task-end, cpu-time)
    ; total data
    (update-data-inline
      create-total-task-computation-data,  total-performance-data-map,  bucket-time-period, interval-count, initial-timestamp,
      id, task-begin, task-end, cpu-time)
    this)

  (total-computation-performance-of [_, id]
    (some-> total-performance-data-map (.get id) computation-performance))

  (period-computation-performance-of [_, id]
    (some-> period-performance-data-map (.get id) computation-performance))
  
  (period-computation-performance-of [_, id, ^long period]
    (some-> period-performance-data-map (.get id) (computation-performance period)))
  
  (total-computation-performance-map [_]
    (->> total-performance-data-map
      .entrySet
      (mapv (fn [[id, performance-data]] (vector id (computation-performance performance-data))))
      (into {})))
  
  (period-computation-performance-map [_]
    (->> period-performance-data-map
      .entrySet
      (mapv (fn [[id, performance-data]] (vector id (computation-performance performance-data))))
      (into {})))
  
  (period-computation-performance-map [_, period]
    (->> period-performance-data-map
      .entrySet
      (mapv (fn [[id, performance-data]] (vector id (computation-performance performance-data, period))))
      (into {})))
  
  (periods [_]
    (loop [i 1, m (transient {})]
      (if (<= i interval-count)
        (recur
          (unchecked-inc i)
          (assoc! m i (* i bucket-time-period)))
        (persistent! m)))))


(defn create-entity-performance-data
  [^long bucket-time-period, ^long interval-count, ^long initial-timestamp]
  (EntityPerformanceData. (ConcurrentHashMap.), (ConcurrentHashMap.), bucket-time-period, interval-count, initial-timestamp))


(defn add-task-data-for
  {:inline (fn [epd, id, task-begin, task-end, cpu-time]
             `(let [^IEntityPerformanceData epd# ~epd]
                (.add-task-data-for epd#, ~id, ~task-begin, ~task-end, ~cpu-time)))}
  [^IEntityPerformanceData epd, id, task-begin, task-end, cpu-time]
  (.add-task-data-for epd, id, task-begin, task-end, cpu-time))


(defn total-computation-performance-of
  {:inline (fn [epd, id]
             `(let [^IEntityPerformanceData epd# ~epd]
                (.total-computation-performance-of epd#, ~id)))}
  [^IEntityPerformanceData epd, id]
  (.total-computation-performance-of epd, id))


(defn period-computation-performance-of 
  {:inline (fn
             ([epd, id]
               `(let [^IEntityPerformanceData epd# ~epd]
                 (.period-computation-performance-of epd#, ~id)))
             ([epd, id, period]
               `(let [^IEntityPerformanceData epd# ~epd]
                  (.period-computation-performance-of epd#, ~id, ~period))))}
  ([^IEntityPerformanceData epd, id]
    (.period-computation-performance-of epd, id))
  ([^IEntityPerformanceData epd, id, ^long period]
    (.period-computation-performance-of epd, id, period)))
  

(defn total-computation-performance-map
  {:inline (fn [epd]
             `(let [^IEntityPerformanceData epd# ~epd]
                (.total-computation-performance-map epd#)))}
  [^IEntityPerformanceData epd]
  (.total-computation-performance-map epd))


(defn period-computation-performance-map 
  {:inline (fn
             ([epd]
               `(let [^IEntityPerformanceData epd# ~epd]
                  (.period-computation-performance-map epd#)))
             ([epd, period]
               `(let [^IEntityPerformanceData epd# ~epd]
                  (.period-computation-performance-map epd#, ~period))))}
  ([^IEntityPerformanceData epd]
    (.period-computation-performance-map epd))
  ([^IEntityPerformanceData epd, ^long period]
    (.period-computation-performance-map epd, period)))


(defn periods
  {:inline (fn [epd]
             `(let [^IEntityPerformanceData epd# ~epd]
                (.periods epd#)))}
  [^IEntityPerformanceData epd]
  (.periods epd))