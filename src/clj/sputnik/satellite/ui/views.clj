; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.ui.views
  (:require
    [sputnik.version :as v]
    [sputnik.satellite.role-based-messages :as r]
    [sputnik.satellite.server :as server]
    [sputnik.satellite.server.management :as mgmt]
    [sputnik.tools.performance-data :as pd]
    [sputnik.tools.format :as fmt]
    [sputnik.tools.file-system :as fs]
    [clojure.pprint :as pp]
    [clojure.string :as string])
  (:import
    java.util.UUID)
  (:use
    (hiccup core page form util element)))


(defn page-not-found
  []
  "Page not found")

(defn login-page
  [{{login-failed "login_failed", username "username"} :query-params, :as request}]
  (html
    [:h1 "Login"]    
    (when login-failed
      [:p [:b (format "Login as \"%s\" failed: wrong username or password!" username)]]) 
    (form-to [:post "/login"]
      [:table
       [:tr [:td "username"] [:td (text-field {:autofocus true} :username)]]
       [:tr [:td "password"] [:td (password-field :password)]]]
      [:p (submit-button {:name "submit"} "Login")])))


(defn- get-name
  [x]
  (cond
    (string? x) x
    (instance? clojure.lang.Named x) (name x)
    :else (str x)))

(defn combine-in-body
  [& html-desc-list]
  (->> html-desc-list
    (mapcat (fn [x]
              (cond-> x
                (and (vector? x) (some-> x first get-name (= "body")))
                rest
                (or (not (sequential? x)) (not (vector? (first x))))
                vector)))
    (into [:body])))


(defmacro html-timing-info
  "Adds generation duration info to the generated html document."
  [title & body]
 `(let [start-time# (System/nanoTime),
        result# (html ~@body)
        end-time# (System/nanoTime)]    
    (html5
      [:title ~title]
      (combine-in-body
        result#
        [:p {:style "position:fixed;right:1em;bottom:0%;"}
         [:i (fmt/datetime-format (System/currentTimeMillis)) [:br] (format "Page generation: %,d ms elapsed." (quot (- end-time# start-time#) 1000000))]]))))


(defn worker-data
  [server-node, rating-period]
  (let [mgr (server/manager server-node),
        rated-workers (mgmt/rated-workers mgr, rating-period, false)]
    rated-workers
    #_(mapv
       (fn [{:keys [worker-id] :as worker-data}]
         (let [worker-node (node/get-remote-node server-node worker-id)]
           (assoc worker-data
             :last-active nil (when worker-node (node/last-activity worker-node)))))
       rated-workers)))


(defn- maybe-add
  [total-concurrency, worker-concurrency]
  (if worker-concurrency
    (+ worker-concurrency (or total-concurrency 0.0))
    total-concurrency))

(defn maybe-multiply-add
  [total-duration, completed-task-count, avg-duration]
  (if avg-duration
    (+ (* avg-duration completed-task-count) (or total-duration 0.0))
    total-duration))


(defn assure-sequential
  [s]
  (if (sequential? s)
    s
    (throw (IllegalArgumentException. (format "Sequential data needed instead of data of class %s!" (class s))))))


(defn update!
  "Updates a the value at the given key in the given transient map by applying f with the addition arguments args to it."
  [m, k, f, & args]
  (assoc! m k (apply f (get m k) args)))

(defn prefix-keyword
  [prefix, k]
  (->>
    (if (instance? clojure.lang.Named k) (name k) (str k))
    (str prefix)
    keyword))

(defn aggregate-properties
 "Aggregates the given properties (input-output-sepcification) with the given aggregate function starting with the given initial value
  for all maps in the given seq.
  property-spec is a collection of [input-output-spec, initial-value, aggregate-fn].
  Different input-output-specification options:
   - [:key-word-only, initial-value, aggregate-fn]
   - [[:result-keyword :param-keyword], initial-value, aggregate-fn]
   - [[:result-keyword [:param-keyword1, :param-keyword2, ...]],  initial-value, aggregate-fn]"
 {:arglists '([map-seq & property-spec] [map-seq, prefix & property-spec])}
 [map-seq & property-spec]
 (let [prefix (when (string? (first property-spec)) (first property-spec)),
       property-spec (if prefix (rest property-spec) property-spec),
       prefix (or prefix "total-")
       result-key+selection-fn-pair 
       (for [[x, init-value, aggregate-fn] property-spec]
         [(cond
            (keyword? x)
              [(prefix-keyword prefix x) (juxt #(get % x))]
            (vector? x)
              (if (== 2 (count x))
                (let [[k v] x]
                  (cond
                    (keyword? v) [k (juxt #(get % v))]
                    (vector? v) [k (apply juxt (for [y v] #(get % y)))]
                    :else (throw (IllegalArgumentException. (format "Only keyword or vector allowed as paramter selection specification in an input-output-specification! But encountered %s!" v)))))
                (throw (IllegalArgumentException. (format "Only two elemts allowed in an input-output-specification! But encountered %s!" x))))
            :else
              (throw (IllegalArgumentException. (format "Only keyword or vector allowed as input-output-specification! But encountered %s!" x))))
          init-value,
          aggregate-fn])]
   (->> (assure-sequential map-seq)
	   (reduce
	     (fn [aggregate-map, m]
	       (reduce
	         (fn [aggregate-map, [[k, selection-fn], init-value, aggregate-fn]]
	           (try
               (update! aggregate-map k 
	               #(apply aggregate-fn
                    (if (nil? %) init-value %), 
                    (selection-fn m)))
               (catch Exception e
                 (throw (Exception. (format "Exception when trying to aggregate property \"%s\"!" (pr-str k)), e)))))          
	         aggregate-map
	         result-key+selection-fn-pair))
	     (transient {}))
	   persistent!)))


(defn server-summary-data
  [rated-worker-coll, client-data]
  (let [connected-rated-workers (filter :connected rated-worker-coll)]
    (merge
      {:worker-count (count connected-rated-workers),
       :total-performance-data
       (pd/aggregate-performance-data (mapv :total-performance-data connected-rated-workers)),
       :selected-period-performance-data
       (pd/aggregate-performance-data (mapv :selected-period-performance-data connected-rated-workers)),
       :thread-count (reduce
                       (fn [n, worker]
                         (+ n (:thread-count worker)))
                       0
                       connected-rated-workers)
       :client-count (count client-data)}
      ; client attribute calculation unchanged (18.11.2014) because it is not performance critical
      (aggregate-properties client-data, "total-"
        [:task-count nil maybe-add],
        [:finished-task-count nil maybe-add],
        [:job-count nil maybe-add]
        [:exception-count 0 +]))))


(defn jobs-progress-data
  [job-map]
  (reduce-kv
    (fn [{:keys [active-jobs-task-count, active-jobs-finished-task-count, total-task-count, total-finished-task-count, exception-count] :as result},
         job-id,
         {:keys [task-count, finished-task-count, exceptions]}]
      (cond-> result
        true
          (assoc :exception-count (+ exception-count (count exceptions)))
        task-count
          (assoc
            :total-task-count
            (if total-task-count (+ total-task-count task-count) task-count))
        finished-task-count
          (assoc
            :total-finished-task-count
            (if total-finished-task-count (+ total-finished-task-count finished-task-count) finished-task-count))
        (and task-count finished-task-count (< finished-task-count task-count))
          (assoc
            :active-jobs-task-count
            (if active-jobs-task-count (+ active-jobs-task-count task-count) task-count),
            :active-jobs-finished-task-count
            (if active-jobs-finished-task-count (+ active-jobs-finished-task-count finished-task-count) finished-task-count))))
    {:exception-count 0}
    job-map))


; client data calculation unchanged (18.11.2014) because it is not performance critical
(defn client-data
  [server-node]
  (let [mgr (server/manager server-node)]
    (->> (mgmt/client-data-map mgr)
      (reduce-kv
        (fn [result, client-id, {:keys [jobs] :as client-data}]
          (let [{:keys [total-task-count, total-finished-task-count, active-jobs-task-count, active-jobs-finished-task-count, exception-count]}
                (jobs-progress-data jobs)]
            (conj! result
              (assoc client-data
                :client-id client-id,
                :job-count (count jobs),
                :exception-count exception-count,
                :task-count total-task-count,
                :finished-task-count total-finished-task-count,
                :active-jobs-task-count active-jobs-task-count,
                :active-jobs-finished-task-count active-jobs-finished-task-count,
                :progress (cond
                            (and active-jobs-task-count, active-jobs-finished-task-count)
                              (/ (* 100.0 active-jobs-finished-task-count) active-jobs-task-count)
                            (and total-task-count, total-finished-task-count)
                              (/ (* 100.0 total-finished-task-count) total-task-count))))))
        (transient []))
      persistent!)))


(defn exception-data
  [server-node]
  (persistent!
    (reduce-kv
      (fn [exception-list, _, {:keys [jobs] :as client-data}]
        (reduce-kv
          (fn [exception-list, _, {:keys [exceptions]}]
            (reduce conj! exception-list exceptions))
          exception-list
          jobs))
      (transient [])
      (-> server-node server/manager mgmt/client-data-map))))


(defn add-estimations
  [now, job-data, computation-performance-data]
  (when computation-performance-data
    (let [{:keys [task-count, finished-task-count]} job-data,
          mean-speed (pd/performance-attribute computation-performance-data, :mean-speed),
          estimated-duration (/ (- task-count finished-task-count) mean-speed)]
      (assoc (pd/computation-performance->map computation-performance-data)
        :estimated-duration estimated-duration
        :estimated-end (+ now estimated-duration)))))


(defn extract-job-attributes
  [job-performance-data-map, now, client-id, job-id, {:keys [task-count, finished-task-count, durations, exceptions] :as job-data}]
  (let [{:keys [period-computation-performance-map, total-computation-performance-map]} job-performance-data-map,
        job-key [client-id, job-id]]
    (assoc (select-keys job-data [:task-count, :finished-task-count, :finished, :register-time])
	     :job-id job-id,
		   :progress (/ (double finished-task-count) task-count),
       :exception-count (count exceptions),
       :selected-period-performance-data (add-estimations now, job-data, (period-computation-performance-map job-key)),
       :total-performance-data           (add-estimations now, job-data, (total-computation-performance-map  job-key)))))


(defn job-data
  [server-node, client-data, rating-period]
  (let [mgr (server/manager server-node),
        job-performance-data-map (mgmt/job-performance-data-map mgr, rating-period),
        now (System/currentTimeMillis)] 
    (->> client-data
	    (reduce
		    (fn [result-map, {:keys [client-id, client-info, jobs] :as client-data}]
		      (assoc! result-map
	          (select-keys client-data [:client-id :client-info :job-count])
	          (->> jobs
	            (reduce-kv
	              (fn [result-coll, job-id, job-data]
	                (conj! result-coll (extract-job-attributes job-performance-data-map, now, client-id, job-id, job-data)))
	              (transient []))
	            persistent!)))
		    (transient {}))
	    persistent!)))


(defn- row
  [tag & columns]
  (into [:tr] (map #(conj tag %) columns)))

(def ^:private table-head (partial row [:th {:valign :top}]))
(def ^:private table-row  (partial row [:td {:align :center}]))


(defn- format-maybe-nil
  [format-fn, value]
  (if value (format-fn value) "N/A"))

(defn- format-maybe-nil-pair
  ([format-fn, value, value-period]
    (format-maybe-nil-pair "%s<br>(%s)", format-fn, value, value-period))
  ([combination-fmt, format-fn, value, value-period]  
    (format combination-fmt
      (format-maybe-nil format-fn value)
      (format-maybe-nil format-fn value-period))))


(defn format-speed
  [time-unit, value]
  (->>
    (case time-unit
	    "sec"     1000
	    "min"  (* 1000 60)
	    "hour" (* 1000 60 60)
      "day"  (* 1000 60 60 24)
      "week" (* 1000 60 60 24 7))
    (* value)
    (format "%,.2f")))

(defn- render-worker-row
  [{:keys [thread-count, assigned-task-count, worker-info, worker-id, last-active, connected, disconnect-time,
           selected-period-performance-data, total-performance-data]},
   time-unit, connected?]
  (let [total-performance-data           (pd/computation-performance->map total-performance-data),
        selected-period-performance-data (pd/computation-performance->map selected-period-performance-data)
        extract-attribute (fn [attribute-kw] (map attribute-kw [total-performance-data, selected-period-performance-data]))]
    (if  connected?
      (table-row 
        worker-info, thread-count, assigned-task-count,
        (apply format-maybe-nil-pair #(format "%.2f" %),           (extract-attribute :total-completion)),
        (apply format-maybe-nil-pair #(format "%3.2f" %),          (extract-attribute :mean-concurrency)),
        (apply format-maybe-nil-pair #(format "%3.2f" %),          (extract-attribute :mean-efficiency)),
        (apply format-maybe-nil-pair fmt/duration-format,          (extract-attribute :mean-duration)),
        (apply format-maybe-nil-pair #(format-speed time-unit, %), (extract-attribute :mean-speed)),
        connected,
        (form-to [:post (format "/admin/worker/%s" worker-id)]
           [:table 
            [:tr 
             [:td "#Threads:"]
             [:td (text-field {:maxlength 3 :size 2 :disabled (when-not connected true)} :thread-count thread-count)]
             [:td (submit-button {:name "change" :disabled (when-not connected true)} "change")]]]),
        (form-to [:post (format "/admin/worker/%s/shutdown" worker-id)]
           [:table
            [:tr
             [:td (check-box "confirm" false) "confirm"]
             [:td (submit-button {:name "shutdown"} "shutdown")]]]))
      (table-row 
        worker-info, thread-count,
        (format-maybe-nil #(format "%.2f" %),           (:total-completion total-performance-data)),
        (format-maybe-nil #(format "%3.2f" %),          (:mean-concurrency total-performance-data)),
        (format-maybe-nil #(format "%3.2f" %),          (:mean-efficiency total-performance-data)),
        (format-maybe-nil fmt/duration-format,          (:mean-duration total-performance-data)),
        (format-maybe-nil #(format-speed time-unit, %), (:mean-speed total-performance-data)),
        [:i (fmt/datetime-format disconnect-time)]))))

(defn- render-worker-table
  [rated-worker-coll, rating-period-map, rating-period, time-unit, connected?]
  (let [rated-worker-coll (filterv #(= (:connected %) connected?) rated-worker-coll),
        period-str (format "<br>(%.1f min)" (/ (rating-period-map rating-period) 1000.0 60.0))]
    [[:h3 (format "%s Worker Nodes (%d)" (if connected? "Connected" "Disconnected") (count rated-worker-coll))]
     (into [:table {:border 1}
            (if connected?
              (table-head "Worker" "#Threads" "#Assigned Tasks"
                (str "Task Completion" period-str), (str "Concurrency" period-str), (str "Efficiency" period-str),
                (str "AVG[duration]" period-str), (str "AVG[Speed]", period-str, "<br>tasks/", time-unit),
                "Connected?", "Setup", "Shutdown")
              (table-head "Worker" "#Threads"
                "Task Completion", "Concurrency", "Efficiency",
                "AVG[duration]", (str "AVG[Speed]", "<br>tasks/", time-unit),
                "Disconnected"))]
	      (->> rated-worker-coll
         (sort-by (if connected? :worker-info :disconnect-time))
         (mapv #(render-worker-row %, time-unit, connected?))))]))


(defn- render-client-table
  [client-data-coll]
  [[:h3 (format "Client Nodes (%d)" (count client-data-coll))]
   (into [:table {:border 1} (table-head "Client" "#Jobs" "#Tasks" "#Finished Tasks" "Progress" "#Exceptions" "Connected?" "Disconnected")]
     (map
       (fn [{:keys [job-count, task-count, finished-task-count, progress client-info, last-active, exception-count, connected, disconnect-time]}]
         (table-row client-info, job-count, task-count, finished-task-count,
           (if progress (format "%5.2f%%" progress) "N/A"),
           exception-count,
           connected, 
           (cond
             ;last-active (fmt/datetime-format last-active)
             disconnect-time [:i (fmt/datetime-format disconnect-time)]
             :else "-")))
       (sort-by :client-info client-data-coll)))])


(defn- format-job-measure
  [both?, fmt, value1, value2]
  (if both?
    (format-maybe-nil-pair fmt, value1, value2)
    (format-maybe-nil fmt, value1)))


(defn- latest-job-comparison
  [job1, job2]
  (let [{{estimated-end-1 :estimated-end, finished-time-1 :last-update-timestamp} :total-performance-data,
         finished?-1 :finished,
         register-time-1 :register-time} job1,
        {{estimated-end-2 :estimated-end, finished-time-2 :last-update-timestamp} :total-performance-data,
         finished?-2 :finished,
         register-time-2 :register-time} job2]
    (if (or finished?-1 finished?-2)
      ; one of the jobs is finished
      (cond
        ; both finished: latest finished comes first
        (and finished?-1 finished?-2) (> finished-time-1 finished-time-2)
        ; only job 1 finished => later in list than job 2 (which is still processed)
        finished?-1 false
        ; only job 2 finished => later in list than job 1 (which is still processed)
        finished?-2 true)
      (cond
        ; both have estimations: latest estimation comes first
        (and estimated-end-1 estimated-end-2) (> estimated-end-1 estimated-end-2)
        estimated-end-1 false
        estimated-end-2 true
        ; both have no estimation data: compare register times (latest first)
        :else (> register-time-1 register-time-2)))))


(defn- render-job-information
  [job-data-map, rating-period-map, rating-period, time-unit]
  (let [job-count (reduce-kv #(+ %1 (count %3)) 0 job-data-map)
        period-str (format "<br>(%.1f min)" (/ (rating-period-map rating-period) 1000.0 60.0))]
    [[:h2 (format "Jobs (%d)" job-count)]
     (into [:table {:border 1}
            (table-head "Job ID" "#Tasks" "#Finished Tasks" "Progress" "#Exceptions" "Finished" "Current Duration"
              (str "Concurrency" period-str),
              (str "Efficiency" period-str),
              (str "AVG[Duration]" period-str),
              (str "AVG[Speed]", period-str, "<br>tasks/" time-unit),
              (str "CPU Time" period-str),
              (str "Estimated Duration" period-str)
              (str "(Estimated) End" period-str))]
       (apply concat
         (for [[{:keys [client-info job-count]} jobs] (sort-by (comp :client-info first) job-data-map)]
	         (into
	           [[:tr [:td {:colspan 15, :align :center, :style "color:navy"} [:b (format "%s (#jobs = %d)" client-info job-count)]]]]
	           (for [{:keys [job-id, task-count, finished-task-count, finished, progress, exception-count,
                           selected-period-performance-data, total-performance-data]}
                   (sort latest-job-comparison jobs)
                   :let [in-progress? (not finished),
                         job-duration (:measure-period total-performance-data),
                         extract-attribute (fn [attribute-kw] (map attribute-kw [total-performance-data, selected-period-performance-data]))]]
	             (table-row
	               job-id, task-count, finished-task-count, (format "%5.2f%%" (* 100 progress)), exception-count, finished,
                 (format-maybe-nil fmt/duration-with-days-format, job-duration),
                 (apply format-job-measure in-progress?, #(format "%3.2f" %), (extract-attribute :mean-concurrency)),
                 (apply format-job-measure in-progress?, #(format "%3.2f" %), (extract-attribute :mean-efficiency)),
	               (apply format-job-measure in-progress?, fmt/duration-format, (extract-attribute :mean-duration)),
                 (apply format-job-measure in-progress?, #(format-speed time-unit, %), (extract-attribute :mean-speed)),
                 (apply format-job-measure in-progress?, fmt/duration-with-days-format, (extract-attribute :total-cpu-time)),
                 (if finished
                   ""
                   (apply format-job-measure in-progress?, fmt/duration-with-days-format, (extract-attribute :estimated-duration))),
                 (if finished
                   (fmt/datetime-format (:last-update-timestamp total-performance-data))
                   (apply format-job-measure in-progress?, fmt/datetime-format, (extract-attribute :estimated-end)))))))))]))


(defn real-number?
  [x]
  (and x (not (or (Double/isNaN x) (Double/isInfinite x)))))


(defn average-task-duration
  [{:keys [total-duration, total-completion] :as performance-data}]
  (when (and total-duration total-completion (pos? total-completion))
    (/ total-duration total-completion)))



(defn- estimate-duration
  [remaining-task-count, performance-data]
  (when remaining-task-count
    (if (zero? remaining-task-count)
      0
      (let [speed (some-> performance-data :total-mean-speed )]
        (when (and speed (pos? speed))
          (/ remaining-task-count speed))))))

(defn- render-server-summary
  [{:keys [total-performance-data, selected-period-performance-data,
           total-job-count, total-task-count, total-finished-task-count, worker-count,
           total-exception-count,
           client-count, thread-count]},
   rating-period-map, rating-period, time-unit]
  (let [period-str (format "(%.1f min)" (/ (rating-period-map rating-period) 1000.0 60.0)),
        now (System/currentTimeMillis),
        remaining-task-count (when (and total-task-count total-finished-task-count)
                               (- total-task-count total-finished-task-count))
        estimated-duration (estimate-duration remaining-task-count, total-performance-data),
        estimated-duration-period (estimate-duration remaining-task-count, selected-period-performance-data),
        estimated-end (some-> estimated-duration (+ now)),
        estimated-end-period (some-> estimated-duration-period (+ now)),        
        total-progress (when (and total-task-count, total-finished-task-count)
                         (/ (* 100.0 total-finished-task-count) total-task-count))]
    [[:h2 "Server Summary"]
     [:table {:border 1}
      [:tr
       [:td [:b "Connected Worker Count:"]]
       [:td {:align :center} (format-maybe-nil #(format "%d" (long %)) worker-count)]]
      [:tr
       [:td [:b "Thread Count:"]]
       [:td {:align :center} (format-maybe-nil #(format "%d" (long %)) thread-count)]]
      [:tr
       [:td [:b "Client Count:"]]
       [:td {:align :center} (format-maybe-nil #(format "%d" (long %)) client-count)]]
      [:tr
       [:td [:b "Total Job Count:"]]
       [:td {:align :center} (format-maybe-nil #(format "%d" (long %)) total-job-count)]]
      [:tr
       [:td [:b "Total Task Count:"]]
       [:td {:align :center} (format-maybe-nil #(format "%d" (long %)) total-task-count)]]
      [:tr
       [:td [:b "Total Finished Task Count:"]]
       [:td {:align :center} (format-maybe-nil #(format "%d" (long %)) total-finished-task-count)]]
      [:tr
       [:td [:b "Total Progress:"]]
       [:td {:align :center} (format-maybe-nil #(format "%3.2f%%" %) total-progress)]]
      [:tr
       [:td [:b "Total Exception Count:"]]
       [:td {:align :center} (format-maybe-nil #(format "%d" %) total-exception-count)]]]
     [:p]
	   [:table {:border 1}
      [:tr [:th] [:th "Total"] [:th period-str]]
	    [:tr
	     [:td [:b "Average Concurrency:"]]
	     [:td {:align :center} (format-maybe-nil #(format "%5.2f" %), (:total-mean-concurrency total-performance-data))]
       [:td {:align :center} (format-maybe-nil #(format "%5.2f" %), (:total-mean-concurrency selected-period-performance-data))]]
      [:tr
	     [:td [:b "Average Efficiency:"]]
	     [:td {:align :center} (format-maybe-nil #(format "%5.2f" %), (:mean-efficiency total-performance-data))]
       [:td {:align :center} (format-maybe-nil #(format "%5.2f" %), (:mean-efficiency selected-period-performance-data))]]
	    [:tr
	     [:td [:b "Task Completion:"]]
	     [:td {:align :center} (format-maybe-nil #(format "%.2f" %) (:total-completion total-performance-data))]
       [:td {:align :center} (format-maybe-nil #(format "%.2f" %) (:total-completion selected-period-performance-data))]]
	    [:tr
	     [:td [:b "Task Duration Sum:"]]
	     [:td {:align :center} (format-maybe-nil fmt/duration-with-days-format (:total-duration total-performance-data))]
       [:td {:align :center} (format-maybe-nil fmt/duration-with-days-format (:total-duration selected-period-performance-data))]]
      [:tr
	     [:td [:b "CPU Time Sum:"]]
	     [:td {:align :center} (format-maybe-nil fmt/duration-with-days-format (:total-cpu-time total-performance-data))]
       [:td {:align :center} (format-maybe-nil fmt/duration-with-days-format (:total-cpu-time selected-period-performance-data))]]
      [:tr
	     [:td [:b "Average Computation Speed:"]]
	     [:td {:align :center} (format-maybe-nil #(str (format-speed time-unit, %) " tasks/" time-unit) (:total-mean-speed total-performance-data))]
       [:td {:align :center} (format-maybe-nil #(str (format-speed time-unit, %) " tasks/" time-unit) (:total-mean-speed selected-period-performance-data))]]
	    [:tr
	     [:td [:b "Average Task Duration:"]]
	     [:td {:align :center} (format-maybe-nil fmt/duration-format, (average-task-duration total-performance-data))]
       [:td {:align :center} (format-maybe-nil fmt/duration-format, (average-task-duration selected-period-performance-data))]]
      [:tr
       [:td [:b "Estimated Remaining Duration:"]]
       [:td {:align :center} (format-maybe-nil fmt/duration-with-days-format estimated-duration)]
       [:td {:align :center} (format-maybe-nil fmt/duration-with-days-format estimated-duration-period)]]
      [:tr
       [:td [:b "Estimated End:"]]
       [:td {:align :center} (format-maybe-nil fmt/datetime-format estimated-end)]
       [:td {:align :center} (format-maybe-nil fmt/datetime-format estimated-end-period)]]]]))


(defn- render-exceptions
  [exception-coll]
  [:textarea {:rows 50 :cols 100}
   (string/join "\n\n"
      (map-indexed
        (fn [i [_ e _]]
          (format "Exception #%d:\n%s" (inc i) (escape-html e)))
        exception-coll))])



(def ^:const ^:private rating-period-factor (* 60 1000))
(def ^:private time-unit-options ["sec", "min", "hour", "day", "week"])
(def ^:private default-time-unit "min")


(defn rating-period-options
  [rating-period-map]
  (->> rating-period-map
    (sort-by key)
    (mapv (fn [[period-id, period-ms]] [(format "%.1f mins" (/ period-ms 1000.0 60.0)) period-id]))))


(defn- render-view-settings
  [rating-period-map, rating-period, time-unit]
  (let [rating-period-display (/ (double rating-period) 1000 60)]
    [[:h2 "View Settings"]
     (form-to [:get "/admin"]
      [:table 
        [:tr 
         [:td [:b "Rating Period:"]]
         [:td (drop-down :rating-period (rating-period-options rating-period-map) rating-period)]]
        [:tr 
         [:td [:b "Speed Time Unit:"]]
         [:td (drop-down :time-unit time-unit-options time-unit)]]
        [:tr [:td (submit-button "refresh")]]])]))


(defn- render-admin-panel
  []
  [[:h2 "Admin Server Control"]
   [:h3 "Job Data"]
   (form-to [:post "/admin/clear-finished-jobs"]
     (submit-button {:name "clearfinishedjobs"} "Clear finished jobs"))
   [:h3 "Shutdown"]
   (form-to [:post "/admin/shutdown-workers"]
     [:table
      [:tr [:td (check-box "confirm" false) "confirm"]]
      [:tr [:td (submit-button {:name "shutdown"} "shutdown all workers")]]])])


(defn get-rating-period
  [input-str, rating-period-map]
  (let [periods (keys rating-period-map),
        min-period (reduce min periods),
        max-period (reduce max periods)]
    (or
      (try
        (let [input (Long/parseLong input-str)]
          (when (<= min-period input max-period)
            input))
        (catch Exception e
          nil))
      max-period)))


(defn title
  ([server-node]
    (title server-node, nil))
  ([server-node, page-title]
    (if page-title
      (format "%s - Sputnik (%s)" page-title (v/sputnik-version))
      (format "Sputnik (%s)" (v/sputnik-version)))))


(defn admin-main
  [server-node, {{:keys [rating-period, time-unit]} :params}]
  (html-timing-info
    (title server-node, "Admin")
    (let [rating-period-map (-> server-node server/manager mgmt/rating-period-map)]
      [:body
       [:h1 "Admin"]
       [:p [:b (title server-node)]]
       [:p (link-to "/admin/exceptions" (submit-button "Exceptions")) (link-to "/admin/logs" (submit-button "Server log files")) (link-to "/logout" (submit-button "logout"))]
       
       (let [rating-period (get-rating-period rating-period, rating-period-map),
             time-unit (or ((set time-unit-options) time-unit) default-time-unit),
             rated-worker-coll (worker-data server-node, rating-period),           
	           client-data (client-data server-node),           
             job-data-map (job-data server-node, client-data, rating-period),
             {:keys [total-avg-concurrency, total-avg-concurrency-period] :as summary-data} (server-summary-data rated-worker-coll, client-data)]
         (concat
           (render-server-summary summary-data, rating-period-map, rating-period, time-unit)
           [[:h2 "Nodes"]]
           (render-worker-table rated-worker-coll, rating-period-map, rating-period, time-unit, true)
           (render-worker-table rated-worker-coll, rating-period-map, rating-period, time-unit, false)
           (render-client-table client-data)
           (render-job-information job-data-map, rating-period-map, rating-period, time-unit)
           (render-view-settings rating-period-map, rating-period, time-unit)
           (render-admin-panel)))])))


(defn public-index
  [server-node]
  (html-timing-info
    "Sputnik"
    [:body
     [:h1 "Sputnik!"]
     (link-to "admin" (submit-button "Admin"))
     (let [rating-period-map (-> server-node server/manager mgmt/rating-period-map)
           rating-period (reduce max (keys rating-period-map)),
           time-unit "hour",
           rated-worker-coll (worker-data server-node, rating-period),           
	         client-data (client-data server-node),                      
           summary-data (server-summary-data rated-worker-coll, client-data)]
       (concat (render-server-summary summary-data, rating-period-map, rating-period, time-unit)))]))


(defn exceptions
  [server-node]
  (html-timing-info
    (title server-node, "Exceptions")
    (let [exception-coll (exception-data server-node)]
      [:body
       [:h1 (format "Exceptions (%d)" (count exception-coll))]
       [:p (link-to "/admin" (submit-button "back"))]
       [:p (render-exceptions exception-coll)]])))


(defn- logfile-link
  [filename]
  (let [filename (escape-html filename)]
    [:p (link-to (str "logs/" filename) filename)]))


(def ^:const logfile-pattern #".*\.log(\.\d+)?$")

(defn server-logs-list
  [server-node]
  (html-timing-info
    (title server-node, "Server log files")
    (into
      [:body
       [:h1 "Server log files"]
       (link-to "/admin" (submit-button "back"))]
      (->> (fs/list-files "." logfile-pattern)
         (mapv (comp logfile-link fs/filename))))))


(defn- valid-logfile
  [logname]
  (when (re-matches logfile-pattern logname)
    (first (fs/list-files "." (re-pattern (str (fs/filename logname) "$"))))))


(defn server-log
  [server-node, logname]
  (when-let [logfile (valid-logfile logname)]
    (let [logfile-title (format "Server log file: %s" (fs/filename logfile))]
      (html-timing-info
        (title server-node, logfile-title)
        [:body
         [:h1 logfile-title]
         (link-to "/admin/logs" (submit-button "back"))
         [:p [:textarea {:rows 50 :cols 100} (escape-html (slurp logfile))]]]))))


(defn worker-thread-setup
  [server-node, worker-id, thread-count-str]
  (let [thread-count (try (Long/parseLong thread-count-str) (catch Throwable t nil))]
    (when (and worker-id thread-count)
      (server/worker-thread-setup server-node, worker-id, thread-count))))


(defn clear-finished-jobs
  [server-node]
  (mgmt/remove-finished-job-data (server/manager server-node)))


(defn worker-shutdown
  [server-node, worker-id, {{:keys [worker-id, confirm]} :params}]
  (when (= confirm "true")
    (server/worker-shutdown server-node, worker-id)))


(defn shutdown-workers
  [server-node, {{:keys [confirm]} :params}]
  (when (= confirm "true")
    (server/shutdown-workers server-node)))