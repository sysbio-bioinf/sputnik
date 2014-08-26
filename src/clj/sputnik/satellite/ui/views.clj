; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.ui.views
  (:require
    [sputnik.satellite.node :as node]
    [sputnik.satellite.role-based-messages :as r]
    [sputnik.satellite.server :as server]
    [sputnik.satellite.server.management :as mgmt]
    [sputnik.tools.format :as fmt]
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


(defmacro html-timing-info
  "Adds generation duration info to the generated html document."
  [& body]
 `(let [start-time# (System/nanoTime),
        result# (html ~@body)
        end-time# (System/nanoTime)]    
    (html5
      result#
      (html
        [:p {:style "position:fixed;right:1em;bottom:0%;"}
         [:i (format "Page generation: %,d ms elapsed." (quot (- end-time# start-time#) 1000000))]]))))


(defn worker-data
  ([server-node]
    (worker-data server-node, (* 15 60 1000)))
  ([server-node, rating-period]
    (let [mgr (server/manager server-node),
          rated-workers (mgmt/rated-workers mgr rating-period, false)]
      (->
        (mapv
          (fn [{:keys [worker-id, disconnect-time] :as worker-data}]
            (let [worker-node (node/get-remote-node server-node worker-id)]
              (assoc worker-data
                :last-active (when worker-node (node/last-activity worker-node)))))
          rated-workers)
        (with-meta (meta rated-workers))))))

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
  [worker-data, client-data]
  (let [data (merge
               {:worker-count (count worker-data)}
               (aggregate-properties worker-data, "total-",
                 [:avg-concurrency nil maybe-add],
                 [:avg-concurrency-period nil maybe-add],
                 [:task-completion nil maybe-add],
                 [:task-completion-period nil maybe-add],
                 [[:total-duration [:task-completion :avg-duration]] nil maybe-multiply-add],
                 [[:total-duration-period [:task-completion-period :avg-duration-period]] nil maybe-multiply-add],
                 [:avg-computation-speed nil maybe-add],
                 [:avg-computation-speed-period nil maybe-add],                 
                 [:tasks-in-interval nil maybe-add],
                 [:tasks-in-interval-period nil maybe-add],
                 [[:total-avg-efficiency [:avg-efficiency :tasks-in-interval]] nil maybe-multiply-add]
                 [[:total-avg-efficiency-period [:avg-efficiency-period :tasks-in-interval-period]] nil maybe-multiply-add]
                 [:cputime-sum nil maybe-add]
                 [:cputime-sum-period nil maybe-add]
                 [:avg-parallelism nil maybe-add]
                 [:avg-parallelism-period nil maybe-add])
               {:client-count (count client-data)}
               (aggregate-properties client-data, "total-"
				         [:task-count nil maybe-add],
                 [:finished-task-count nil maybe-add],
                 [:job-count nil maybe-add]))]
     (-> data
       (update-in [:total-avg-efficiency] #(when % (/ % (:total-tasks-in-interval data))))
       (update-in [:total-avg-efficiency-period] #(when % (/ % (:total-tasks-in-interval-period data)))))))


(defn client-data
  [server-node]
  (let [mgr (server/manager server-node)]
    (->> (mgmt/client-data-map mgr)
      (reduce-kv
        (fn [result, client-id, {:keys [jobs] :as client-data}]
          (let [client-node (node/get-remote-node server-node client-id)]
            (let [task-count (reduce-kv #(+ %1 (:task-count %3)) 0 jobs),
                  finished-task-count (reduce-kv #(+ %1 (:finished-task-count %3)) 0 jobs)]
              (conj! result
                (assoc client-data
                  :client-id client-id,
                  :job-count (count jobs),
	                :task-count task-count,
	                :finished-task-count finished-task-count,
                  :progress (/ (double finished-task-count) task-count),
                  :last-active (when client-node (node/last-activity client-node)))))))
        (transient []))
      persistent!)))

(defn exception-data
  [server-node]
  (for [[_ {:keys [jobs]}] (-> server-node server/manager mgmt/client-data-map), [_ {:keys [exceptions]}] jobs, e exceptions]
    e))

(defn estimated-duration
  [task-count, finished-task-count, avg-duration, avg-concurrency]
  (when (and avg-duration (pos? avg-duration) avg-concurrency)
    (/
      (* (- task-count finished-task-count) (double avg-duration))
      avg-concurrency)))

(defn job-data
  [client-data, rating-period]
  (let [now (System/currentTimeMillis)] 
    (->> client-data
	    (reduce
		    (fn [result-map, {:keys [client-id, client-info, jobs] :as client-data}]
		      (assoc! result-map
	          (select-keys client-data [:client-id :client-info :job-count])
	          (->> jobs
	            (reduce-kv
	              (fn [result-coll, job-id, {:keys [task-count, finished-task-count, durations, exceptions] :as job-data}]
	                (conj! result-coll
	                  (let [{:keys [avg-duration, avg-concurrency, avg-computation-speed, avg-efficiency, cputime-sum, avg-parallelism, interval-begin, interval-end]}
                            (mgmt/computation-statistics durations), 
                          {avg-duration-period :avg-duration, avg-concurrency-period :avg-concurrency, avg-computation-speed-period :avg-computation-speed,
                           avg-efficiency-period :avg-efficiency, cputime-sum-period :cputime-sum, avg-parallelism-period :avg-parallelism}
                            (mgmt/computation-statistics durations (- now rating-period), now),
	                        est-duration (estimated-duration task-count, finished-task-count, avg-duration, avg-concurrency),
	                        est-duration-period (estimated-duration task-count, finished-task-count, avg-duration, avg-concurrency-period)]
	                    (assoc (select-keys job-data [:task-count, :finished-task-count, :finished])
	                      :job-id job-id,
		                    :progress (/ (double finished-task-count) task-count),
                        :job-duration (when (and interval-end interval-begin) (- interval-end interval-begin)),
		                    :avg-duration avg-duration,
                        :avg-duration-period avg-duration-period,
                        :avg-concurrency avg-concurrency,
                        :avg-concurrency-period avg-concurrency-period,
                        :avg-computation-speed avg-computation-speed,
                        :avg-computation-speed-period avg-computation-speed-period,
		                    :estimated-duration est-duration,
	                      :estimated-duration-period est-duration-period,
                        :estimated-end (when est-duration (+ now est-duration)),
                        :estimated-end-period (when est-duration-period (+ now est-duration-period))
                        :avg-efficiency avg-efficiency,
                        :avg-efficiency-period avg-efficiency-period,
                        :cputime-sum cputime-sum,
                        :cputime-sum-period cputime-sum-period,
                        :avg-parallelism avg-parallelism,
                        :avg-parallelism-period avg-parallelism-period,
                        :exception-count (count exceptions)))))
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
  [{:keys [thread-count, assigned-task-count, task-completion, task-completion-period, worker-info, worker-id, last-active,
           avg-concurrency, avg-concurrency-period, avg-duration, avg-duration-period, avg-computation-speed, avg-computation-speed-period,
           avg-parallelism, avg-parallelism-period, avg-efficiency, avg-efficiency-period,
           connected, disconnect-time]},
   time-unit]
  (table-row 
    worker-info, thread-count, assigned-task-count,
    (format-maybe-nil-pair #(format "%.2f" %), task-completion, task-completion-period),
    (format-maybe-nil-pair #(format "%3.2f" %), avg-concurrency, avg-concurrency-period),
    (format-maybe-nil-pair #(format "%3.2f" %), avg-parallelism, avg-parallelism-period),
    (format-maybe-nil-pair #(format "%3.2f" %), avg-efficiency, avg-efficiency-period),
    (format-maybe-nil-pair fmt/duration-format avg-duration, avg-duration-period),
    (format-maybe-nil-pair #(format-speed time-unit, %), avg-computation-speed, avg-computation-speed-period),    
    connected,
    (cond
      last-active (fmt/datetime-format last-active)
      disconnect-time [:i (fmt/datetime-format disconnect-time)]
      :else "N/A"),
    (form-to [:post (format "/admin/worker/%s" worker-id)]
      [:table 
        [:tr 
         [:td "#Threads:"]
         [:td (text-field {:maxlength 3 :size 2 :disabled (when-not connected true)} :thread-count thread-count)]
         [:td (submit-button {:name "change" :disabled (when-not connected true)} "change")]]])))

(defn- render-worker-table
  [worker-data-coll, rating-period, time-unit]
  (let [period-str (format "<br>(%.1f min)" (/ (double rating-period) 1000 60))]
    [[:h3 (format "Worker Nodes (%d)" (count worker-data-coll))]
	   (into [:table {:border 1}
	           (table-head "Worker" "#Threads" "#Assigned Tasks"
               (str "Task Completion" period-str), (str "Concurrency" period-str), (str "Parallelism" period-str), (str "Efficiency" period-str),
	             (str "AVG[duration]" period-str), (str "AVG[Speed]", period-str, "<br>tasks/", time-unit), 
              "Connected?" "Last Active" "Setup")]
	     (->> worker-data-coll (sort-by :worker-info) (map #(render-worker-row %, time-unit))))]))


(defn- render-client-table
  [client-data-coll]
  [[:h3 (format "Client Nodes (%d)" (count client-data-coll))]
   (into [:table {:border 1} (table-head "Client" "#Jobs" "#Tasks" "#Finished Tasks" "Progress" "Connected?" "Last Active")]
     (map
       (fn [{:keys [job-count, task-count, finished-task-count, progress client-info, last-active, connected, disconnect-time]}]
         (table-row client-info, job-count, task-count, finished-task-count,
                    (format "%5.2f%%" (* 100 progress)) connected, 
                    (cond
								      last-active (fmt/datetime-format last-active)
								      disconnect-time [:i (fmt/datetime-format disconnect-time)]
								      :else "N/A")))
       (sort-by :client-info client-data-coll)))])


(defn- format-job-measure
  [both?, fmt, value1, value2]
  (if both?
    (format-maybe-nil-pair fmt, value1, value2)
    (format-maybe-nil fmt, value1)))


(defn- render-job-information
  [job-data-map, rating-period, time-unit]
  (let [job-count (reduce-kv #(+ %1 (count %3)) 0 job-data-map)
        period-str (format "<br>(%.1f min)" (/ (double rating-period) 1000 60))]
    [[:h3 (format "Jobs (%d)" job-count)]
     (into [:table {:border 1}
            (table-head "Job ID" "#Tasks" "#Finished Tasks" "Progress" "#Exceptions" "Finished" "Current Duration"
              (str "Concurrency" period-str),
              (str "Parallelism" period-str),
              (str "Efficiency" period-str),
              (str "AVG[Duration]" period-str),
              (str "AVG[Speed]", period-str, "<br>tasks/" time-unit),
              (str "CPU Time" period-str),
              (str "Estimated Duration" period-str)
              (str "Estimated End" period-str))]
       (apply concat
         (for [[{:keys [client-info job-count]} jobs] (sort-by (comp :client-info first) job-data-map)]
	         (into
	           [[:tr [:td {:colspan 15, :align :center, :style "color:navy"} [:b (format "%s (#jobs = %d)" client-info job-count)]]]]
	           (for [{:keys [job-id, task-count, finished-task-count, finished, progress, 
	                         avg-duration, avg-duration-period, avg-concurrency, avg-concurrency-period,
                           avg-computation-speed, avg-computation-speed-period, job-duration
                           estimated-duration, estimated-duration-period, estimated-end, estimated-end-period,
                           avg-efficiency, avg-efficiency-period, cputime-sum, cputime-sum-period, avg-parallelism, avg-parallelism-period,
                           exception-count]}
                   (sort-by :job-id jobs)
                   :let [in-progress? (not finished)]]
	             (table-row
	               job-id, task-count, finished-task-count, (format "%5.2f%%" (* 100 progress)), exception-count, finished,
                 (format-maybe-nil fmt/duration-with-days-format, job-duration),
                 (format-job-measure in-progress?, #(format "%3.2f" %), avg-concurrency, avg-concurrency-period),
                 (format-job-measure in-progress?, #(format "%3.2f" %), avg-parallelism, avg-parallelism-period),
                 (format-job-measure in-progress?, #(format "%3.2f" %), avg-efficiency, avg-efficiency-period),
	               (format-job-measure in-progress?, fmt/duration-format, avg-duration, avg-duration-period),
                 (format-job-measure in-progress?, #(format-speed time-unit, %), avg-computation-speed, avg-computation-speed-period),
                 (format-job-measure in-progress?, fmt/duration-with-days-format, cputime-sum, cputime-sum-period),
                 (format-job-measure in-progress?, fmt/duration-with-days-format, estimated-duration, estimated-duration-period),
                 (format-job-measure in-progress?, fmt/datetime-format, estimated-end, estimated-end-period)))))))]))


(defn real-number?
  [x]
  (and x (not (or (Double/isNaN x) (Double/isInfinite x)))))


(defn- render-server-summary
  [{:keys [total-avg-concurrency, total-avg-concurrency-period, total-task-completion, total-task-completion-period,
           total-duration, total-duration-period, total-avg-computation-speed, total-avg-computation-speed-period,
           total-avg-efficiency,total-avg-efficiency-period, total-cputime-sum, total-cputime-sum-period,
           total-avg-parallelism, total-avg-parallelism-period,
           total-job-count, total-task-count, total-finished-task-count, worker-count, client-count]},
   rating-period, time-unit]
  (let [period-str (format "Last %.1f min" (/ (double rating-period) 1000 60)),
        now (System/currentTimeMillis),
        est-duration
          (when (and total-task-count total-finished-task-count total-avg-computation-speed)
            (/ (- total-task-count total-finished-task-count) total-avg-computation-speed)),
        est-duration-period
          (when (and total-task-count total-finished-task-count total-avg-computation-speed-period)
            (/ (- total-task-count total-finished-task-count) total-avg-computation-speed-period)),
        est-end
          (when est-duration
            (+ now est-duration)),
        est-end-period
          (when est-duration-period
            (+ now est-duration-period)),
        total-progress
          (when (and total-finished-task-count total-task-count)
            (/ (* 100.0 total-finished-task-count) total-task-count))]
    [[:h2 "Server Summary"]
     [:table {:border 1}
      [:tr
       [:td [:b "Worker Count:"]]
       [:td {:align :center} (format-maybe-nil #(format "%d" (long %)) worker-count)]]
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
       [:td {:align :center} (format-maybe-nil #(format "%3.2f%%" %) total-progress)]]]
     [:p]
	   [:table {:border 1}
      [:tr [:th] [:th "Total"] [:th period-str]]
	    [:tr
	     [:td [:b "Average Concurrency:"]]
	     [:td {:align :center} (format-maybe-nil #(format "%5.2f" %), total-avg-concurrency)]
       [:td {:align :center} (format-maybe-nil #(format "%5.2f" %), total-avg-concurrency-period)]]
      [:tr
	     [:td [:b "Average Parallelism:"]]
	     [:td {:align :center} (format-maybe-nil #(format "%5.2f" %), total-avg-parallelism)]
       [:td {:align :center} (format-maybe-nil #(format "%5.2f" %), total-avg-parallelism-period)]]
      [:tr
	     [:td [:b "Average Efficiency:"]]
	     [:td {:align :center} (format-maybe-nil #(format "%5.2f" %), total-avg-efficiency)]
       [:td {:align :center} (format-maybe-nil #(format "%5.2f" %), total-avg-efficiency-period)]]
	    [:tr
	     [:td [:b "Task Completion:"]]
	     [:td {:align :center} (format-maybe-nil #(format "%.2f" %) total-task-completion)]
       [:td {:align :center} (format-maybe-nil #(format "%.2f" %) total-task-completion-period)]]
	    [:tr
	     [:td [:b "Task Duration Sum:"]]
	     [:td {:align :center} (format-maybe-nil fmt/duration-with-days-format total-duration)]
       [:td {:align :center} (format-maybe-nil fmt/duration-with-days-format total-duration-period)]]
      [:tr
	     [:td [:b "CPU Time Sum:"]]
	     [:td {:align :center} (format-maybe-nil fmt/duration-with-days-format total-cputime-sum)]
       [:td {:align :center} (format-maybe-nil fmt/duration-with-days-format total-cputime-sum-period)]]
      [:tr
	     [:td [:b "Average Computation Speed:"]]
	     [:td {:align :center} (format-maybe-nil #(str (format-speed time-unit, %) " tasks/" time-unit) total-avg-computation-speed)]
       [:td {:align :center} (format-maybe-nil #(str (format-speed time-unit, %) " tasks/" time-unit) total-avg-computation-speed-period)]]
	    [:tr
	     [:td [:b "Average Task Duration:"]]
	     [:td {:align :center} (format-maybe-nil fmt/duration-format, (when (and (real-number? total-duration) (real-number? total-task-completion))
                                                                     (quot total-duration total-task-completion)))]
       [:td {:align :center} (format-maybe-nil fmt/duration-format, (when (and (real-number? total-duration-period) (real-number? total-task-completion-period))
                                                                      (quot total-duration-period total-task-completion-period)) )]]
      [:tr
       [:td [:b "Estimated Remaining Duration:"]]
       [:td {:align :center} (format-maybe-nil fmt/duration-with-days-format est-duration)]
       [:td {:align :center} (format-maybe-nil fmt/duration-with-days-format est-duration-period)]]
      [:tr
       [:td [:b "Estimated End:"]]
       [:td {:align :center} (format-maybe-nil fmt/datetime-format est-end)]
       [:td {:align :center} (format-maybe-nil fmt/datetime-format est-end-period)]]]]))


(defn- render-exceptions
  [exception-coll]
  [:textarea {:rows 30 :cols 100}
   (string/join "\n\n"
      (map-indexed
        (fn [i [_ e _]]
          (format "Exception #%d:\n%s" (inc i) e))
        exception-coll))])



(def ^:const ^:private rating-period-factor (* 60 1000))
(def ^:private time-unit-options ["sec", "min", "hour", "day", "week"])
(def ^:private default-time-unit "min")

(defn- render-view-settings
  [rating-period, time-unit]
  (let [rating-period-display (/ (double rating-period) 1000 60)]
    [[:h3 "View Settings"]
     (form-to [:get "/admin"]
      [:table 
        [:tr 
         [:td [:b "Rating Period:"]]
         [:td (text-field {:maxlength 7, :size 5, :style "text-align:right"} :rating-period (format "%.1f" rating-period-display)) " min"]]
        [:tr 
         [:td [:b "Speed Time Unit:"]]
         [:td (drop-down :time-unit time-unit-options time-unit)]]
        [:tr [:td (submit-button "refresh")]]])]))


(defn- render-admin-panel
  []
  [[:h2 "Admin Server Control"]
   (form-to [:post "/admin/clear-finished-jobs"]
     (submit-button {:name "clearfinishedjobs"} "Clear finished jobs"))
   (form-to [:post "/admin/export-worker-task-durations"]
     (submit-button {:name "exportworkertaskdurations"} "Export worker task durations"))])


(defn get-rating-period
  [input-str, default]
  (->
    (try
      (let [input (Double/parseDouble input-str)]
        (when (<= 0.1 input) input))
      (catch Exception e nil))
    (or default)
    (* 10)
    long
    (* rating-period-factor)
    (quot 10)))


(defn admin-main
  [server-node, {{:keys [rating-period, time-unit]} :params}]
  (html-timing-info
    [:body
     [:h1 "Admin "]
     [:p (link-to "/admin/exceptions" (submit-button "Exceptions")) (link-to "/logout" (submit-button "logout"))]
	   [:p (node/node-info server-node)]
	   [:h2 "Connected Nodes"]
	   (let [rating-period (get-rating-period rating-period, 15),
           time-unit (or ((set time-unit-options) time-unit) default-time-unit),
           worker-data (worker-data server-node, rating-period),           
	         client-data (client-data server-node),           
           job-data-map (job-data client-data, rating-period),
           {:keys [total-avg-concurrency, total-avg-concurrency-period] :as summary-data} (server-summary-data worker-data, client-data)]
	     (concat
	       (render-worker-table worker-data, rating-period, time-unit)
	       (render-client-table client-data)
         (render-job-information job-data-map, rating-period, time-unit)
         (render-server-summary summary-data, rating-period, time-unit)
         (render-view-settings rating-period, time-unit)
         (render-admin-panel)))]))


(defn public-index
  [server-node]
  (html-timing-info
    [:body
     [:h1 "Sputnik!"]
     (link-to "admin" (submit-button "Admin"))
     (let [rating-period (* 30 rating-period-factor),
           time-unit "hour",
           worker-data (worker-data server-node, rating-period),           
	         client-data (client-data server-node),                      
           summary-data (server-summary-data worker-data, client-data)]
       (concat (render-server-summary summary-data, rating-period, time-unit)))]))


(defn exceptions
  [server-node]
  (html-timing-info
    (let [exception-coll (exception-data server-node)]
      [:body
       [:h1 (format "Exceptions (%d)" (count exception-coll))]
       [:p (link-to "/admin" (submit-button "back"))]
       [:p (render-exceptions exception-coll)]])))


(defn worker-thread-setup
  [server-node, worker-id-str, thread-count-str]
  (let [worker-id (try (UUID/fromString worker-id-str) (catch Throwable t nil)),
        thread-count (try (Long/parseLong thread-count-str) (catch Throwable t nil))]
    (when (and worker-id thread-count)
      (server/worker-thread-setup server-node, worker-id, thread-count))))


(defn clear-finished-jobs
  [server-node]
  (mgmt/remove-finished-job-data (server/manager server-node)))


(defn export-worker-task-durations
  [server-node]
  (mgmt/export-worker-task-durations "worker-task-durations.map" (server/manager server-node)))