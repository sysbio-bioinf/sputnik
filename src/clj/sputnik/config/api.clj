; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.config.api
  (:require
    [clojure.java.io :as io]
    [clojure.string :as string]
    [clojure.options :refer [defn+opts]]
    [sputnik.tools.file-system :as fs]
    [sputnik.config.meta :as meta]
    [sputnik.config.options :as opt]
    sputnik.config.lang)
   (:import
    java.io.FileNotFoundException))


(def ^:dynamic ^:private *possible-path-prefix* nil)

(defn load-config-file
  [url]
  (if-let [f (fs/find-file url :possible-directory *possible-path-prefix*)]
    (let [url-file (io/file url),
          path (.getParent url-file)]
      (binding [*possible-path-prefix* (if path
                                         (if (.isAbsolute url-file)
                                           path
                                           (.getPath (io/file *possible-path-prefix* path)))
                                         *possible-path-prefix*)] 
        (-> f io/reader load-reader)))
    (throw (FileNotFoundException. (format "File \"%s\" not found!" url)))))


(defn+opts load-config
  "Loads a sputnik configuration file and returns all define configuration data in map from symbol to value."
  [url | {namespace "config", add-config-id false}]
  (locking load-config
    (let [config-ns-symb (gensym namespace)
	        config-ns (create-ns config-ns-symb)
          config-words (meta/config-commands (the-ns 'sputnik.config.lang))]
	    (binding [*ns* config-ns]
	      (clojure.core/refer-clojure)        
	      (require
         ['sputnik.config.api :refer ['load-config-file]]
         ['sputnik.config.lang :refer config-words])
	      (try (load-config-file url)
	        (catch Exception e
             (remove-ns config-ns-symb)
	           (throw (Exception. (format "Error loading configuration \"%s\"!" url) e)))))
	    (let [config-map (->> config-ns 
	                       ns-publics
	                       (filter #(-> % val meta/config-type?))
	                       (reduce 
	                         (fn [m, [k v]] 
	                           (assoc! m (meta/with-config-type (meta/config-type v) k) (var-get v))) 
	                         (transient {}))
	                       persistent!)]
	      (remove-ns config-ns-symb)
	      (cond->> config-map
          add-config-id
          (reduce-kv
            (fn [res, k, v]
              (assoc res k (assoc v :sputnik/config-id k)))
            {}))))))


(defn quote-creation-fn
  [config-data]
  (if (#{:sputnik/job :sputnik/client :sputnik/cluster} (meta/config-type config-data))
    (meta/prewalk
      (fn [x]
        (if (and (instance? clojure.lang.MapEntry x) (= (key x) :creation-fn))
          [(key x) `(quote ~(val x))]
          x))
      config-data)
    config-data))


(defn save-config
  "Save the given Sputnik configuration to a file as Clojure data."
  [url, config-map]
  (with-open [f (io/writer url)]
    (binding [*out* f,
              ;*print-dup* true,
              *print-meta* true]
      (doseq [[symbol config-data] config-map]
        ; no pprint since it does not write the metadata of the config
        (prn (list 'def (meta/with-config-type (meta/config-type config-data) symbol) (quote-creation-fn (dissoc config-data :sputnik/config-id))))))))


(defn select-configs
  "Returns the config values of the given type."
  [config-type, config-map]
  (->> config-map    
    (filter #(= (meta/config-type (key %)) config-type))
    (mapv val)))


(defn use-communication
  "Add communication options to the given node."
  [comm, node]
  (update-in node [:options] merge comm))


(defn use-server
  "Add server information to the given node."
  [{{server-address :address} :host,  {:keys [sputnik-port]} :options, :as server}, node]
  (update-in node [:options]
    #(-> %
       (dissoc :hostname)
       (merge
         {:server-hostname server-address,
          :server-port sputnik-port}))))


(defn prepare-client-job
  "For the given client the job configuration is transformed to the needed :job-setup-fn and :job-setup-args key value pairs-"
  [{{:keys [job]} :options, :as client}]
  (when-not job
    (throw (IllegalArgumentException. (str "The selected client needs a job! Otherwise it is pointless to launch a client! Client: " (pr-str client)))))
  (if-let [creation-fn (eval (:creation-fn job))]
    (let [{:keys [job-setup-fn, job-setup-args] :as params} (creation-fn job)]
      (if (and job-setup-fn job-setup-args)
        (-> client
          (update-in [:options] dissoc :job)
          (update-in [:options] assoc :job-setup-fn job-setup-fn, :job-setup-args job-setup-args))
        (let [missing (filter #(not (contains? params %)) [:job-setup-fn, :job-setup-args])]
          (throw
            (IllegalArgumentException.
              (str "Creation function (:creation-fn) of the given client's job returned a configuration with the keys: "
                (string/join ", " missing)))))))
    (throw (IllegalArgumentException. "The job of the selected client has no creation function (:creation-fn)!"))))


(def ^:const ^:private common-option-categories [:node :communication :logging])

(defn node-config
  "Returns the configuration for the given node. Intended for serialization purposes.
  Counterpart of `sputnik.config.api/node-options`."
  [node]
  (let [node (if (= (meta/config-type node) :sputnik/client) (prepare-client-job node) node)]
    (update-in node [:options] #(opt/=>config (conj common-option-categories (meta/config-type node)), %))))


(defn cluster-config
  [{:keys [communication, server] :as cluster}]
  (let [communication! (partial use-communication communication),
        server! (partial use-server server)]
    (-> cluster
      (update-in [:server]  (comp node-config communication!))
      (update-in [:client]  (comp node-config communication! server!))
      (update-in [:workers] #(mapv (comp node-config communication! server!) %)))))


(defn- =>node-type
  [node-type]
  (keyword "sputnik" (name node-type)))


(defn node-options
  "Extracts the nodes options from the given config returning a flat map.
  Counterpart of `sputnik.config.api/node-config`."
  [node-type, node-config]
  (opt/config=> (conj common-option-categories (=>node-type node-type)), node-config))