; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.control.launch
  (:require
    [sputnik.config.api :as cfg]
    [sputnik.config.lang :as cfg-lang]
    [sputnik.config.meta :as meta]
    [sputnik.control.remote :as remote]
    [sputnik.version :as v]
    [sputnik.tools.file-system :as fs]
    [sputnik.tools.format :as fmt]
    [clojure.java.io :as io]
    [clojure.string :as string]
    [clojure.pprint :as pp])
  (:import
    java.util.Date
    java.text.SimpleDateFormat)
  (:use
    [clojure.options :only [defn+opts]]
    [clojure.stacktrace :only [print-cause-trace]]))


(def properties-url-map
  {:sputnik/worker      "%s-sputnik-worker.cfg",
   :sputnik/server      "%s-sputnik-server.cfg",
   :sputnik/client      "%s-sputnik-client.cfg"
   :sputnik/rest-client "%s-sputnik-rest-client.cfg"})


(defn node-name
  [node]
  (-> node :host :name))

(defn node-type-name
  [node]
  (-> node meta/config-type name))


(defn properties-url
  "Creates the url of the properties file for the given node."
  [node]
  (format (properties-url-map (meta/config-type node)) (node-name node)))


(defn create-config-file
  [{:keys [host, options] :as node}, url]
  (with-open [out (io/writer url)]
    (binding [*out* out]
      (println (format "; sputnik %s -- %s (%s, %s)", (v/sputnik-version), (meta/config-type node), (:name host), (:address host)))
      (println (format "; created %s", (fmt/datetime-format (System/currentTimeMillis))))
      (pp/pprint options)))
  nil)


(defn create-config-file-in-directory
  [node, directory]
  (let [url (io/file directory (properties-url node))]
    (create-config-file node, url)
    ; return url of properties file
    url))



(defn create-configuration-files
  "Creates the configuration files for all nodes of the given cluster in a temporary directory."
  [{:keys [server, client, workers] :as cluster}, directory]
  (create-config-file-in-directory server, directory)
	(create-config-file-in-directory client, directory)
  (doseq [w workers]
    (create-config-file-in-directory w, directory)))





(defn copy-payload-files
  "Copies all files in the given directory."
  [payload-file-list, target-directory]
  ; copy the complete specified payload to the given directory
  (doseq [payload-file payload-file-list]
    (try
      (fs/copy-file (:url payload-file), target-directory)
      (catch Exception e
        (println
          (format "Failed to copy the file \"%s\" to the directory \"%s\" for payload type \"%s\":\n%s"
            (:url payload-file)
            (-> target-directory io/file .getPath)
            (meta/config-type payload-file)
            payload-file))
        (print-cause-trace e)
        (throw e)))))


(defn prepare-payload
  [node-list, payload-list, directory]
  ; check existence of all payloads
  (when (not-every? #(fs/exists? (:url %)) payload-list)
    (throw (Exception. (format "The following payloads do not exist: %s" (->> payload-list (map :url) (remove fs/exists?) (string/join ", "))))))
  ; delete directory if it exists already
  (fs/delete-directory directory)
  (let [payload-dir (fs/create-directory directory)]
    (doseq [node node-list]
      (create-config-file-in-directory node, payload-dir))
    ; copy files (only) to the payload-directory
    (copy-payload-files (filter #(fs/file? (:url %)) payload-list), payload-dir)))


(defn prepare-payload-cluster
  "Creates the cluster properties files and copies all payload files in the given directory."
  [cluster, {:keys [sputnik, files] :as payload}, directory]
  ; delete directory if it exists already
  (fs/delete-directory directory)
  ; create directory 
  (let [payload-dir (fs/create-directory directory)]
    ; create cluster configuration files in that directory
    (create-configuration-files cluster, payload-dir)
    ; copy specified payload in that directory
	  (copy-payload-files (list* sputnik, files), payload-dir)))


(defn+opts deploy-payload
  "Uploads the file-directory and the payload directories to the given hosts."
  [node-list, payload-list, payload-directory | :as options]
  (let [node-list (vec (distinct node-list)),
        target-dir (fs/filename payload-directory)]
    (remote/delete-directory node-list, target-dir, options)
    (remote/upload-directories node-list, payload-directory, "", options)
    (remote/upload-directories node-list, (->> payload-list (map :url) (filter fs/directory?)), target-dir, options))
  nil)

(defn+opts deploy-payload-cluster
  "Uploads the file-directory and the payload directories to the given hosts."
  [{:keys [server, client, workers, communication] :as cluster}, {:keys [sputnik, directories, files] :as payload},
   payload-directory | :as options]
  (deploy-payload
    (list* server, client, workers),
    (let [payload-list (list* sputnik, (concat directories, files))]
      (if (:ssl-enabled communication)
        (conj payload-list (cfg-lang/payload-entity (:keystore communication)) (cfg-lang/payload-entity (:truststore communication)))
        payload-list)),
    payload-directory,
    options)
  nil)


(defn classpath
  [node-type, payload-list]
  (->> payload-list    
    (map (comp fs/filename :url))
    (list* ".")
    (string/join ":")))


(defn log-prefix
  [node]
  (str (node-name node) "." (node-type-name node)))

(defn node-startup-command
  [node]
  ; TODO: might be changed in the future when payload is managed per job
  (format "-e \"(use 'sputnik.satellite.main) (sputnik.satellite.main/-main \\\"-t\\\" \\\"%s\\\" \\\"%s\\\")\"" 
    (node-type-name node), (properties-url node)))


(defn java-command
  [node, payload-list, payload-directory]
  (format "nohup %s -cp %s %s clojure.main %s > %s-output.log 2> %s-error.log &" 
    (or (:sputnik-jvm node) "java")
    (classpath (meta/config-type node), payload-list),
    (or (:sputnik-jvm-opts node) ""),
    (node-startup-command node),
    ; output.log prefix
    (log-prefix node),
    ; error.log prefix
    (log-prefix node)))


(defn escape-double-quotes
  [s]
  (-> s (string/replace #"\\" "\\\\\\\\") (string/replace #"\"" "\\\\\"")))

(defn start-script-name
  [node]
  (format "start-%s-%s.sh" (node-name node) (node-type-name node)))

(defn create-remote-script-command
  [node, start-script]
  (format "echo \"#!/bin/sh\n\n%1$s\" > %2$s && chmod u+x %2$s"
    (escape-double-quotes start-script)
    (start-script-name node)))


(defn+opts start
  "Starts a sputnik satellite on the specified node.
  <verbose>Print information that the node start is triggered.</>"
  [node, payload-list, payload-directory | {verbose false} :as options]
  (let [start-script (java-command node, payload-list, payload-directory),
        root-dir (fs/filename payload-directory)]
    (when verbose
      (println (format "Starting %s on %s." (node-type-name node) (node-name node)))
      (flush))
    (remote/execute-script node
      (remote/unchecked-script 
        (cd ~root-dir)
        ~(create-remote-script-command node, start-script)
        ~start-script)
      options)))


(defn+opts start-nodes
  "Starts the given nodes."
  [node-list, payload-list, payload-directory | :as options]
  (doseq [f (mapv #(future (start %, payload-list, payload-directory, options)) node-list)]
    (deref f)))

(defn+opts start-nodes-cluster
  "Starts all nodes of the cluster.
  <start-client>Specifies if the client process shall be started as well.</start-client>"
  [{:keys [server, client, workers] :as cluster}, {:keys [sputnik, files, directories]  :as payload}, payload-directory | {start-client true, start-server true} :as options]
  (let [payload-list (list* sputnik, files, directories)]; start server first (if needed)
	  (when start-server
	    (start-nodes [server], payload-list, payload-directory, options))
	  ; start workers
	  (start-nodes workers, payload-list, payload-directory, options)
	  ; start client last (if needed)
	  (when start-client
	    (start-nodes [client], payload-list, payload-directory, options))))


(defn+opts kill-all-java
  [cluster]
  (let [{:keys [server, client, workers]} (cfg/cluster-config cluster)]
    (remote/execute-script (list* server, client, workers),
    (remote/script ~"killall -9 -q java || echo \"Nothing to kill!\"")))
  nil)


(defn+opts create-payload-in
  [sputnik-config-url, directory]
  (let [{:syms [cluster, payload] :as cfg} (cfg/load-config sputnik-config-url)]
    (prepare-payload-cluster cluster, payload, directory)))


; TODO: before doing anything in launch, all the configuration data should be checked!
(defn+opts launch-cluster
  [sputnik-config-url | {directory "sputnik-payload"} :as options]
  (let [{:syms [cluster, payload] :as cfg} (cfg/load-config sputnik-config-url)
        cluster (cfg/cluster-config cluster)]
    (prepare-payload-cluster cluster, payload, directory)
    (deploy-payload-cluster cluster, payload, directory, options)
    (start-nodes-cluster cluster, payload, directory, options)
    nil))


(defn keystores-files-only
  [{:keys [keystore, truststore, ssl-enabled] :as communication}]
  (assoc communication
    :keystore (fs/filename keystore)
    :truststore (fs/filename truststore)))


(defn+opts launch-nodes
  "Starts the given workers that shal be connected to the given server using the specified communication parameters.
  The payload will end up in the current working directory of the worker which is the given payload-directory.
  Note that the payload-directory is deleted if it already exists."
  [node-list, {:keys [keystore, truststore, ssl-enabled] :as communication}, payload-list, payload-directory | :as options]
  (try
    (let [node-list (mapv #(cfg/node-config (cfg/use-communication (keystores-files-only communication), %)) node-list),
          payload-list (if ssl-enabled (conj payload-list (cfg-lang/payload-entity keystore) (cfg-lang/payload-entity truststore)) payload-list)]
      (prepare-payload node-list, payload-list, payload-directory)
      (deploy-payload  node-list, payload-list, payload-directory, options)
      (start-nodes     node-list, payload-list, payload-directory, options)
      nil)
    (catch Exception e
      (or
        (when-let [e (some->> e .getCause .getCause)]
          (when (instance? clojure.lang.ExceptionInfo e)
            (when (-> e ex-data :object :cause ex-data :object :type (= :pallet/ssh-connection-failure))
              (throw (RuntimeException. "SSH connection failed! Did you forget to load your private ssh key via ssh-add?" e)))))
        (throw e)))))