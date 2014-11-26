; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-control.launch
  (:require
    [jppfnik-control.config :as config]
    [jppfnik-control.properties :as props]
    [jppfnik-control.validation :as valid]
    [jppfnik-control.remote :as remote]
    [jppfnik-control.defaults :as defaults]
    [jppfnik-control.file-system :as fs]
    [clojure.java.io :as io]
    [clojure.string :as string])
  (:import
    java.util.Date
    java.text.SimpleDateFormat)
  (:use
    [clojure.options :only [defn+opts]]
    [clojure.stacktrace :only [print-cause-trace]]))



(def properties-url-map
  {:sputnik/node   "jppfnik-node-%s.properties",
   :sputnik/server "jppfnik-server-%s.properties",
   :sputnik/client "jppfnik-client-%s.properties"})


(defn properties-url
  "Creates the url of the properties file for the given node."
  [node]
  (format (properties-url-map (type node)) (-> node :host :name)))


(defn create-properties-file
  [{:keys [host, jppf-options] :as node}, directory]
  (let [properties-url (io/file directory (properties-url node))] 
    (props/write-properties
	    properties-url,
	   (props/map->properties-map jppf-options),
	    ; TODO: figure out how to make the version number accessible from code
	    :comment 
	    (format "jppfnik-control %s -- %s (%s, %s)" 
	      "0.0.1" (type node) (:name host) (:address host)))
    ; return url of properties file(create-configuration-files cluster, directory)
    properties-url))


(defn create-ssl-properties-file
  [{:keys [keystore, keystore-password, truststore, truststore-password, config, ssl-directory, jppf-options] :as ssl}, directory]
  (let [properties-url (io/file directory config)] 
    (props/write-properties
      properties-url,
      (merge
        (props/map->properties-map jppf-options)
        (props/map->properties-map
          {
            :jppf.ssl
            {
              :keystore
	            {
	              :file (->> keystore (io/file ssl-directory) .getPath)
	              :password keystore-password
	            } 
             
              :truststore
	            {
	              :file (->> truststore (io/file ssl-directory) .getPath)
	              :password truststore-password
	            }
            }
          }))
      ; TODO: figure out how to make the version number accessible from code
      :comment (format "jppfnik-control %s -- SSL" "0.0.1"))))


(defn create-configuration-files
  "Creates the configuration files for all nodes of the given cluster in a temporary directory."
  [{:keys [server, client, nodes, ssl] :as cluster}, directory]
  (when ssl (create-ssl-properties-file ssl, directory))  
  (create-properties-file server, directory)
	(create-properties-file client, directory)
  (doseq [node nodes]
    (create-properties-file node, directory)))





(defn copy-payload-files
  "Copies all files in the given directory."
  [{:keys [jppfnik-client, jppfnik-node, jppfnik-server, jars, files]  :as payload}, target-directory]
  ; copy the complete specified payload to the given directory
  (doseq [file-payload (list* jppfnik-client, jppfnik-node, jppfnik-server, (concat jars, files))]
    (try
      (fs/copy-file (:url file-payload), target-directory)
      (catch Exception e
        (println
          (format "Failed to copy the file \"%s\" to the directory \"%s\" for payload type \"%s\":\n%s"
            (:url file-payload)
            (-> target-directory io/file .getPath)
            (type file-payload)
            file-payload))
        (print-cause-trace e)
        (throw e)))))


(defn prepare-payload
  "Creates the cluster properties files and copies all payload files in the given directory."
  [cluster, payload, directory]
  ; delete directory if it exists already
  (fs/delete-directory directory)
  ; create directory 
  (let [payload-dir (fs/create-directory directory)]
    ; create cluster configuration files in that directory
    (create-configuration-files cluster, payload-dir)
    ; copy specified payload in that directory
	  (copy-payload-files payload, payload-dir)))


(defn+opts deploy-payload
  "Uploads the file-directory and the payload directories to the given hosts."
  [{:keys [server, client, nodes, user, ssl] :as cluster}, 
   {:keys [directories] :as payload},
   payload-directory
   | {deploy-client true}]
  (let [hosts (->> (cond->> nodes deploy-client (list* client) true (list* server)) (map :host) distinct vec),
        target-dir (fs/filename payload-directory)]
    (remote/delete-directory hosts, user, target-dir)
    (remote/upload-directories hosts, user, payload-directory, "")
    (remote/upload-directories hosts, user, (concat (map :url directories) (when ssl [(:ssl-directory ssl)])), target-dir))
  nil)


(defn classpath
  [node-type, {:keys [jppfnik-client, jppfnik-node, jppfnik-server, jars, files, directories]  :as payload}]
  (->> (concat jars, files, directories) 
    (list*
      (case node-type
        :sputnik/server jppfnik-server
        :sputnik/node   jppfnik-node
        :sputnik/client jppfnik-client))
    (map (comp fs/filename :url))
    (string/join ":")))

(def jppfnik-node-mains
  {
   :sputnik/server "jppfnik-server"
   :sputnik/node   "jppfnik-node"
   :sputnik/client "jppfnik-client"
  })


(defn node-startup-command
  [node, {:keys [job-fn, job-args, result-fn, async] :as work}]
  (case (type node)
    (:sputnik/server :sputnik/node) 
      (format "-e \"(use '%1$s.main) (%1$s.main/-main)\"" (get jppfnik-node-mains (type node)))
      ; TODO: work related arguments need to be added for the client!!!
    :sputnik/client 
      (format "-e \"(use '%1$s.main) (%1$s.main/-main \\\"-j\\\" \\\"%2$s\\\" \\\"-J\\\" \\\"%3$s\\\" \\\"-r\\\" \\\"%4$s\\\")\"" 
        (get jppfnik-node-mains (type node))
        job-fn
        job-args
        result-fn)))


(defn java-command
  [node, payload, payload-directory, work]
  (format "nohup %s -cp %s -Djppf.config=%s -Dlog4j.configuration=log4j.properties -Djava.util.logging.config.file=logging.properties %s clojure.main %s > %s-output.log 2> %s-error.log &"
    (or (get-in node [:host :jvm]) "java")
    (classpath (type node), payload)
    (properties-url node)
    (or (:sputnik-jvm-opts node) "")
    (node-startup-command node, work)
    ; output.log prefix
    (get jppfnik-node-mains (type node))
    ; error.log prefix
    (get jppfnik-node-mains (type node))))



(defn escape-double-quotes
  [s]
  (-> s (string/replace #"\\" "\\\\\\\\") (string/replace #"\"" "\\\\\"")))

(defn start-script-name
  [node]
  (format "start-%s.sh" (-> node :host :name)))

(defn create-remote-script-command
  [node, start-script]
  (format "echo \"#!/bin/sh\n\n%1$s\" > %2$s && chmod u+x %2$s"
    (escape-double-quotes start-script)
    (start-script-name node)))


(defn+opts start-nodes
  "Starts all nodes of the cluster.
  <start-client>Specifies if the client process shall be started as well.</start-client>"
  [work, {:keys [server, client, nodes, user] :as cluster}, payload, payload-directory | {start-client true}]
  ; start order: server, nodes, client
  (doseq [node (concat [server] nodes (when start-client [client]))] 
    (let [start-script (java-command node, payload, payload-directory, work),
          root-dir (fs/filename payload-directory)]
      (remote/execute-script (:host node), user
        (remote/unchecked-script 
          (cd ~root-dir)
          ~(create-remote-script-command node, start-script)
          ~start-script)))))



(defn+opts launch
  [sputnik-config-url | {directory "sputnik-payload"} :as options]
  (let [{:syms [cluster, payload, work] :as cfg} (config/load-config sputnik-config-url)]
    (prepare-payload cluster, payload, directory)
    (deploy-payload cluster, payload, directory, options)
    (start-nodes work, cluster, payload, directory, options)
    nil))