; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.main
  (:require
    [sputnik.satellite.server :as server]
    [sputnik.satellite.worker :as worker]
    [sputnik.satellite.client :as client]
    [sputnik.satellite.rest-client :as rest-client]
    [sputnik.satellite.ui.launch :as ui]
    [sputnik.tools.threads :as threads]
    [sputnik.tools.resolve :as resolve]
    [sputnik.tools.logging :as log]
    [sputnik.tools.file-system :as fs]
    [sputnik.version :as v]
    [sputnik.config.api :as cfg]
    [clojure.options :refer [->option-map]]
    [clojure.string :as string]
    [clojure.java.io :as io]
    [clojure.pprint :refer [pprint]]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.tools.logging :refer [debug, error, *force*]]
    [clojure.tools.cli :as cli]
    [txload.core :as tx]))


(defn launch-web-ui
  [server-node, {:keys [min-port, max-port, admin-user, admin-password] :as options}]
  (ui/launch server-node, min-port, max-port, admin-user, admin-password, (->option-map options)))


(defn start-server-with-config
  [{:keys [hostname, port, scheduling-strategy] :as options}]
  (let [server-node (server/start-server hostname, port, (->option-map options))
        web-server (launch-web-ui server-node, options)] 
    (doto server-node
      (server/set-web-server web-server))))


(defn start-worker-with-config
  [{:keys [server-hostname, server-port, thread-count, cpus] :as options}]  
  (worker/start-worker server-hostname, server-port
    :worker-thread-count (or thread-count cpus),
   (->option-map options))
  ; workaround: keep worker alive otherwise it could shutdown ... (better solution via non-daemon threads)
  (while true
    (Thread/sleep 60000)))


(defn build-job-setup-function
  [{:keys [job-function, job-args] :as options}]
  (let [f (resolve/resolve-fn job-function)]
    (if job-args
      (apply partial f job-args)
      f)))

(defn start-client-with-config
  ([options]
    (start-client-with-config options, false))
  ([{:keys [server-hostname, server-port] :as options}, main?]
    (client/start-automatic-client
      server-hostname, server-port,
	    (build-job-setup-function options),
	    (->option-map options))
    (when main?
      (Thread/sleep 1000)
      (shutdown-agents)
      (System/exit 0))))


(defn start-rest-client-with-config
  ([options]
    (start-rest-client-with-config options, false))
  ([{:keys [rest-port, server-hostname, server-port] :as options}, main?]
    (rest-client/start-rest-client rest-port, server-hostname, server-port, (->option-map options))
    (when main?
      (Thread/sleep 1000)
      (shutdown-agents)
      (System/exit 0))))


(defn print-usage
  ([banner]
    (print-usage banner, 0))
  ([banner, return]
    (let [version (v/sputnik-version)]
      (println
        (format
          (str "Sputnik Satellite (Sputnik %s)\n"
            "Usage: java -jar sputnik-%s.jar satellite <options>* <config-file>\n\n"
            "Options:\n"
            "%s")
          version
          version
          banner))
      (System/exit return))))


(defn- read-config
  [config-url]
  (with-open [w (-> config-url io/reader java.io.PushbackReader.)]
    (read w)))


(defn- setup-logging
  [{:keys [log-file, nickname, numa-id] :as options}]
  (log/configure-logging
    :filename (or log-file (format "%s.log" (worker/node-name nickname, numa-id)))
    (->option-map options)))


(defn start-node
  [node-type, options, main?]
  (tx/enable)
  (case node-type
    :server (start-server-with-config options)
    :worker (start-worker-with-config options)
    :client (start-client-with-config options, main?)
    :rest-client (start-rest-client-with-config options, main?)))


(def ^:private cli-options
  [["-h" "--help" "Show help." :default false]
   [nil  "--version" "Show version." :default false]
   ["-t" "--type ROLE" "Specifies the type of this Sputnik node: worker, server, client, rest-client." :parse-fn keyword]])


(defn -main
  [& args]
  (try
    (let [{:keys [options, arguments, summary, errors]} (cli/parse-opts args, cli-options),
          {:keys [help, version, type]} options,
          config-file (cond
                        (or (nil? arguments) (not= (count arguments) 1))
                          (do
                            (println "ERROR: You need to specify exactly one config file to start a sputnik node.")
                            (print-usage summary, 1))                        
                        (not (fs/exists? (first arguments)))
                          (do
                            (println (format "There is no config file named \"%s\"!" (first arguments)))
                            (print-usage summary, 1))
                        :else
                          (first arguments))]      
	    (cond
        errors (do
                 (doseq [err-msg errors] (println err-msg))
                 (print-usage summary, 1))
	      help (print-usage summary)
        version (println (format "Sputnik Satellite (Sputnik %s)" (v/sputnik-version)))
        (not (#{:server :worker :client :rest-client} type)) (do (println (format "ERROR: The node type can only be one of \"worker\", \"server\", \"client\" or \"rest-client\" and not \"%s\"!" (name type)))
                                                               (print-usage summary, 1))
        type (let [config (read-config config-file)
                   options (cfg/node-options type, config)]
               (setup-logging options)
               (threads/setup-default-thread-exception-handler!)
               (when (= type :worker)
                 (threads/replace-agent-thread-pools!))
               (binding [*force* :agent]
                 (debug (format "Read config:\n%s" (with-out-str (pprint config))))
                 (debug (format "Config decoded to option map:\n%s" (with-out-str (pprint options))))
                 (debug (format "Started with command: %s" (pr-str (System/getProperty "sun.java.command"))))
                 (debug (format "Initial classpath: %s" (System/getProperty "java.class.path")))
                 (start-node type, options, true)))
	      :else (print-usage summary, 1)))
    (catch Throwable t
      (error (format "Error during main method of sputnik:\n%s" (with-out-str (print-cause-trace t))))
      (print-cause-trace t)
      (System/exit 2))))