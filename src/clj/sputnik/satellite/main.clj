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
    [txload.core :as tx]
    [clojure.string :as str]
    [seesaw.util :as util])
  (:import (java.util List)
           (java.io FilenameFilter File)))


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
  [{:keys [log-file, nickname, numa-id, cpu-id] :as options}]
  (log/configure-logging
    :filename (or log-file (format "%s.log" (worker/node-name nickname, numa-id, cpu-id)))
    (->option-map options)))


(defn start-node
  [node-type, options, main?]
  (tx/enable)
  (case node-type
    :server (start-server-with-config options)
    :worker (start-worker-with-config options)
    :client (start-client-with-config options, main?)
    :rest-client (start-rest-client-with-config options, main?)))


(defn worker-process
  [config-file, {:keys [numa-id, cpu-id, thread-count] :as execution-options}, {:keys [nickname, sputnik-jvm-opts, sputnik-numactl] :as config-options}]
  (let [java-bin (str (System/getProperty "java.home") "/bin/java")
        classpath (System/getProperty "java.class.path")
        user-dir (System/getProperty "user.dir"),
        command (-> ["nohup"]
                  (cond->
                    numa-id (into [(or sputnik-numactl "numactl") (format "--cpunodebind=%s" numa-id) "--localalloc"])
                    cpu-id (into [(or sputnik-numactl "numactl") (format "--physcpubind=%s" cpu-id) "--localalloc"]))
                  (into [java-bin, "-cp", classpath])
                  ; java parameters
                  (cond->
                    sputnik-jvm-opts (into (str/split sputnik-jvm-opts #" ")))
                  ; programm class
                  (into ["sputnik.satellite.run" "-t" "worker"])
                  ; sputnik parameters
                  (cond->
                    numa-id (into ["--numa-id" (str numa-id)])
                    cpu-id (into ["--cpu-id" (str cpu-id)])
                    thread-count (into ["--thread-count" (str thread-count)]))
                  (into [config-file])),
        output-filename (format "%s-worker.out" (worker/node-name nickname, numa-id, cpu-id))]
    #_(println "Executing:") #_(pprint command)
    (-> (ProcessBuilder. ^List command)
      (.directory (io/file user-dir))
      (.redirectErrorStream true)
      (.redirectOutput (io/file output-filename))
      (.start))))


(defn wait-for
  [^Process process]
  (.waitFor process))


(defn directory-filter
  ^FilenameFilter [re-expr]
  (reify FilenameFilter
    (accept [this, dir, name]
      (and
        (boolean (re-matches re-expr, name))
        (.isDirectory (io/file dir, name))))))


(defn read-file
  [dir, filename]
  (let [f (io/file dir, filename)]
    (when-not (.exists f)
      (throw (RuntimeException. (format "File \"%s\" does not exist!" (.getAbsolutePath f)))))
    (slurp f)))


(defn parse-number
  [s]
  (->> s
    (re-find #"\d+")
    Long/parseLong))


(defn cpu-info
  [cpu-dir]
  {:siblings (->> (str/split (read-file cpu-dir, "topology/thread_siblings_list") #"[-,\n]")
               (remove nil?)
               (mapv #(Long/parseLong %))
               set),
   :cpu-id (-> ^File cpu-dir .getName parse-number)
   :numa-node (parse-number (read-file cpu-dir, "topology/physical_package_id"))})


(defn remove-thread-siblings
  [cpus]
  (let [nodes-to-keep (->> cpus
                        (mapv #(-> % :siblings sort first))
                        set)]
    (filterv #(contains? nodes-to-keep (:cpu-id %)) cpus)))


(defn numa-topology
  []
  (let [cpu-dir-name "/sys/devices/system/cpu",
        cpu-dir (io/file cpu-dir-name)]
    (when-not (.exists cpu-dir)
      (throw (RuntimeException. (format "Directory \"%s\" with CPU information does not exist!" cpu-dir-name))))
    (->> (.listFiles cpu-dir (directory-filter #"cpu\d+"))
      (mapv cpu-info)
      (group-by :numa-node)
      (reduce-kv
        (fn [m, numa-id, cpus]
          (assoc m numa-id (remove-thread-siblings cpus)))
        {}))))


;"nohup %s --cpunodebind=%s --membind=%s %s -cp %s %s sputnik.satellite.run %s 2>&1 > %s-worker.out &"
(defn launch-single-worker-processes
  "Launch a single worker process for each thread distributed equally among the different NUMA nodes
  and omitting hyperthreading nodes (thread siblings)."
  [config-file, {:keys [thread-count, numa-nodes] :as config-options}]
  (let [thread-count (* thread-count numa-nodes),
        available-nodes (->> (numa-topology)
                          vals
                          ; interleave cpu cores of all numa nodes to distribute processes equally
                          (apply interleave)
                          vec)]
    (when (> thread-count (count available-nodes))
      (throw
        (RuntimeException.
          (format "Not enough cpu nodes available (%s) to spawn %s processes eclusively on each cpu node."
            (count available-nodes) thread-count))))
    (let [processes (->> available-nodes
                      (take thread-count)
                      (mapv
                        (fn [{:keys [cpu-id]}]
                          ; only one thread for each process
                          (worker-process config-file, {:cpu-id cpu-id, :thread-count 1}, config-options))))]
      (doseq [proc processes]
        (wait-for proc)))))


(defn launch-worker-processes-per-numa-node
  [config-file, {:keys [numa-nodes] :as config-options}]
  (let [processes (mapv
                    (fn [numa-id]
                      (worker-process config-file, {:numa-id numa-id}, config-options))
                    (range numa-nodes))]
    (doseq [proc processes]
      (wait-for proc))))


(defn launch-one-worker-process
  [config-file, config-options]
  (wait-for
    (worker-process config-file, {}, config-options)))


(defn setup-worker-processes
  [config-file]
  (let [config (read-config config-file)
        options (cfg/node-options :worker, config)]
    (setup-logging options)
    (threads/setup-default-thread-exception-handler!)
    (binding [*force* :agent]
      (debug (format "Read config:\n%s" (with-out-str (pprint config))))
      (debug (format "Config decoded to option map:\n%s" (with-out-str (pprint options))))
      (debug (format "Started with command: %s" (pr-str (System/getProperty "sun.java.command"))))
      (debug (format "Initial classpath: %s" (System/getProperty "java.class.path")))
      (let [{:keys [numa-nodes, single-processes?]} options]
        (if single-processes?
          (launch-single-worker-processes config-file, options)
          (if (and numa-nodes (> numa-nodes 1))
            (launch-worker-processes-per-numa-node config-file, options)
            (launch-one-worker-process config-file, options)))))))


(defn adjust-logfile
  [{:keys [nickname, numa-id, cpu-id] :as options}]
  (assoc options :log-file (format "%s-worker.log" (worker/node-name nickname, numa-id, cpu-id))))


(def ^:private cli-options
  [["-h" "--help" "Show help." :default false]
   [nil  "--version" "Show version." :default false]
   ["-t" "--type ROLE" "Specifies the type of this Sputnik node: worker, server, client, rest-client, worker-launch." :parse-fn keyword]
   ["-n" "--numa-id ID" "Specifies the NUMA node id of the worker." :parse-fn #(Long/parseLong %)]
   ["-c" "--cpu-id ID" "Specifies the CPU id of the worker." :parse-fn #(Long/parseLong %)]
   ["-T" "--thread-count N" "Specifies the number of threads to use (overwrites config file value)." :parse-fn #(Long/parseLong %)]])


(defn -main
  [& args]
  (try
    (let [{:keys [options, arguments, summary, errors]} (cli/parse-opts args, cli-options),
          {:keys [help, version, type, numa-id, cpu-id, thread-count]} options,
          config-file (cond
                        (or (nil? arguments) (not= (count arguments) 1))
                          (do
                            (println "ERROR: You need to specify exactly one config file to start a sputnik node.")
                            (println "INVOCATION:" (str/join " " args))
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
        (not (#{:server, :worker, :worker-launch, :client, :rest-client} type))
          (do
            (println (format "ERROR: The node type can only be one of \"worker\", \"worker-launch\", \"server\", \"client\" or \"rest-client\" and not \"%s\"!" (name type)))
            (print-usage summary, 1))
        (= type :worker-launch) (setup-worker-processes config-file)
        type (let [config (read-config config-file)
                   options (cond-> (cfg/node-options type, config)
                             (= type :worker)
                             (cond->
                               numa-id (assoc :numa-id numa-id)
                               cpu-id (assoc :cpu-id cpu-id)
                               ; overwrite thread-count if give as command line parameter
                               thread-count (assoc :thread-count thread-count)
                               true adjust-logfile))]
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
      (flush)
      (System/exit 2))))