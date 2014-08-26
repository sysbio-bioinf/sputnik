; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.control.main
  (:require
    [clojure.string :as string]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.tools.cli :as cli]
    [sputnik.tools.logging :as log]
    [sputnik.tools.error :as e]
    [sputnik.tools.file-system :as fs]
    [sputnik.config.api :as cfg]
    [sputnik.config.lang :as cfg-lang]
    [sputnik.config.meta :as meta]
    [sputnik.control.host-usage :as usage]
    [sputnik.control.launch :as l]
    [sputnik.version :as v]))


(def ^:dynamic ^:private *in-repl* false)


(defn show-host-usage
  [config-filename, detailed?, repetition-count]
  (let [cfg (try 
              (cfg/load-config config-filename) 
              (catch Exception e 
                (println (format "Configuration \"%s\" could not be loaded!" config-filename))
                (println "Details:\n")
                (print-cause-trace e)
                (when-not *in-repl*
                  (System/exit 1))))]
    (usage/host-usage-info
      (mapv cfg/node-config (cfg/select-configs :sputnik/node cfg))
      repetition-count
      :details detailed?
      :cols 200)))


(defn decode-node-spec
  [node-spec-str]
  (let [[_, role-name, node-name] (re-matches #"([^@]*)@([^@]*)" node-spec-str)]
    (if (and role-name, node-name)
      {:role-name role-name, :node-name node-name}
      (e/illegal-argument "The node specification must have the format \"rolename@nodename\" but instead \"%s\" was given!" node-spec-str))))


(defn ->symbol
  [x]
  (if (symbol? x)
    x
    (symbol (name x))))

(defn lookup
  [config-map, config-type, symbol-name]
  (if-let [config (config-map (->symbol symbol-name))]
    (let [ctype (meta/config-type config)]
      (if (= config-type ctype)
        config
        (e/illegal-argument "Configuration \"%s\" has type \"%s\" instead of \"%s\"!" symbol-name, ctype, config-type)))
    (e/illegal-argument "No configuration with name \"%s\" found!" symbol-name)))

(defn spec->node
  [config-map, role-type, {:keys [role-name, node-name] :as node-spec}]
  (let [role (lookup config-map, role-type, role-name),
        node (lookup config-map, :sputnik/node, node-name)]
    (cfg-lang/apply-role role, node)))


(defn add-job-parameter
  [job-parameter-list-str, client]
  (if (string/blank? job-parameter-list-str)
    client
    (reduce
      (fn [c, kv-str]
        (if (string/blank? kv-str)
          c
          (let [[_ k v] (re-matches #"(.*)=(.*)" kv-str)]
            (assoc-in c [:options :job (keyword k)] v))))
      client
      (string/split job-parameter-list-str #","))))


(defn initiate-launch-sequence
  [config-filename, launch, server-spec-str, worker-spec-str-list, client-spec-str, rest-client-spec-str, job-parameter-list-str, communication-name, payload-name-list, additional-payload-list, payload-directory, raise-on-error]
  (when-not (#{:all, :server, :worker, :client, :rest-client} launch)
    (e/illegal-argument "Launch role %s specified but only \"server\", \"worker\", \"client\", \"rest-client\" or \"all\" are allowed!" launch))
  (let [config-map (cfg/load-config config-filename),
        create-node (fn [role-type, spec-str] (spec->node config-map, role-type, (decode-node-spec spec-str))),
        server (when server-spec-str (create-node :sputnik/server, server-spec-str)),
        worker-list (when-not (string/blank? worker-spec-str-list) (mapv #(cfg/use-server server, (create-node :sputnik/worker, %)) (string/split worker-spec-str-list #","))),
        client (when client-spec-str
                 (add-job-parameter job-parameter-list-str,
                   (cfg/use-server server
                     (create-node :sputnik/client, client-spec-str)))),
        rest-client (when rest-client-spec-str
                      (cfg/use-server server
                          (create-node :sputnik/rest-client, rest-client-spec-str))),
        communication (when communication-name (lookup config-map, :sputnik/communication, (->symbol communication-name))),
        payload-list (when-not (string/blank? payload-name-list) (mapv #(lookup config-map, :sputnik/payload-entity, (->symbol %)) (string/split payload-name-list #","))),
        payload-list (if (string/blank? additional-payload-list)
                       payload-list
                       (into payload-list (mapv cfg-lang/payload-entity (string/split additional-payload-list #","))))]
    (when (and (#{:all :server} launch) server)
      (l/launch-nodes [server], communication, payload-list, (str payload-directory "-server"), :verbose true, :raise-on-error raise-on-error))
    (when (and (#{:all :worker} launch) worker-list)
      (l/launch-nodes worker-list, communication, payload-list, (str payload-directory "-worker"), :verbose true, :raise-on-error raise-on-error))
    (when (and (#{:all :client} launch) client)
      (l/launch-nodes [client], communication, payload-list, (str payload-directory "-client"), :verbose true, :raise-on-error raise-on-error))
    (when (and (#{:all :rest-client} launch) rest-client)
      (l/launch-nodes [rest-client], communication, payload-list, (str payload-directory "-rest-client"), :verbose true, :raise-on-error raise-on-error))))

; (initiate-launch-sequence "example/aco_all.clj", :all, "std-server@mindstorm", "worker-12@node0,worker-12@node1", "example-client@mindstorm", "sputnik-comm", "sputnik,aco-jar,solomon-instances,homberger-instances", "")


(defn- print-help
  ([banner]
    (print-help banner, nil))
  ([banner, msg]
    (let [version (v/sputnik-version)]
      (when msg
        (println msg "\n"))
      (println
        (format
          (str "Sputnik Control (Sputnik %s)\n"
            "Usage: java -jar sputnik-%s.jar control [-u|-l] <options> <config-file>\n\n"
            "Options:\n"
            "%s")
          version
          version
          banner))
      (flush))))



(def ^:private cli-options
  [["-h" "--help" "Show help." :default false]
   [nil  "--version" "Show version." :default false]
   ["-u" "--usage" "Display cpu usage of hosts specified in the given configuration." :default false]
   ["-n" "--repetition-count N" "Repetition count of the selected action (e.g. --usage)." :default 3 :parse-fn #(when % (Integer/parseInt %)) :validate [integer? "Must be a positive integer!"]]
   ["-l" "--launch WHAT?" "Launch nodes of the given role: \"server\", \"worker\", \"client\", \"rest-client\", \"all\"." :parse-fn keyword]
   ["-S" "--server SERVER" "Specifies the server to start. The server needs to be specified as \"rolename@nodename\"."]
   ["-W" "--worker WORKER-LIST" "Specifies the list of workers to start. The worker list needs to be specified as \"rolename1@nodename1,rolename2@nodename2,...\"."]
   ["-C" "--client CLIENT" "Specifies the client to start. The client needs to be specified as \"rolename1@nodename\"."]
   ["-R" "--rest-client REST-CLIENT" "Specifies the REST client to start. The REST client needs to be specified as \"rolename1@nodename\"."]
   ["-J" "--job PARAM-SPEC" "Specifies job parameters for the job that is run on the client. Parameter specification as \"keyword1=value1,keyword2=value2,...\"."]
   ["-P" "--payload CONFIGNAME" "Specifies the payload configuration to use."]
   ["-F" "--additional-payload PATHS" "Specifies file/directory names that shall be used as additional payload."]
   ["-D" "--payload-directory DIRECTORY" "Specifies the name of the payload directory that is used on the remote computer." :default "sputnik-payload"]
   ["-T" "--communication CONFIGNAME" "Specifies the communication configuration that shall be used."]
   ["-d" "--details" "Display detailed cpu usage."]
   ["-E" "--raise-on-error" :default false]])




(defn -main
  [& args]
  (log/configure-logging :filename "sputnik-control.log", :level :warn)
  (let [{:keys [options, arguments, summary, errors]} (cli/parse-opts args, cli-options),
        ; get params
        {:keys [help, usage, repetition-count, details, version,
                launch, server, worker, client, rest-client payload, additional-payload, payload-directory, communication,
                job, raise-on-error]} options,
        config-filename (first arguments)]
    (cond
      errors (do
               (doseq [err-msg errors] (println err-msg))
               (print-help summary)
               (when-not *in-repl*
                 (System/exit 1)))
      version (println (format "Sputnik Control (Sputnik %s)" (v/sputnik-version)))
      help (print-help summary)
      (nil? config-filename) (print-help summary "A configuration file is needed!")
      (not (fs/file? config-filename)) (print-help summary (format "The given configuration filename \"%s\" does not refer to a file!" config-filename))
      usage (show-host-usage config-filename, details, repetition-count)
      launch (initiate-launch-sequence config-filename, launch, server, worker, client, rest-client, job, communication, payload, additional-payload, payload-directory, raise-on-error)
      :else (print-help summary, "No action selected!")))
  (when-not *in-repl*
    (shutdown-agents)))


(defmacro main
  [& args]
  (let [args (mapv str args)]
    `(binding [*in-repl* true]
       (-main ~@args))))