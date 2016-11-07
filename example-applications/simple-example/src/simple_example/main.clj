; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns simple-example.main
  (:gen-class)
  (:require
    [clojure.tools.cli :as cli]
    [sputnik.tools.logging :as l]
    [sputnik.tools.file-system :as fs]
    [sputnik.api :as s]
    [simple-example.core :as c]))



(def ^:private cli-options
  [["-h" "--help" "Show help." :default false]
   ["-c" "--config URL" "URL to Sputnik client configuration file."]
   ["-T" "--thread-count N" "Number of threads to use when local instead of distributed computation is used (no --config specified)" :parse-fn #(Long/parseLong %) :default 1]
   ["-s" "--seed S" "Seed for the PRNG." :parse-fn #(Long/parseLong %) :default (System/currentTimeMillis)]
   ["-t" "--task-count T" "Number of computational tasks." :parse-fn #(Long/parseLong %) :default 1000]
   ["-p" "--point-count P" "Number of points per tasks."   :parse-fn #(Long/parseLong %) :default 100000000]
   ["-I" "--implementation I" "Implementation to use: either \"manual\" or \"future\"." :parse-fn keyword :default :manual]
   ["-P" "--progress-report" "Specifies whether a progress report is printed." :default false]
   ["-L" "--log-level L" "Specifies the log level." :parse-fn keyword, :default :info, :validate [#{:error, :warn, :info, :debug, :trace} "Must be one of: error, warn, info, debug, trace."]]
   [nil "--setup" "Start the Sputnik GUI to setup the Sputnik cluster." :default false]])


(defn- print-help
  ([banner]
    (print-help banner, nil))
  ([banner, msg]
    (when msg
      (println msg "\n"))
    (println
      (format
        (str "Usage: java -jar simple-example-<VERSION>.jar run <OPTIONS> <FILES>\n\n"
          "Options:\n"
          "%s")
        banner))
    (flush)))



(defn- execute-main
  [ns, arguments]
  (require ns)
  (eval `(~(symbol (name ns) "-main") ~@arguments)))


(defn -main
  [& args]
  (let [{:keys [options, arguments, summary, errors]} (cli/parse-opts args, cli-options),
        {:keys [help, setup, config, seed, task-count, point-count, progress-report, thread-count, implementation, log-level]} options]
    (l/configure-logging :filename "client.log" :log-level log-level)
    (cond
      errors (do
               (doseq [err-msg errors] (println err-msg))
               (print-help summary)
               (System/exit 1)),
      help  (do (print-help summary) (System/exit 0)),
      setup (execute-main 'sputnik.control.gui, (rest arguments))      
      (and config (not (fs/file? config))) (print-help summary (format "The given configuration filename \"%s\" does not refer to a file!" config))
      :else (do
              (case implementation
                :manual (c/estimate-pi seed, task-count, point-count,
                          :progress? progress-report
                          :mode (if config :sputnik :parallel)
                          :sputnik-config config
                          :thread-count thread-count)
                :future (s/with-sputnik {:mode (if config :sputnik :parallel)
                                         :sputnik-config config
                                         :thread-count thread-count}
                          (c/estimate-pi-new seed, task-count, point-count))) 
              (System/exit 0)))))