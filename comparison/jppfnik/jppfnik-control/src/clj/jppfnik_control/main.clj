; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-control.main
  (:gen-class)
  (:require
    [jppfnik-control.config :as config]
    [jppfnik-control.host-usage :as usage])
  (:use 
    [clojure.tools.cli :only [cli]]
    [clojure.stacktrace :only [print-cause-trace]]))


(defn show-host-usage
  [config-name, detailed?, repetition-count]
  (let [cfg (try 
              (config/load-config config-name) 
              (catch Exception e 
                (println (format "Configuration \"%s\" could not be loaded!" config-name))
                (println "Details:\n")
                (print-cause-trace e)
                (System/exit 1)))]
    (usage/host-usage-info
      (config/select-configs :sputnik/host cfg)
      (first (config/select-configs :sputnik/user cfg)) ; first is not optimal here
      repetition-count
      :details detailed?
      :cols 200)))


(defn -main
  [& args]
  (let [[{:keys [help usage repetition-count details] :as options} args banner] 
          (cli args
            ["-h" "--help" "Show help." :default false :flag true]
            ["-u" "--usage" "Display cpu usage of hosts specified in the configuration named by this parameter."]
            ["-n" "--repetition-count" "Repitition count of the selected action (e.g. --usage)." :default 3 :parse-fn #(Integer/parseInt %)]
            ["-d" "--details" "Display detailed cpu usage." :flag true])]
    (cond
      help (println banner)
      usage (show-host-usage usage, details, repetition-count)
      :else (println banner)))
  (shutdown-agents))