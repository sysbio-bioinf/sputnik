; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-client.main
  (:gen-class)
  (:require
    [jppfnik-client.run :as run]
    [clojure.java.io :as io])
  (:use
    [clojure.tools.cli :only [cli]]
    [clojure.stacktrace :only [print-cause-trace]]))


(defn compile-task
  [& namespaces]
  (let [dir (io/file *compile-path*)]
    ; first ensure that compile path directory exists
    (.mkdirs dir)
    ; then compile all given namespaces
    (doseq [n namespaces]
      (compile n))))


(defn -main*
  [& args]
  (let [[{:keys [help] :as options} additional-arguments banner] 
          (cli args
            ["-h" "--help"      "Show help." :default false :flag true]
            ["-j" "--job-fn"    "Specifies the job function that will return the jobs to execute."]
            ["-J" "--job-args"  "Specifies parameters that have to be passed to the job function."]
            ["-r" "--result-fn" "Specifies the result listener function that is called when tasks are finished."]
            ["-p" "--progress"  "Specifies whether a progress report is printed." :default false :flag true])]
    (when help
      (println banner)
      (flush)
      (System/exit 1))
    
    ; first compile the task namespace to get the sputnik.client.SputnikTask class
    (compile-task 'jppfnik-client.task)
    (run/run options)
    (shutdown-agents)))


(defn -main
  [& args]
    (try
      (apply -main* args)
      (catch Throwable t
        (print-cause-trace t)
        (flush)
        (System/exit 2))))

(defmacro main
  [& args]
 `(-main* ~@(map str args)))