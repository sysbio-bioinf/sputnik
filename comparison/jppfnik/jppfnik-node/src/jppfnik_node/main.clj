; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-node.main
  (:gen-class)
  (:use 
    [clojure.tools.cli :only [cli]]
    [clojure.stacktrace :only [print-cause-trace]])
  (:import
    org.jppf.node.NodeLauncher))


(defn -main
  [& args]
  (let [[{:keys [help] :as options} additional-arguments banner] 
          (cli args
            ["-h" "--help" "Show help." :default false :flag true])]
    (when help
      (println banner)
      (flush)
      (System/exit 1))

    (try
      (NodeLauncher/main (into-array String []))
      (catch Throwable t
        (print-cause-trace t)
        (flush)
        (System/exit 2)))))