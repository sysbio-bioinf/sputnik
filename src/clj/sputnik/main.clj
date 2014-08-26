; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.main
  (:require
    [clojure.tools.cli :as cli]
    [sputnik.version :as v]))




(def ^:private cli-options
  [["-h" "--help" "Show help." :default false]
   [nil  "--version" "Show version." :default false]])


(defn- print-help
  ([banner]
    (print-help banner, nil))
  ([banner, msg]
    (let [version (v/sputnik-version)]
      (when msg
        (println msg "\n"))
      (println
        (format
          (str "Sputnik %s\n"
            "Usage: java -jar sputnik-%s.jar [-h] [control|control-gui|satellite] <options> <config-file>\n\n"
            "Options:\n"
            "%s")
          version
          version
          banner))
      (flush))))


(defn execute-main
  [ns, arguments]
  (require ns)
  (eval `(~(symbol (name ns) "-main") ~@arguments)))


(defn -main
  [& args]
  (let [{:keys [options, arguments, summary, errors]} (cli/parse-opts args, cli-options, :in-order true),
        ; get params
        {:keys [help, version]} options,
        mode (keyword (first arguments))]
    (cond
      errors (do
               (doseq [err-msg errors] (println err-msg))
               (print-help summary)
               (System/exit 1))
      version (println (format "Sputnik version: %s" (v/sputnik-version)))
      help (do (print-help summary) (System/exit 0))
      (= mode :control)     (execute-main 'sputnik.control.main,   (rest arguments))
      (= mode :satellite)   (execute-main 'sputnik.satellite.main, (rest arguments))
      (= mode :control-gui) (execute-main 'sputnik.control.gui,    (rest arguments))
      :else (print-help summary, "Mode must be one of: control, control-gui or satellite!"))))