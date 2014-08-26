; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.tools.logging
  (:import
    org.apache.log4j.PropertyConfigurator
    java.util.Properties)
  (:require
    [clojure.string :as string])
  (:use
    [clojure.options :only [defn+opts]]))


(defn level->string
  [level]
  (-> level name string/upper-case))

(defn+opts logging-properties
  "Creates the properties map to configure the logging environment.
  <filename>Filename of the log.</>
  <level>Log level that is globally used.</>
  <max-size>Maximum size of the log file in MB</>
  <backup-count>Maximum number of backup files of the log (RollingFileAppender)</>
  <custom-levels>Custom log levels for given classes/namespaces</>"
  [| {filename "default.log", log-level (choice :info :trace, :debug, :warn, :error, :fatal), max-size 100, backup-count 10, custom-levels {}}]
  (let [log-props (doto (System/getProperties)
                    (.setProperty "log4j.rootLogger" (format "%s, file" (level->string log-level)))
                    (.setProperty "log4j.appender.file" "org.apache.log4j.RollingFileAppender")
                    (.setProperty "log4j.appender.file.File" filename)
                    (.setProperty "log4j.appender.file.MaxFileSize" (format "%sMB" max-size))
                    (.setProperty "log4j.appender.file.MaxBackupIndex" (str backup-count))
                    (.setProperty "log4j.appender.file.layout" "org.apache.log4j.PatternLayout")
                    (.setProperty "log4j.appender.file.layout.ConversionPattern" "%d{ABSOLUTE} %5p %c: %m%n"))]
    (reduce-kv
      (fn [props, namespace, level]
        (doto props
          (.setProperty (format "log4j.logger.%s" (name namespace)) (level->string level))))
      log-props
      custom-levels)))


(defn+opts configure-logging
  "Configures the logging environment with the given options."
  [| :as options]
  (PropertyConfigurator/configure (logging-properties options)))