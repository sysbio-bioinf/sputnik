(def version "0.0.1")

(defproject feature-selection-jppf version
  :min-lein-version "2.0.0"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [clojure.options "0.2.9"]
                 [frost "0.4.0"]
                 [jppfnik-client "0.0.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.slf4j/slf4j-api "1.7.7"]
                 [org.slf4j/slf4j-log4j12 "1.7.7"]]
  
  :profiles {:dev {:dependencies [[clj-debug "0.7.5"]]}}
  
  :main feature-selection-jppf.main
  :aot [feature-selection-jppf.main]
  
  :jvm-opts ^:replace []
  
  :jar-name ~(format "feature-selection-jppf-lib-%s.jar" version)
  :uberjar-name ~(format "feature-selection-jppf-%s.jar" version))
