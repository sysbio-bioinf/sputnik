(def version "1.0.0")

(defproject feature-selection version
  :min-lein-version "2.0.0"
  :description "Feature selection via a parallel genetic algorithm for a nearest neighbor classifier."
  :url "https://github.com/guv/sputnik/example-application/feature-selection/"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [sputnik "0.3.1"]
                 #_[pareto "0.1.0"]]
  
  :profiles {:dev {:dependencies [[clj-debug "0.7.3"]]}}
  
  :main feature-selection.main
  :aot [feature-selection.main]
  
  :jvm-opts ^:replace []
  
  :jar-name ~(format "feature-selection-lib-%s.jar" version)
  :uberjar-name ~(format "feature-selection-%s.jar" version)
  
  :repositories ^:replace
  {"releases" {:url "https://archiva.frontlinecoders.de/archiva/repository/internal"
               :signing {:gpg-key "D9E0A4CF"}},
   "snapshots" "https://archiva.frontlinecoders.de/archiva/repository/snapshots"})
