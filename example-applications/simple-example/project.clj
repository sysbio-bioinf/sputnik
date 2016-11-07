(def version "0.1.3")

(defproject simple-example version
  :min-lein-version "2.0.0"
  :description "Simple example usage of the Sputnik parallelization library."
  :url "https://github.com/guv/sputnik/example-application/simple-example/"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [sputnik "0.5.1"]]
  
  :main simple-example.main
  :aot [simple-example.main]
  
  :jar-name ~(format "simple-example-lib-%s.jar" version)
  :uberjar-name ~(format "simple-example-%s.jar" version)
  
  :profiles {:dev {:dependencies [[clj-debug "0.7.5"]]}})
