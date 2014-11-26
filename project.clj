(defn get-version
  []
  (let [version-fn (try
                     (load-file "src/clj/sputnik/version.clj")
                     (catch java.io.FileNotFoundException e
                       ; workaround for CCW (version number is not needed anyway)
                       (constantly "0.0.0-REPL-DEV")))]
    (version-fn)))

(def version (get-version))

(defproject sputnik version
  :min-lein-version "2.0.0"
  :description "Sputnik is a Clojure library for parallelization of computations to distributed computation nodes. Sputnik does not only handle the distribution and execution of tasks but also the configuration and deployment of the server and the workers."
  :url "https://github.com/sysbio-bioinf/sputnik/"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [clojure.options "0.2.9"]
                 [org.clojure/tools.cli "0.3.1"]
                 [txload "0.1.0"]
                 ; persistence:
                 [frost "0.4.1"]
                 ; logging:
                 [org.clojure/tools.logging "0.2.6"]
                 [org.slf4j/slf4j-api "1.7.7"]
                 [org.slf4j/slf4j-log4j12 "1.7.7"]                 
                 ; sputnik.satellite.ui:
                 [ring "1.3.1"]
                 [compojure "1.2.1"]
                 [hiccup "1.0.5"]
                 [com.cemerick/friend "0.2.0"]
                 [clj-time "0.8.0"]
                 ; sputnik.control:
                 [org.cloudhoist/pallet "0.7.5"]
                 [seesaw "1.4.4" :exclusions [org.swinglabs.swingx/swingx-core]]
                 [quil "1.7.0"]
                 [org.bouncycastle/bcpkix-jdk15on "1.50"]]
  
  :profiles
  {:dev {:dependencies [[clj-debug "0.7.5"]
                        [org.clojure/test.check "0.5.9"]]}
   :debug {:jvm-opts ["-Djavax.net.debug=ssl:record"]}}  
   
  
  :repositories
  {"sonatype-snapshots" "https://oss.sonatype.org/content/repositories/snapshots",
   "sonatype" "https://oss.sonatype.org/content/repositories/releases/"}
  
;  :repl-options
;  {:init (do
;           (require '[sputnik.tools.logging :s log])
;           (log/configure-logging :filename "sputnik-control.log", :log-level :debug))}
    
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :resource-paths ["resources"]
    
  :main sputnik.fake-main
  :aot [sputnik.fake-main]
  
  :jar-name ~(format "sputnik-lib-%s.jar" version)
  :uberjar-name ~(format "sputnik-%s.jar" version))
