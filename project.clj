(defn get-version
  []
  (let [version-fn (try
                     (load-file "src/clj/sputnik/version.clj")
                     (catch java.io.FileNotFoundException e
                       (load-file "workspace/sputnik/src/clj/sputnik/version.clj")))]
    (version-fn)))

(def version (get-version))

(defproject sputnik version
  :min-lein-version "2.0.0"
  :description "Sputnik is a Clojure library for parallelization of computations to distributed computation nodes. Sputnik does not only handle the distribution and execution of tasks but also the configuration and deployment of the server and the workers."
  :url "https://github.com/guv/sputnik/"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [clojure.options "0.2.9"]
                 [org.clojure/tools.cli "0.3.1"]
                 ; persistence:
                 [frost "0.4.0"]
                 ; logging:
                 [org.clojure/tools.logging "0.2.6"]
                 [org.slf4j/slf4j-api "1.7.7"]
                 [org.slf4j/slf4j-log4j12 "1.7.7"]                 
                 ; sputnik.satellite.ui:
                 [ring "1.2.2"]
                 [compojure "1.1.8" :exclusions [org.clojure/tools.macro]]
                 [hiccup "1.0.5"]
                 [com.cemerick/friend "0.2.0"]
                 [clj-time "0.4.4"]
                 ; sputnik.control:
                 [org.cloudhoist/pallet "0.7.5"]
                 [seesaw "1.4.4" :exclusions [org.swinglabs.swingx/swingx-core]]
                 [quil "1.7.0"]
                 [org.bouncycastle/bcpkix-jdk15on "1.50"]]
  
  :profiles
  {:dev {:dependencies [[clj-debug "0.7.3"]]}
   :debug {:jvm-opts ["-Djavax.net.debug=ssl:record"]}
   :1.2 {:dependencies [[org.clojure/clojure "1.2.1"]]}
   :1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
   :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
   :reflection {:global-vars {*warn-on-reflection* true}}}
  
  :plugins [[org.timmc/lein-otf "2.0.1"]]
  
  :aliases 
  {"all" ["with-profile" "dev,1.2:dev,1.3:dev"]
   "cleanbuild" ["do" "clean," "compile" ":all"]
   "check" ["with-profile" "reflection" "do" "clean," "compile" ":all"]}  
  
  :repositories ^:replace
  {"sonatype-snapshots" "https://oss.sonatype.org/content/repositories/snapshots",
   "sonatype" "https://oss.sonatype.org/content/repositories/releases/",
   "releases" {:url "https://archiva.frontlinecoders.de/archiva/repository/internal"
                                        :signing {:gpg-key "D9E0A4CF"}},
   "snapshots" "https://archiva.frontlinecoders.de/archiva/repository/snapshots"}
  
;  :repl-options
;  {:init (do
;           (require '[sputnik.tools.logging :s log])
;           (log/configure-logging :filename "sputnik-control.log", :log-level :debug))}
    
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :resource-paths ["resources"]
  
  ; set main with :skip-aot for lein-otf
  :main ^:skip-aot sputnik.main
  
  :jar-name ~(format "sputnik-lib-%s.jar" version)
  :uberjar-name ~(format "sputnik-%s.jar" version))
