(defproject jppfnik-control "0.0.1"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo}
  :dependencies 
  [[org.clojure/clojure "1.6.0"]
   [org.cloudhoist/pallet "0.7.1"]
   [org.clojure/tools.cli "0.2.2"]
   [org.slf4j/slf4j-api "1.6.1"]
   [org.slf4j/slf4j-log4j12 "1.6.1"]
   [clojure.options "0.2.9"]]
  
  :repositories
  {"sonatype-snapshots" "https://oss.sonatype.org/content/repositories/snapshots"
   "sonatype" "https://oss.sonatype.org/content/repositories/releases/"}
  
  :profiles
  {:dev {:dependencies [[clj-debug "0.7.5"]]}}
  
  :source-paths ["src/clj"]
  ;:java-source-paths ["src/jvm"]
  
  :main jppfnik-control.main
)
