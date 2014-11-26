(defproject jppfnik-tools "0.0.1"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo}
  
  :dependencies [[clj-time "0.4.4"]]
  
  :profiles
  {:dev 
    {:dependencies 
      [[org.clojure/clojure "1.6.0"]
       [clj-debug "0.7.5"]]}})
