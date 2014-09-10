(ns sputnik.fake-main
  (:gen-class))


(defn -main
  [& args]
  (require 'sputnik.main)
  (apply (resolve 'sputnik.main/-main) args))