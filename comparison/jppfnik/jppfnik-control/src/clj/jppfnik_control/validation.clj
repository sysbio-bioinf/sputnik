; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-control.validation
  (:require
    [jppfnik-control.config :as config]
    [clojure.java.io :as io])
  (:import
    java.io.FileNotFoundException))



(defmulti valid-config? "Checks whether a given sputnik configuration is valid." type)


(defmethod valid-config? :default
  [x]
  (if-let [cfg-type (config/config-type x)]
    (throw (Exception. (format "Unregistered config type \"%s\"!" cfg-type)))
    (throw (IllegalArgumentException. (format "Wrong object of type \"%s\" passed to valid-config?" (type x))))))




(defn file-exists?
  "Checks whether the file specified by the given url exists."
  [url]
  (-> url io/file .exists boolean))

(defn directory-exists?
  "Checks whether the directory specified by the given url exists."
  [url]
  (let [f (io/file url)]
    (and (.exists f) (.isDirectory f))))



(derive :sputnik/clojure-jar-payload :sputnik/jar-payload)
(derive :sputnik/sputnik-jar-payload :sputnik/jar-payload)

(defmethod valid-config? :sputnik/jar-payload
  [{:keys [url, project-name, version]}]
  ; TODO: Überprüfung der pom.xml auf Project-Name/Gruppe und Version implementieren
  (or (file-exists? url)
    (throw (FileNotFoundException. (format "File \"%s\" does not exist!" url)))))


(defmethod valid-config? :sputnik/directory-payload
  [{:keys [url]}]
  (or (directory-exists? url)
    (throw (FileNotFoundException. (format "Directory \"%s\" does not exist!" url)))))

