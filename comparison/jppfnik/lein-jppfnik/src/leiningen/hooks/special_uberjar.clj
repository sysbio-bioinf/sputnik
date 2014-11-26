; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns leiningen.hooks.special-uberjar
  (:require
    [robert.hooke :as hooke]
    [clojure.java.io :as io]
    leiningen.uberjar))



(defn create-pred
  "Creates a predicate based on the type of the given argument.
  For a string an equality check is created; for a pattern a matching check is created."
  [x]
  (cond
    (string? x) (fn [s] (= s x))
    (instance? java.util.regex.Pattern x) (fn [s] (re-matches x s))
    :else (constantly true)))


(defn create-jar-filter
  "Creates a filter function from the :exclude-from-uberjar in the project.clj."
  [project]
  (if-let [exclusions (:exclude-from-uberjar project)]
    (let [preds (map create-pred exclusions)]
      (fn [f]
        (let [fname (-> f io/file .getName)]
          (or
            (not-any? #(% fname) preds)
            (println "EXCLUDING" fname)))))
    (constantly true)))


(defn exclude-jars
  "Intercepts leiningen.uberjar/write-components to filter the jars included in the uberjar.
  The exluded jars are specified in the project.clj via :exclude-from-uberjar [#\"clojure-.*jar\" \"clojure-1.3.0.jar\"],
  as regular expression or string."
  [original-write-components, project, jars, out]
  (let [jars (filter (create-jar-filter project) jars)]
    (original-write-components project jars out)))


(defn activate []
  (hooke/add-hook 
    #'leiningen.uberjar/write-components
    exclude-jars))