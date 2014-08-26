; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.config.meta
  (:require
    [clojure.walk :as walk]))


(defmacro with-type
  "Set the given type on the result of the given expression."
  [type, expr]
 `(vary-meta ~expr assoc :type ~type))


(defmacro with-config-type
  "Set the given config-type on the result of the given expression."
  [type, expr]
 `(vary-meta ~expr assoc :sputnik/config-type ~type))


(defn config-type
  "Determine config-type (if any) of the given argument."
  [x]
  (-> x meta :sputnik/config-type))


(defn config-type?
  "Checks if the given argument is a config type."
  [x]
  (let [t (config-type x)]
    (and t (keyword? t) (-> t namespace (= "sputnik")))))


(defn config-def?
  "Is the given public namespace mapping [symbol value] referring to a Sputnik configuration variable."
  [x]
  (-> x second meta :sputnik/config))


(defn config-commands
  "List all public Sputnik configuration variables in the given namespace."
  [ns]
  (->> ns ns-publics (filter config-def?) keys vec))




(defn preserve-meta
  "Adds the metadata of the old form (if any) to the new form."
  [new-form, old-form]
  (if (instance? clojure.lang.IObj new-form)
    (with-meta new-form (merge (meta old-form) (meta new-form)))
    new-form))


(defn walk
  "Walk function analog to clojure.walk/walk but preserving metadata."
  [inner outer form]
  (preserve-meta (walk/walk inner outer form), form))


(defn postwalk
  "Postwalk function analog to clojure.walk/postwalk but preserving metadata."
  [f form]
  (walk (partial postwalk f) f form))


(defn prewalk
  "Prewalk function analog to clojure.walk/prewalk but preserving metadata."
  [f form]
  (walk (partial prewalk f) identity (f form)))