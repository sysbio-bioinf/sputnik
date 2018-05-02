; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.tools.format
  (:require
    (clj-time
      [core :as t]
      [coerce :as c]
      [format :as f])))


(def ^:private datetime-formatter (f/formatter "dd.MM.yyyy HH:mm:ss" (t/default-time-zone)))
(def ^:private datetime-filename-formatter (f/formatter "yyyyMMdd-HHmmss" (t/default-time-zone)))

(defn datetime-format
  "Renders the date time given as milliseconds to a string using the following format:
  \"dd.MM.yyyy HH:mm:ss\""
  [^long millis]
  (f/unparse datetime-formatter (c/from-long millis)))


(defn datetime-filename-format
  "Renders the date time given as milliseconds to a string using the following format:
  \"yyyyMMdd-HHmmss\""
  [^long millis]
  (f/unparse datetime-filename-formatter (c/from-long millis)))


(defn duration-data
  [^long duration-in-millis]
  (let [milliseconds (mod duration-in-millis 1000),
        duration-in-secs (quot duration-in-millis 1000),
        seconds (mod duration-in-secs 60),
        duration-in-mins (quot duration-in-secs 60),
        minutes (mod duration-in-mins 60),
        duration-in-hours (quot duration-in-mins 60),
        hours (mod duration-in-hours 24),
        days (quot duration-in-hours 24)]
    {:milliseconds milliseconds,
     :seconds seconds,
     :minutes minutes,
     :hours hours,
     :days days}))


(defn duration-format
  "Renders the duration given as milliseconds to a string using the following format:
  \"HH:mm:ss,SSS\""
  [^long duration-in-millis]
  (let [{:keys [hours, minutes, seconds, milliseconds]} (duration-data duration-in-millis)]
    (format "%02d:%02d:%02d,%03d" hours, minutes, seconds, milliseconds)))


(defn duration-with-days-format
  "Renders the duration given as milliseconds to a string using the following format:
  \"ddd HH:mm:ss,SSS\""
  [^long duration-in-millis]
  (let [{:keys [days, hours, minutes, seconds, milliseconds]} (duration-data duration-in-millis)]
    (format "%03dd %02d:%02d:%02d,%03d" days, hours, minutes, seconds, milliseconds)))

