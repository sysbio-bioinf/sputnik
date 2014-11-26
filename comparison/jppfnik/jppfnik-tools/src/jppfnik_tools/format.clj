; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-tools.format
  (:use
    [clj-time.core :only [default-time-zone, day]]
    [clj-time.coerce :only [from-long]]
    [clj-time.format :only [formatter, unparse]]))


(def ^:private datetime-formatter (formatter "dd.MM.yyyy HH:mm:ss" (default-time-zone)))
(def ^:private duration-formatter (formatter "HH:mm:ss,SSS"))
(def ^:private datetime-filename-formatter (formatter "yyyyMMdd-HHmmss" (default-time-zone)))

(defn datetime-format
  "Renders the date time given as milliseconds to a string using the following format:
  \"dd.MM.yyyy HH:mm:ss\""
  [millis]
  (unparse datetime-formatter (from-long (long millis))))

(defn duration-format
  "Renders the duration given as milliseconds to a string using the following format:
  \"HH:mm:ss,SSS\""
  [millis]
  (unparse duration-formatter (from-long (long millis))))

(defn duration-with-days-format
  "Renders the duration given as milliseconds to a string using the following format:
  \"ddd HH:mm:ss,SSS\""
  [millis]
  (let [date (from-long (long millis))]
    (str
    (format "%03dd " (dec (day date)))
    (unparse duration-formatter date))))

(defn datetime-filename-format
  "Renders the date time given as milliseconds to a string using the following format:
  \"yyyyMMdd-HHmmss\""
  [millis]
  (unparse datetime-filename-formatter (from-long (long millis))))