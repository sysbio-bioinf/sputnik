; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.tools.error)


(defn illegal-argument
  "Throws an IllegalArgumentException with the given format applied to the given arguments as message."
  [fmt & args]
  (throw (IllegalArgumentException. ^String (apply format fmt args))))


(defn exception
  "Throws an Exception with the given format applied to the given arguments as message."
  [fmt & args]
  (throw (Exception. ^String (apply format fmt args))))