; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-control.file-system
  (:require
    [clojure.java.io :as io]
    [clojure.string :as string])
  (:use
    [clojure.options :only [defn+opts]]))


(defn filename
  "Returns the filename of the given url."
  [url]
  (-> url io/file .getName))


(defn assure-directory-name
  "Returns the path of the given url with a trailing slash."
  [url]
  (let [filepath (-> url io/file .getPath (string/replace #"\\", "/"))]
    (str filepath "/")))


(defn create-directory
  "Creates the given directory if it does not exist already."
  [url]
  (let [f (io/file url)]
    (when-not (.exists f)
      (.mkdirs f))
    f))

(defn+opts delete-directory
  "Deletes a given directory."
  [f | {silently false} :as options]
  (let [f (io/file f)]
    (when (.exists f)
	    (when (.isDirectory f)
	      (doseq [child (.listFiles f)]
	        (delete-directory child, options)))
	    (io/delete-file f silently))))


(defn copy-file
  [source-file, target-directory]
  (io/copy (io/file source-file) (io/file target-directory (filename source-file))))