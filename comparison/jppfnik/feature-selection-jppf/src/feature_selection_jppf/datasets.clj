; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns feature-selection-jppf.datasets
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.options :refer [defn+opts]]
    [jppfnik-tools.file-system :as fs])
  (:import
    java.util.ArrayList
    java.util.List))


(def ^:private loaded-datasets (ref {}))

(defn- load-dataset
  [load-fn, filename]
  (deref
    (dosync
      (if-let [ds ((ensure loaded-datasets) filename)]
        ds
        (let [ds (delay (load-fn filename))]
          (alter loaded-datasets assoc filename ds)
          ds)))))


(defn read-lines
  [filename, separator]
  (let [file (fs/find-file filename),
        data-lines (mapv #(-> % (str/split separator) vec)
                     ; try to load data via resource or direct file name
                     (some-> file slurp str/split-lines))]
    (when-not file
      (throw (IllegalArgumentException. (format "File %s does not exist!" filename))))
    (when (empty? data-lines)
      (throw (IllegalArgumentException. (format "File %s does not contain anything!" filename))))
    data-lines))


(defn create-sample
  [column]
  {:name (column 0),
   :class (Double/parseDouble (column 1)),
   :gene-expressions (into-array Double/TYPE (map #(Double/parseDouble %) (drop 2 column)))})


(defn load-csv-dataset
  [filename]
  (let [data-lines (read-lines filename, #",")]
    {:gene-ids (->> data-lines (drop 2) (mapv first))
     :samples (->> data-lines (apply map vector) (drop 1) (mapv create-sample))}))

(defn dataset-properties
  [{:keys [samples] :as dataset}]
  {:sample-count (count samples),
   :samples-per-class (->> samples (map :class) frequencies),
   :gene-count (-> samples first :gene-expressions count)})


(defn csv-dataset-by-class
  "Returns the dataset specified by the given filename as a vector containing a vector of samples per class."
  [filename]
  (let [ds (load-dataset load-csv-dataset, filename)]
    (->> ds
      :samples
      (group-by :class)
      (mapv val))))


(defn csv-dataset-sequential
  "Returns the dataset specified by the given filename as a vector of samples.
  THe order of the samples in the vector is identical to the order in the file."
  [filename]
  (let [ds (load-dataset load-csv-dataset, filename)]
    (:samples ds)))


(defn load-folds
  "Loads all folds specifications from the given file.
   [
    ; fold spec 1
    [
     ; fold 1/3
     [[index_{1,1} class_{1,1}], [index_{1,2} class_{1,2}], ...]
     ; fold 2/3
     [[index_{2,1} class_{2,1}], [index_{2,2} class_{2,2}], ...]
     ; fold 3/3
     [[index_{3,1} class_{3,1}], [index_{3,2} class_{3,2}], ...]
    ]
    ; fold spec 2
    [...]
    .
    .
    .
   ]"
  [filename]
  (let [file (fs/find-file filename),
        data-lines (some-> file slurp str/split-lines)]
    (when-not file
      (throw (IllegalArgumentException. (format "File %s does not exist!" filename))))
    (when (empty? data-lines)
      (throw (IllegalArgumentException. (format "File %s does not contain anything!" filename))))
    (->> data-lines
      (partition-by str/blank?)
      (take-nth 2)
      (mapv
        (fn [folds]
          (mapv
            (fn [fold]
              (->> (str/split fold #",")
                (mapv
                  (fn [pair]
                    (mapv #(dec (java.lang.Long/parseLong %)) (rest (re-matches #"\((\d+);(\d+)\)" pair)))))))
            folds))))))