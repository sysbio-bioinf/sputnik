; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-control.properties
  (:require
    [clojure.string :as string]
    [clojure.java.io :as io])
  (:use
    [clojure.options :only [defn+opts]])
  (:import
    java.util.Properties))


(defn- combined-keyword
  [parent-kw, child-kw]
  (keyword 
    (format "%s.%s" (name parent-kw) (name child-kw))))

(defn- split-keyword
  [kw]
  (-> kw
    name
    (string/split #"\.")
    (->>
      (reduce #(conj! %1 (keyword %2)) (transient []))
      persistent!)))

(defn- flatten-map
  [[k m]]
  (for [[km v] m]
    [(combined-keyword k km) v]))



(defn map->properties-map
  "Creates a properties map from a given map.
  That means a tree of maps is reduced to a single map with hierarchical keywords like:
  {:a {:x 1} :b {:s \"test\", :y 42} => {:a.x 1, :b.s \"test\", :b.y 42}
  This map can be used to create a java properties file."
  [m]
  (loop [kv-pairs m, prop-map (transient {})]
    (if (seq kv-pairs) 
      (let [{map-vals true, atom-vals false} (group-by #(-> % (nth 1) map?) kv-pairs)]
        (recur
          (mapcat flatten-map map-vals)
          (reduce conj! prop-map atom-vals)))
      (persistent! prop-map))))




(defn- split-at-pred
  "Splits the given coll at the position where the predicate pred does not hold for an element the first time.
  Returns [first-part second-part] where first-part is a vector and second-part is a seq, empty vector or not in the result."
  [pred coll]
  (loop [s (seq coll), first-part (transient [])]
    ; if we have elements left, ...
    (if s
      ; ... then examine the first element ...
      (let [f (first s)]
        ; if the predicate holds for the first element, ...
        (if (pred f)
          ; ... then add it to the first part of the split and continue ...
          (recur (next s), (conj! first-part f))
          ; ... else return the first and second part
          [(persistent! first-part) s]))
      ; ... else there is no second part - just return the first part.
      [(persistent! first-part)])))


(defn- vec-last
  "Efficient access to the last element of a given vector."
  [v]
  (peek v))

(defn- group-vec->map
  "Builds a map of the given keyword prefix group where only the last keyword of the keyword vector is used.

  Example:
  input: [[[:a :b :x] v1] [[:a :b :y] v2]  [[:a :b :z] v3]
  output: {:x v1, :y v2, :z v3}"
  [v]
  (persistent!
    (reduce
      (fn [m [k v]]
        (assoc! m (vec-last k) v))
      (transient {})
      v)))




(defn map->map-tree
  "Creates a tree of maps from a mixed map structure that contains tree of maps or hierachical keywords.
  This map is better suited for inspection by a user."
  [m]
  (let [; sorted list of keyword-vector value pairs by length of the keyword-vector
        kwvec-value-pairs (->> m map->properties-map (map (fn [[k v]] [(split-keyword k) v])) (sort-by #(-> % first count) >)),
        max-kwvec-count (-> kwvec-value-pairs first first count)]
    ; kwvec-value-pairs need to be sorted by length of the keyword-vector for correctness!
    (loop [kwvec-value-pairs kwvec-value-pairs, n max-kwvec-count]
      (if (= n 1)
        ; every key-value pair has a keyword-vector of size 1 only -> build the final map
        (persistent! (reduce (fn [m [kvec v]] (assoc! m (first kvec) v)) (transient {}) kwvec-value-pairs))
        (let [[max-length-pairs remaining] (split-at-pred #(= n (-> % first count)) kwvec-value-pairs),
              n-1 (dec n),
              ; group key-value pairs by n-1 prefix of the key
              prefix-grouped (group-by #(-> % first (subvec 0 n-1)) max-length-pairs)
              ; build a collection of prefix-key and map of the group
              prefix-key-map-pairs (map (fn [[prefix-key group-vec]] [prefix-key (group-vec->map group-vec)]) prefix-grouped)]
          ; add the created key-map-pairs in front of the remaining key-value pairs and continue with n-1
          (recur (concat prefix-key-map-pairs remaining), n-1))))))



(defn+opts write-properties
  "Writes the given properties-map to the file with the given filename.
  The given map must have the structure of the return value of the function `map->properties-map`.
  <comment>A comment string that is added to the begin of the file.</comment>"
  [url, m | {comment nil}]
  (let [props (Properties.)]
    (doseq [[k v] m]
      (when-not (nil? v)
        (.setProperty props (name k) (str v))))
    (with-open [w (io/writer url)]
      (.store props w comment))))