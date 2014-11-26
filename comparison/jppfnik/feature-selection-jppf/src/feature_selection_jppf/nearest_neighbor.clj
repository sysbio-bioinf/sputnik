; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns feature-selection-jppf.nearest-neighbor)


(defprotocol Classifier
  (classify [classifier, sample] "Classifies a given sample."))


(deftype NearestNeighborClassifier [classify-fn, classes-array, train-samples-array]
  Classifier
  (classify [this, sample]
    (classify-fn classes-array, train-samples-array, sample)))


(defn- create-train-samples-array
  [train-samples-vector]
  (let [samples-count (count train-samples-vector),
        train-samples-array ^objects (make-array (Class/forName "[D") samples-count)]
    (loop [i 0]
      (when (< i samples-count)
        (let [{:keys [^doubles gene-expressions]} (nth train-samples-vector i),
              n (alength gene-expressions),
              genes ^doubles (make-array Double/TYPE n)]
          (java.lang.System/arraycopy gene-expressions, 0, genes, 0, n)
          (aset train-samples-array i genes)
          (recur (unchecked-inc i)))))
    train-samples-array))


(defn- create-class-array
  [train-samples-vector]
  (let [samples-count (count train-samples-vector),
        class-array ^objects (make-array Object samples-count)]
    (loop [i 0]
      (when (< i samples-count)
        (let [{:keys [class]} (nth train-samples-vector i)]
          (aset class-array i class)
          (recur (unchecked-inc i)))))
    class-array))


(defn- create-nearest-neighbor-classifier
  [classify-fn, train-samples-vector]
  (let [train-samples-vector (cond-> train-samples-vector (not (vector? train-samples-vector)) vec)]
    (NearestNeighborClassifier.
      classify-fn,
      (create-class-array train-samples-vector),
      (create-train-samples-array train-samples-vector))))



(defn squared-distance
  ^double [^doubles a, ^doubles b]
  (let [size (alength a)]
    (assert (== size (alength b)))
    (loop [i 0, diff-sum 0.0]
      (if (< i size)
        (let [diff (- (aget a i) (aget b i))]
          (recur (unchecked-inc i), (+ diff-sum (* diff diff))))
        diff-sum))))


(defn classify-1-NN
  [^objects classes-array, ^objects train-samples-array, sample]
  (let [n (alength train-samples-array)]
    (loop [i 0, smallest-dist java.lang.Double/POSITIVE_INFINITY, smallest-id -1]
      (if (< i n)
        (let [dist (squared-distance sample, (aget train-samples-array i))]
          (if (< dist smallest-dist)
            (recur (unchecked-inc i), dist, i)
            (recur (unchecked-inc i), smallest-dist, smallest-id)))
        (when (<= 0 smallest-id)
          (aget classes-array smallest-id))))))


(defn ^Classifier build-1-NN
  [train-samples-vector]
  (create-nearest-neighbor-classifier classify-1-NN, train-samples-vector))



(defn test-classifier
  [classifier, test-samples-vector]
  (let [n (count test-samples-vector)]
    (loop [i 0, correct 0, wrong 0]
      (if (< i n)
        (let [{:keys [gene-expressions, class]} (nth test-samples-vector i),
              classified-correctly? (= (classify classifier, gene-expressions) class)]
          (recur (unchecked-inc i),
            (if classified-correctly? (unchecked-inc correct) correct),
            (if classified-correctly? wrong (unchecked-inc wrong))))
        {:count n, :correct correct, :wrong wrong}))))