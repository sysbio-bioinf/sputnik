; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns feature-selection-jppf.merit
  (:require
    [feature-selection-jppf.bitset :as bs]))



(defn value-at
  {:inline (fn [samples, sample-id, gene-id] `(aget ^"[D" (nth ~samples ~sample-id) ~gene-id))}
  ^double [samples, ^long sample-id, ^long gene-id]
  (aget ^doubles (nth samples sample-id) gene-id))


(defn sum
  ^double [samples, ^long gene-id]
  (let [n (count samples)]
    (loop [i 0, sum 0.0]
      (if (< i n)
        (recur
          (unchecked-inc i),
          (let [x (value-at samples, i, gene-id)]
            (+ sum x)))
        sum))))


(defn squared-sum
  ^double [samples, ^long gene-id]
  (let [n (count samples)]
    (loop [i 0, squared-sum 0.0]
      (if (< i n)
        (recur
          (unchecked-inc i),
          (let [x (value-at samples, i, gene-id)]
            (+ squared-sum (* x x))))
        squared-sum))))




(defmacro doarray
  [[value, index, array], & body]
  `(let [a# ~array,
         n# (alength a#)]
     (loop [i# 0]
       (when (< i# n#)
         (let [~index i#
               ~value (aget a# i#)]
           ~@body)
         (recur (unchecked-inc i#))))))


(defn gene-id-array
  ^longs [selected-genes]
  (let [n (bs/cardinality selected-genes),
        gene-ids (make-array Long/TYPE n)]
    (bs/reduce-over-set-bits [gid selected-genes] [i 0]
      (aset-long gene-ids i gid)
      (unchecked-inc i))
    gene-ids))


(defn class-mean
  ^double [^doubles classes-array]
  (let [n (alength classes-array)]
    (loop [i 0, sum 0.0]
      (if (< i n)
        (recur (unchecked-inc i), (+ sum (aget classes-array i)))
        (/ sum n)))))


(defn class-sd
  ^double [^doubles classes-array, ^double mean-class-value]
  (let [n (alength classes-array)]
    (loop [i 0, sum 0.0]
      (if (< i n)
        (let [class (aget classes-array i)
              diff (- class mean-class-value)]
          (recur (unchecked-inc i), (+ sum (* diff diff))))
        (Math/sqrt (/ sum (dec n)))))))


(defn feature-mean
  ^double [samples, ^long gene-id]
  (let [n (count samples)]
    (loop [i 0, sum 0.0]
      (if (< i n)
        (recur
          (unchecked-inc i),
          (let [x (value-at samples, i, gene-id)]
            (+ sum x)))
        (/ sum n)))))


(defn feature-mean-values
  ^doubles [samples, ^longs gene-id-array]
  (let [n (alength gene-id-array)
        mean-array (make-array Double/TYPE n)]
    (doarray [gene-id, i, gene-id-array]
      (aset-double mean-array, i, (feature-mean samples, gene-id)))
    mean-array))


(defn feature-sd
  ^double [samples, ^double mean, ^long gene-id]
  (let [n (count samples)]
    (loop [i 0, sum 0.0]
      (if (< i n)
        (recur
          (unchecked-inc i),
          (let [x (value-at samples, i, gene-id)
                diff (- x mean)]
            (+ sum (* diff diff))))
        (Math/sqrt (/ sum (unchecked-dec n)))))))


(defn feature-sd-values
  ^doubles [samples, ^longs gene-id-array, ^doubles mean-values]
  (let [n (alength gene-id-array),
        results (make-array Double/TYPE n)]
    (doarray [gene-id, i, gene-id-array]
      (aset-double results, i, (feature-sd samples, (aget mean-values i), gene-id)))
    results))



(definterface IMeritCalculation
  (^double feature_covariance [^long i, ^long j])
  (^double feature_abs_correlation_coefficient [^long i, ^long j])
  (^double feature_correlation_sum [^long i])
  (^double average_feature_correlation [])
  (^double class_feature_covariance [^long i])
  (^double class_feature_abs_correlation_coefficient [^long i])
  (^double class_feature_correlation_sum [])
  (^double average_class_feature_correlation [])
  (^double merit []))


(deftype MeritCalculation [samples, gene-id-array, classes-array, ^long sample-count, ^long feature-count,
                           ^doubles feature-mean-array, ^doubles feature-sd-array, 
                           ^double class-mean-value, ^double class-sd-value]
  IMeritCalculation
  
  (feature-covariance [this, i, j]
    (let [gene-i (aget gene-id-array i),
          gene-j (aget gene-id-array j),
          mean-i (aget feature-mean-array i),
          mean-j (aget feature-mean-array j)]
    (loop [sample-id 0, prod-sum 0.0]
      (if (< sample-id sample-count)
        (let [sample ^doubles (nth samples sample-id)
              xi (aget sample gene-i),
              xj (aget sample gene-j)]
          (recur
            (unchecked-inc sample-id),
            (+ prod-sum (* (- xi mean-i) (- xj mean-j)))))
        (/ prod-sum (unchecked-dec sample-count))))))
  
  (feature-abs-correlation-coefficient [this, i, j]
    (let [sd-i (aget feature-sd-array i),
          sd-j (aget feature-sd-array j)]
      (Math/abs
        (/
          (.feature-covariance this, i, j)
          (* sd-i sd-j)))))
  
  (feature-correlation-sum [this, i]    
    (loop [j 0, sum 0.0]
      (if (<= j i)
        (recur
          (unchecked-inc j),
          (let [c (cond-> (.feature-abs-correlation-coefficient this, i, j)
                    (< j i) (* 2.0))]
            (+ sum c)))
        sum)))
  
  (average-feature-correlation [this]
    (loop [i 0, sum 0.0]
      (if (< i feature-count)
        (recur
          (unchecked-inc i),
          (+ sum (.feature-correlation-sum this, i)))
        (/ sum (* feature-count feature-count)))))
  
  
  (class-feature-covariance [this, i]
    (let [feature-gene (aget gene-id-array i),          
          feature-mean-value (aget feature-mean-array i)]
    (loop [sample-id 0, prod-sum 0.0]
      (if (< sample-id sample-count)
        (let [sample ^doubles (nth samples sample-id)
              feature-value (aget sample feature-gene),
              class-value   (aget classes-array sample-id)]
          (recur
            (unchecked-inc sample-id),
            (+ prod-sum (* (- feature-value feature-mean-value) (- class-value class-mean-value)))))
        (/ prod-sum (unchecked-dec sample-count))))))
  
  (class-feature-abs-correlation-coefficient [this, i]
    (let [feature-sd-value (aget feature-sd-array i)]
      (Math/abs
        (/
          (.class-feature-covariance this, i,)
          (* feature-sd-value class-sd-value)))))
  
  
  (average-class-feature-correlation [this]
    (loop [i 0, sum 0.0]
      (if (< i feature-count)
        (recur
          (unchecked-inc i),
          (+ sum (.class-feature-abs-correlation-coefficient this, i)))
        (/ sum feature-count))))
  
  
  (merit [this]
    (let [k feature-count]
      (/
        (* k (.average-class-feature-correlation this))
        (Math/sqrt
          (+ k
            (* k (unchecked-dec k) (.average-feature-correlation this))))))))


(defn ^MeritCalculation merit-calculation
  [samples, selected-genes]
  (let [gene-expressions (mapv :gene-expressions samples),
        classes-array (into-array Double/TYPE (map :class samples)),
        class-mean-value (class-mean classes-array),
        class-sd-value (class-sd classes-array, class-mean-value),
        gene-id-array (gene-id-array selected-genes),
        feature-mean-values (feature-mean-values gene-expressions, gene-id-array),
        feature-sd-values (feature-sd-values gene-expressions, gene-id-array, feature-mean-values)]
     (MeritCalculation. gene-expressions, gene-id-array, classes-array, (count samples), (alength gene-id-array),
       feature-mean-values, feature-sd-values, class-mean-value, class-sd-value)))


(defn merit-fitness
  ^double [samples, selected-genes]
  (.merit (merit-calculation samples, selected-genes)))