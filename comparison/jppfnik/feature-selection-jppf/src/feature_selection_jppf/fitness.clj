; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns feature-selection-jppf.fitness
  (:require
    [clojure.set :as set]
    [clojure.options :refer [defn+opts]]
    [jppfnik-tools.functions :as r]
    [feature-selection-jppf.bitset :as b]
    [feature-selection-jppf.merit :as m]
    [feature-selection-jppf.nearest-neighbor :as nn]))



(defn knuth-shuffle
  [rand-fn, coll]
  (let [coll (transient (if (vector? coll) coll (vec coll))),
        n (count coll)]
    (loop [i (dec n), coll coll]
      (if (< 1 i)
        (let [j (long (rand-fn (inc i)))
              x_i (coll i)
              x_j (coll j)]
          (recur (unchecked-dec i), (assoc! coll, i x_j, j x_i)))
        (persistent! coll)))))


(defn partitionv
  [^long n, coll]
  (let [coll (if (vector? coll) coll (vec coll)),
        size (long (count coll))]
    (loop [i 0, result-vec (transient [])]
      (if (< i size)
        (recur
          (unchecked-add i n),
          (conj! result-vec
            (loop [j 0, partition-vec (transient [])]
              (let [from (unchecked-add i j)]
                (if (and (< j n) (< from size))
                  (recur
                    (unchecked-inc j),
                    (conj! partition-vec (nth coll from)))
                  (persistent! partition-vec))))))
        (persistent! result-vec)))))


(defn equal-partitions
  [rand-fn, k, data-coll]
  (let [n (count data-coll)
        item-count (quot n k)
        more-items-classes (mod n k),
        first-items-count (* (inc item-count) more-items-classes),
        data-coll (knuth-shuffle rand-fn, data-coll)]
    (knuth-shuffle rand-fn,
      (into
        (into [] (subvec (partitionv (inc item-count) data-coll) 0 more-items-classes))
        (partitionv item-count (subvec data-coll first-items-count))))))


(defn replace-data-with-coordinates
  "Produces a coordinate partition vector by Replacing the data with its coordinates [outer, inner] such that the data can be retrieved
  via (get-in class-ds-vector [outer, inner])."
  [class-ds-vector]
  (let [n (count class-ds-vector)]
    (loop [outer-pos 0, result-vec (transient [])]
      (if (< outer-pos n)
        (recur
          (unchecked-inc outer-pos),
          (conj! result-vec
            (let [class-ds (nth class-ds-vector outer-pos),
                  m (count class-ds)]
              (loop [inner-pos 0, result-vec (transient [])]
                (if (< inner-pos m)
                  (recur
                    (unchecked-inc inner-pos),
                    (conj! result-vec [outer-pos, inner-pos]))
                  (persistent! result-vec))))))
        (persistent! result-vec)))))


(defn replace-coordinates-with-data
  "Uses the given coordinate partition vector (which represents a k-fold-partitioning with equal class distribution) to create the specified 
  k-fold-partitioning of the data."
  ([coord-partition-vector, class-ds-vector]
    (replace-coordinates-with-data coord-partition-vector, class-ds-vector, identity))
  ([coord-partition-vector, class-ds-vector, transformation-fn]
  (mapv
    (fn [coord-partition]
      (mapv
        (fn [coord]
          (try (transformation-fn (get-in class-ds-vector coord))
            (catch Throwable t
              (println coord))))
        coord-partition))    
    coord-partition-vector)))


(defn cross-validation-setup
  [rand-fn, class-ds-vector]
  (let [three-partitions-per-class (mapv (partial equal-partitions rand-fn, 3) class-ds-vector)]
    {:train-instances (->> three-partitions-per-class (mapcat #(take 2 %)) (apply concat) vec),
     :test-instances (->> three-partitions-per-class (map last) (apply concat) vec)}))


(defn- extract-indices
  [sample-vector]
  (mapv (comp vector first) sample-vector))


(defn defined-folds->train-instances
  [defined-folds]
  (->> defined-folds (take 2) (apply concat) extract-indices))

(defn defined-folds->test-instances
  [defined-folds]
  (->> defined-folds last extract-indices))

(defn defined-folds->train-instances-by-class
  [defined-folds]
  (->>  defined-folds (take 2) (apply concat) (group-by second) vals (mapv extract-indices)))


(defn cross-validation-setup-given-folds
  "Create cross validation setup for the given folds.
  defined-folds: [[[a_1, class_a1], [a_2, class_a2], ...] [[b_1, class_b1], [b_2, class_b2], ...] [[c_1, class_c1], [c_2, class_c2], ...]]
  training: a_i, b_i
  test: c_i"
  [rand-fn, defined-folds]
  {:train-instances (defined-folds->train-instances defined-folds),
   :test-instances (defined-folds->test-instances defined-folds)})



(defn select-gene-expressions
  [^doubles gene-expressions, individual]
  (let [selected-genes individual,
        n (b/cardinality selected-genes)
        selected (double-array n)]
    (loop [i 0, j (b/next-set-bit selected-genes, 0)]
      (if (<= 0 j)
        (do
          (aset-double selected i (aget gene-expressions j))
          (recur (unchecked-inc i), (b/next-set-bit selected-genes, (unchecked-inc j))))
        selected))))



(defn all-but
  "Returns the original sequence except excluding its i-th element."
  [i, coll]
  (keep-indexed (fn [j, x] (when-not (= i j) x)) coll))


(defn select-train-instances
  [dataset-partitions-selected-features, i]
  (apply concat (all-but i dataset-partitions-selected-features)))


(defn select-test-instances
  [dataset-partitions-selected-features, i]
  (nth dataset-partitions-selected-features i))


(defn error
  [throw?, fmt & args]
  (let [msg (apply format fmt args)]
    (if throw?
      (throw (IllegalArgumentException. ^String msg))
      (println "ERROR:" msg))))


(defn check-cv-partitions
  [throw?, dataset-partitions, train-instances, test-instances]
  (let [all-instances (set (apply concat dataset-partitions)),
        train-instances (set train-instances),
        test-instances (set test-instances)]
    (when-not (empty? (set/intersection train-instances test-instances))
      (error throw? "Train instances and set instances are not disjoint!"))
    (when-not (= (set/union train-instances test-instances) all-instances)
      (error throw? "Not all instances are partitioned into the train and test sets!"))))



(defn evaluate-feature-selection
  "Evaluates the feature selection represe"
  [load-datasets-fn, file-list, sample-list, individual-long-array]
  (let [selected-genes (b/readonly-bitset-from-long-array individual-long-array), ; convert to bitset
        selected-gene-count (b/cardinality selected-genes)]
    (if (== selected-gene-count 0)
      {:fitness 0.0}
      (let [; load data set
            load-datasets-fn (r/resolve-fn load-datasets-fn),
            _ (when-not load-datasets-fn (throw (IllegalArgumentException. (format "Symbol %s given must specify a function to load the dataset but does not resolve to a function!" load-datasets-fn))))
            sample-vector (load-datasets-fn file-list),
            chosen-samples (mapv #(get-in sample-vector %) sample-list)]
        {:fitness (m/merit-fitness chosen-samples, selected-genes)}))))



(defn test-feature-selection
  "Evaluates the feature selection represe"
  [train-instance-coords, test-instance-coords, load-datasets-fn, file-list, individual-long-array]
  (let [individual (b/readonly-bitset-from-long-array individual-long-array), ; convert to bitset
        selected-gene-count (b/cardinality individual)]
    (if (== selected-gene-count 0)
      {:test-accuracy 0.0}
      (let [; load data set
            load-datasets-fn (r/resolve-fn load-datasets-fn),
            _ (when-not load-datasets-fn (throw (IllegalArgumentException. (format "Symbol %s given must specify a function to load the dataset but does not resolve to a function!" load-datasets-fn))))
            class-ds-vector (load-datasets-fn file-list),
            ; create partitions for k-fold cross validation
            create-instances (fn [instance-list] (first
                                                   (replace-coordinates-with-data
                                                     (vector instance-list),
                                                     class-ds-vector,
                                                     #(update-in % [:gene-expressions] select-gene-expressions, individual)))),
            train-samples (create-instances train-instance-coords),
            test-samples (create-instances test-instance-coords),
            classifier (nn/build-1-NN train-samples),
            {:keys [correct, count]} (nn/test-classifier classifier, test-samples)]
        (let [accuracy (/ (double correct) count)]
          {:test-accuracy accuracy})))))



(defn local-evaluation
  [load-datasets-fn, file-list, sample-list, population]
  (let [begin-time (System/currentTimeMillis),
        evaluated-population (mapv
                               (fn [individual]
                                 (merge
                                     individual
                                     (evaluate-feature-selection load-datasets-fn, file-list, 
                                       sample-list, (b/to-long-array (:selected-genes individual)))))
                               population),
        end-time (System/currentTimeMillis),
        duration (- end-time begin-time)]
    (with-meta evaluated-population
      {:duration duration})))


(defn local-test
  [load-datasets-fn, file-list, train-instance-coords, test-instance-coords, population]
  (let [begin-time (System/currentTimeMillis),
        tested-population (mapv
                            (fn [individual]
                              (merge
                                individual
                                (test-feature-selection train-instance-coords, test-instance-coords, load-datasets-fn, file-list, (b/to-long-array (:selected-genes individual)))))
                            population),
        end-time (System/currentTimeMillis),
        duration (- end-time begin-time)]
    (with-meta tested-population
      {:duration duration})))