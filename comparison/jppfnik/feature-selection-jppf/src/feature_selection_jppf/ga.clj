; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns feature-selection-jppf.ga
  (:require
    [clojure.string :as str]
    [clojure.stacktrace :as st]
    [clojure.options :refer [defn+opts]]
    [jppfnik-tools.format :as fmt]
    [feature-selection-jppf.analysis :as a]
    [feature-selection-jppf.bitset :as b]
    [feature-selection-jppf.tools :as t]))


(defn create-rand-fn
  [seed]
  (let [rnd (java.util.Random. seed)]
    (fn
      ([] (.nextDouble rnd))
      ([x] (* x (.nextDouble rnd))))))



(defn rescale-fitness
  [alpha, fitness-values]
  (if (== 1 (count (distinct fitness-values)))
    fitness-values
    (let [fitness-values (vec (sort > fitness-values)),        
         n (count fitness-values),
         max-fitness (fitness-values 0),
         min-fitness (fitness-values (dec n)),
         fitness-range (- max-fitness min-fitness),
         alpha*n-1 (dec (* alpha n)),
         slope (/ alpha*n-1 fitness-range),
         addend (- 1 (* slope min-fitness))]
     (mapv
       (fn [x] (+ (* slope x) addend))
       fitness-values))))


(defn+opts generate-random-individual
  [rand-fn, individual-size | {one-probability 0.01}]
  (let [individual-size (long individual-size),
        one-probability (double one-probability)]
    (loop [i 0, bs (b/writable-bitset individual-size)]
      (if (< i individual-size)
        (recur (unchecked-inc i), (b/set-bit-to bs, i, (< (rand-fn) one-probability)))
        {:selected-genes (b/make-readonly bs)
         :selected-genes-count (b/cardinality bs)}))))


(defn all-genes-selected-individual
  [gene-count]
  (-> (b/writable-bitset gene-count)
    (b/set-bits 0 gene-count)
    b/make-readonly))


(defn choose-random-element
  ^long [rand-fn, cdf]
  (let [sum (peek cdf),
        rnd (rand-fn sum),
        n (count cdf)]
    (loop [idx 0]
      (if (< idx n)
        (let [v (nth cdf idx)]
	        (if (<= v rnd)	          
	          (recur (unchecked-inc idx))
	          idx))
        ; signal error
        Long/MAX_VALUE))))


(defn+opts fitness-values
  [population | {rescale-factor nil}]
  (cond->> (map :fitness population) rescale-factor (rescale-fitness rescale-factor)))


(defn+opts selection
  [population, select-count, rand-fn | :as options]
  (let [fitness-values (fitness-values population, options)
        cdf (vec (rest (reductions + 0.0 fitness-values))),
        n (long select-count)]
    (loop [i 0, selected (transient [])]
      (if (< i n)
        (let [rnd-index (choose-random-element rand-fn, cdf)]
          (recur (unchecked-inc i), (conj! selected (nth population rnd-index))))
        (persistent! selected)))))


(defn+opts crossover
  [population, rand-fn, individual-size | {crossover-probability 0.05}]
  (let [n (count population)
        n-1 (dec n)]
    (loop [i 0, children (transient [])]
      (cond
        (< i n-1) (let [p1 (nth population i),
                        p2 (nth population (inc i))]
                    (if (< (rand-fn) crossover-probability)                      
                      (let [pos (unchecked-inc (long (rand-fn (unchecked-subtract individual-size 2))))
                            [c1 c2] (b/one-point-crossover (:selected-genes p1), (:selected-genes p2), pos),
                            c1 {:selected-genes c1},
                            c2 {:selected-genes c2}]
                        (recur (unchecked-add i 2), (-> children (conj! c1) (conj! c2))))
                      (recur (unchecked-add i 2), (-> children (conj! p1) (conj! p2)))))
        (< i n)   (persistent! (conj! children (nth population i)))
        :else     (persistent! children)))))



(defn add-gene
  [rand-fn, individual-size, selected-genes]
  (let [selected-gene-count (b/cardinality selected-genes),
        n (- individual-size selected-gene-count),
        ; random gene position among unselected genes
        pos (long (rand-fn n))]
    (try 
      (cond-> selected-genes (< selected-gene-count individual-size) (b/set-bit (b/nth-clear-bit selected-genes, pos)))
      (catch Throwable t
        (println "Exception in add-gene:" (.getMessage t))
        (println (format "Bitset (%s): %s\n#genes = %s   n = %s   pos = %s" individual-size, selected-genes, selected-gene-count, n, pos))
        (flush)
        (throw t)))))


(defn remove-gene
  [rand-fn, individual-size, selected-genes]
  (let [selected-gene-count (b/cardinality selected-genes)
        ; random gene position among selected genes
        pos (long (rand-fn selected-gene-count))]
    (try 
      (cond-> selected-genes (< 1 selected-gene-count) (b/clear-bit (b/nth-set-bit selected-genes, pos)))
      (catch Throwable t
        (println "Exception in remove-gene:" (.getMessage t))
        (println (format "Bitset (%s): %s\n#genes = %s   pos = %s" individual-size, selected-genes, selected-gene-count, pos))
        (flush)
        (throw t)))))


(defn+opts mutate
  [generation, rand-fn, individual-size, individual | {flip-one-probability 0.5, flip-zero-probability 0.5}]
  (-> individual
    :selected-genes
    b/make-writable
    (cond->>
      (< (rand-fn) flip-zero-probability) (add-gene rand-fn, individual-size)
      (< (rand-fn) flip-one-probability)  (remove-gene rand-fn, individual-size))
    b/make-readonly
    (->>
      (hash-map :selected-genes))))



(defn+opts mutation
  [population, generation, rand-fn, individual-size | :as options]
  (mapv #(mutate generation, rand-fn, individual-size, %, options) population))


(defn update-selected-genes-count
  [population]
  (mapv
    (fn [{:keys [selected-genes] :as individual}]
      (assoc individual :selected-genes-count (b/cardinality selected-genes)))
    population))



(defn+opts evolve
  [generation, rand-fn, evaluate-population-fn, individual-size, population | {keep-best 0.05} :as options]
  (let [n (count population),
        to-keep (long (* keep-best n)),
        best-individuals (vec (take to-keep (sort a/sort-by-fitness population)))
        evaluated-individuals (-> population
                                (selection n, rand-fn, options)
                                (crossover rand-fn, individual-size, options)
                                (mutation  generation, rand-fn, individual-size, options)
                                update-selected-genes-count
                                evaluate-population-fn)]
    ; propagate metadata from evaluation
    (with-meta
      ; replace worst indiviuals by previous best ones
      (into
        (->> evaluated-individuals (sort a/sort-by-fitness) (take (- n to-keep)) vec)
        best-individuals)
      (meta evaluated-individuals))))



(defn+opts selection-entropy
  [population | :as options]
  (let [fitness-values (fitness-values population, options),
        fitness-sum (reduce + fitness-values),
        probabilities (mapv #(/ % fitness-sum) fitness-values),
        log2 (Math/log 2)]
    (-
      (reduce +
        (map #(if (zero? %) 0.0 (* % (/ (Math/log %) log2))) probabilities)))))




(defn+opts print-progress
  [g, population | :as options]
  (t/output-future
    (try
      (let [fitness-values (map :fitness population),
            selected-genes-coll (map :selected-genes population),       
            cardinality-coll (map b/cardinality selected-genes-coll),
            best-fitness-individual  (first (sort a/sort-by-fitness  population)),
            duration (-> population meta :duration)]
        (println
          (format (str "\nGeneration %03d:    selection entropy = %.4f"
                    "\n  fitness:         AVG = %10.4f, MIN = %10.4f, MAX = %10.4f"
                    "\n  selected genes:  AVG = %10.4f, MIN = %10d, MAX = %10d"
                    "\n  Best Individual:"
                    "\n    Fitness:  FITNESS = %.4f, #(SELECTED GENES) = %5d")
            g,
            (selection-entropy population, options),
            (a/mean fitness-values),
            (reduce min fitness-values),
            (reduce max fitness-values),
            (a/mean cardinality-coll),
            (reduce min cardinality-coll),
            (reduce max cardinality-coll),
            (:fitness best-fitness-individual), (b/cardinality (:selected-genes best-fitness-individual))))
        (println (format "\nEvaluation duration: %s (%d ms)" (fmt/duration-with-days-format duration), duration))
        (flush))
      (catch Throwable t
        (println "Error in print-progress:")
        (st/print-cause-trace t)))))



(defn evaluation-runtime
  ^long [population]
  (-> population meta :duration))


(defn print-evaluation-runtime
  [^long total-evaluation-runtime]
  (t/output-future
    (println (format "\nTotal population evaluation runtime: %s (%s ms)" (fmt/duration-with-days-format total-evaluation-runtime), total-evaluation-runtime))
    (flush)))


(defn+opts genetic-algorithm
  "Runs a genetic algorithm which returns the final population.
  evaluate-population-fn must return a list of individuals with :fitness and :selected-genes attributes"
  [freezer, rand-fn, evaluate-population-fn, individual-size | {population-size 100, generation-count 100, progress false} :as options]
  (let [init-population (evaluate-population-fn (repeatedly population-size #(generate-random-individual rand-fn, individual-size, options)))]
    (when progress
      (print-progress 0, init-population, options))
    (a/maybe-freeze init-population, 0, freezer)
    (loop [g 0, population init-population, total-evaluation-runtime (evaluation-runtime init-population)]
      (if (< g generation-count)
        (let [g+1 (unchecked-inc g),
              population (evolve g+1, rand-fn, evaluate-population-fn, individual-size, population, options)]
          (when progress
            (print-progress g+1, population, options))
          (a/maybe-freeze population, g+1, freezer)
          (recur g+1, population, (+ total-evaluation-runtime (evaluation-runtime population))))
        (do
          (print-evaluation-runtime total-evaluation-runtime)
          population)))))