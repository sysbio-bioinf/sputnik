; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns feature-selection-jppf.analysis
  (:require
    [clojure.string :as str]
    [clojure.options :refer [defn+opts, options->>]]
    [frost.quick-freeze :as qf]
    [frost.file-freezer :as ff]
    #_[pareto.core :as p]
    [feature-selection-jppf.bitset :as b]
    [feature-selection-jppf.datasets :as ds]))


(defn mean
  [coll]
  (/ (reduce + coll)
    (double (count coll))))


(defn square
  [x]
  (* x x))


(defn variance  
  [coll]
  (when (seq coll)
	  (- 
	    (mean (mapv square coll))
	    (square (mean coll)))))


(defn standard-deviation
  [coll]
  (when (seq coll)
    (Math/sqrt (variance coll))))


(defn update-population
  [population, f]
  (with-meta
    (mapv f population)
    (meta population)))


(defn selected-genes->long-array
  [individual]
  (update-in individual [:selected-genes] b/to-long-array))


(defn long-array->selected-genes
  [individual]
  (update-in individual [:selected-genes] b/readonly-bitset-from-long-array))


(defn export-population-data
  [population-data, filename]
  (when filename
    (qf/quick-file-freeze filename,
      (-> population-data
        (update-in [:all-populations] #(mapv (fn [population] (update-population population selected-genes->long-array)) %))
        (update-in [:final-population] update-population selected-genes->long-array)
        (update-in [:tested-final-population] update-population selected-genes->long-array))))
  population-data)


(defn maybe-freeze
  [population, generation, freezer]
  (when freezer
    (ff/freeze freezer,
      (-> population
        (update-population selected-genes->long-array)
        (vary-meta assoc :generation generation))))
  population)


(defn remove-quotes
  [gene-ids]
  (mapv #(or (second (re-find #"\"(.*)\"" %)) %) gene-ids))

(defn retrieve-unigenes
  [gene-ids, selected-genes]
  (let [gene-ids (remove-quotes gene-ids)]
    (mapv
      #(nth gene-ids %)
      (b/vector-of-set-bits selected-genes))))


(defn print-population-table
  [sort-attr, n, population]
  (println (apply format (str "%10s %10s %10s %10s %10s") ["Train Fit" "Test Fit" "Train Acc" "Test Acc" "#genes"]))
  (doseq [[i {:keys [selected-genes, fitness, accuracy, test-fitness, test-accuracy]}] (mapv vector (range) (sort-by sort-attr (comp - compare) population))
          :while (< i n)]
    (println
      (format "%10.4f %10.4f %10.4f %10.4f %10d" fitness, test-fitness, accuracy, test-accuracy, (b/cardinality selected-genes)))))


(defn print-population-table-genes
  [sort-attr, n, population, gene-vec]
  (let [gene-vec (remove-quotes gene-vec)]
    (println (apply format (str "%10s %10s %10s %10s %10s %s") ["Train Fit" "Test Fit" "Train Acc" "Test Acc" "#genes" "selected genes"]))
    (doseq [[i {:keys [selected-genes, fitness, accuracy, test-fitness, test-accuracy]}] (mapv vector (range) (sort-by sort-attr (comp - compare) population))
            :while (< i n)]
      (println
        (format "%10.4f %10.4f %10.4f %10.4f %10d %s" fitness, test-fitness, accuracy, test-accuracy, (b/cardinality selected-genes)
          (->> (b/vector-of-set-bits selected-genes) (map gene-vec) (str/join ", ")))))))

(defn print-individuals-genes
  [sort-attr, n, population, gene-vec]
  (let [gene-vec (mapv #(or (second (re-find #"\"(.*)\"" %)) %) gene-vec)]    
    (doseq [[i {:keys [selected-genes]}] (mapv vector (range) (sort-by sort-attr (comp - compare) population))
            :while (< i n)]
      (println (->> (b/vector-of-set-bits selected-genes) (map gene-vec) (str/join ", "))))))


(defn print-population-sorted
  ([sort-attr, n, population]
    (print-population-sorted sort-attr, n, population, nil))
  ([sort-attr, n, population, gene-ids]
    (doseq [[i {:keys [selected-genes, fitness, accuracy, test-fitness, test-accuracy]}] (map vector (range) (sort-by sort-attr > population))
            :while (< i n)]
        (println
          (format
            (str
              "\nIndividual %4d:\n"
              " test-fitness            = %8.4f train-fitness            = %8.4f\n"
              " test-accuracy           = %8.4f train-accuracy           = %8.4f\n"
              " #(selected genes) = %8d\n"
              (when gene-ids " selected genes: %s"))
            (inc i), test-fitness, fitness, test-accuracy, accuracy, (b/cardinality selected-genes),
            (when gene-ids (retrieve-unigenes gene-ids, selected-genes)))))))



(defn load-population-data-map
  [filename]
  (let [population-data (qf/quick-file-defrost filename)]
    (-> population-data
        (update-in [:all-populations] #(mapv (fn [population] (update-population population long-array->selected-genes)) %))
        (update-in [:final-population] update-population long-array->selected-genes)
        (update-in [:tested-final-population] update-population long-array->selected-genes))))


(defn add-selected-gene-count-to-population
  [population]
  (update-population
    population
    (fn [{:keys [selected-genes] :as individual}]
      (assoc individual :selected-genes-count (b/cardinality selected-genes)))))


#_(defn+opts pareto-population
   [population | {attributes [[:accuracy >] [:fitness >] [:selected-genes-count <]]}]
   (apply p/pareto-front-map (add-selected-gene-count-to-population population) attributes))


(defn load-population-data-vector
  [filename]
  (let [population-data (qf/quick-file-defrost-coll filename)]
    (mapv #(update-population % long-array->selected-genes) population-data)))


(defn load-tested-final-population
  [filename]
  (some-> (qf/quick-file-defrost-coll filename, :filter-pred #(-> % meta :generation (= -1)))
    first
    (update-population long-array->selected-genes)))

(defn+opts tested-final-population
  [filename | {unique false, data-map false}]
  (let [load-final-population-fn (if data-map
                                   (comp :tested-final-population load-population-data-map)
                                   load-tested-final-population)]
    (cond->
      (->> filename
        load-final-population-fn
        add-selected-gene-count-to-population)
      unique distinct)))


(defn best-final-individual
  [comparator, final-population]
  (first (sort comparator final-population)))


(defn+opts best-final-individual-file
  [comparator, filename | :as options]
  (best-final-individual comparator, (tested-final-population filename, options)))



(defn sort-by-fitness
  [x y]
  (if (= (:fitness x) (:fitness y))
    (< (:selected-genes-count x) (:selected-genes-count y))
    (> (:fitness x) (:fitness y))))

(defn sort-by-accuracy
  [x y]
  (if (= (:accuracy x) (:accuracy y))
    (< (:selected-genes-count x) (:selected-genes-count y))
    (> (:accuracy x) (:accuracy y))))


(defn+opts measures-best-final-individuals
  [attribute, comparator, filenames | :as options]
  (zipmap [:mean, :standard-deviation]
    ((juxt mean standard-deviation) (mapv (comp #(get % attribute) #(best-final-individual comparator, %, options)) filenames))))

(comment
  (measures-best-final-individuals :test-accuracy, ga/sort-by-fitness  (map #(format "%d-shipp-pop.data" %) (range 1 11)) )
  (measures-best-final-individuals :test-accuracy, ga/sort-by-accuracy (map #(format "%d-shipp-pop.data" %) (range 1 11)))
)


(defn print-key-return-val
  [best-attribute, pair]
  (println best-attribute "=" (first pair))
  (second pair))


(defn+opts best-attribute-individuals
  [best-attribute, best-comparator, filenames | :as options]
  (->> filenames
    (mapcat #(options->> %, options, tested-final-population (group-by best-attribute) (sort-by key best-comparator) first (print-key-return-val best-attribute)))
    vec))

(defn+opts measures-best-attribute-individuals
  [measure-attribute, best-attribute, best-comparator, filenames | :as options]
  (->> (best-attribute-individuals best-attribute, best-comparator, filenames, options)
    (mapv measure-attribute)
    ((juxt mean standard-deviation))
    (zipmap [:mean, :standard-deviation])))


(defn measures-all-final-individuals
  [attribute, final-populations]
  (->> final-populations
    (apply concat)
    (map #(get % attribute))
    ((juxt mean standard-deviation))
    (zipmap [:mean, :standard-deviation])))

(defn+opts measures-all-final-individuals-files
  [attribute, filenames | :as options]
  (->> filenames
    (mapv #(tested-final-population %, options))
    measures-all-final-individuals))



(defn compute-best-individual-measures
  [sort-comparator, attributes, final-populations]
  (let [attributes (if (sequential? attributes) attributes [attributes]),
        best-individuals (mapv (partial best-final-individual sort-comparator) final-populations)]
    (reduce
      (fn [m, attr]
        (let [xs (mapv attr best-individuals)]
          (assoc m attr {:mean (mean xs), :standard-deviation (standard-deviation xs)})))
      {}
      attributes)))


(defn compute-all-individual-measures
  [attributes, final-populations]
  (let [attributes (if (sequential? attributes) attributes [attributes])]
    (reduce
      (fn [m, attr]
        (assoc m attr (measures-all-final-individuals attr, final-populations)))
      {}
      attributes)))


(defn+opts measure-table-data
  [filenames | :as options]
  (let [final-populations (mapv #(tested-final-population %, options) filenames)]
    {:best-fitness (compute-best-individual-measures sort-by-fitness, [:test-accuracy, :selected-genes-count, :accuracy] final-populations),
     :best-accuracy (compute-best-individual-measures sort-by-accuracy, [:test-accuracy, :selected-genes-count, :accuracy] final-populations),
     :all (compute-all-individual-measures [:test-accuracy, :selected-genes-count, :accuracy] final-populations)}))

(defn+opts new-measure-table-data
  [filenames | :as options]
  (let [final-populations (mapv #(tested-final-population %, options) filenames)]
    {:best-fitness (compute-best-individual-measures sort-by-fitness, [:test-accuracy, :selected-genes-count, :fitness] final-populations),
     :all (compute-all-individual-measures [:test-accuracy, :selected-genes-count, :fitness #_:accuracy] final-populations)}))

(defn accuracy-without-gene-selection
  [filenames, map?]
  (->> filenames
    (mapv (if map?
            (comp :test-accuracy-without-selection load-population-data-map)
            (comp :test-accuracy-without-selection meta last load-population-data-vector)))
    ((juxt mean standard-deviation))
    (zipmap [:mean, :standard-deviation])))

(comment
  (measures-all-final-individuals :test-accuracy, (map #(format "%d-shipp-pop.data" %) (range 1 11)))
)


(defn best-individual-genes
  [dataset-filename, filenames]
  (let [final-pop (mapv tested-final-population filenames)
        gene-ids (:gene-ids (ds/load-csv-dataset dataset-filename))
        print-best-individuals (fn [sort-comp]
                                 (doseq [genes (->> final-pop
                                                 (mapv #(->> % (sort sort-comp) first))
                                                 (mapv (comp (partial retrieve-unigenes gene-ids) :selected-genes)))]
                                   (println (str/join ", " genes))))]
    (println "Best by training fitness")
    (print-best-individuals sort-by-fitness)
    (println "\nBest by training accuracy")
    (print-best-individuals sort-by-accuracy)))