; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns feature-selection.main
  (:gen-class)
  (:require
    [clojure.java.io :as io]
    [clojure.tools.cli :as cli]
    [clojure.options :refer [defn+opts, ->option-map]]
    [clojure.string :as str]
    [sputnik.tools.logging :as l]
    [sputnik.satellite.client :as c]
    [frost.quick-freeze :as qf]
    [frost.file-freezer :as ff]
    [feature-selection.analysis :as a]
    [feature-selection.bitset :as b]
    [feature-selection.datasets :as ds]
    [feature-selection.fitness :as f]
    [feature-selection.remote :as r]
    [feature-selection.ga :as ga]
    [feature-selection.nearest-neighbor :as nn]
    [feature-selection.tools :as t]))


(def ^:private ^:dynamic *in-repl* false)





(defn print-population-csv
  [population]
  (println "\nPopultation (CSV):\n")
  (println "selected-genes;train-fitness;train-accuracy;test-fitness;test-accuracy")
  (doseq [{:keys [selected-genes, fitness, accuracy, test-fitness, test-accuracy]} population]
    (->> [(b/cardinality selected-genes), fitness, accuracy, test-fitness, test-accuracy]
      (str/join ";")
      println)))


(defn+opts print-result
  [tested-final-population, load-dataset-fn, filename | {output-best-solutions 10}]
  (t/output-future
    (println "\n\nFinal population:")
    (let [{:keys [gene-ids]} (load-dataset-fn filename)]
      (println (format "\nsorted by train fitness (top %d):\n" output-best-solutions))
      (a/print-population-sorted :fitness, output-best-solutions, tested-final-population, gene-ids))
    (print-population-csv tested-final-population))
  tested-final-population)



(defn without-gene-selection
  [filename, folds-filename]
  (let [folds-specs (ds/load-folds folds-filename)
        dataset (ds/csv-dataset-sequential filename)]
    (->> (for [[a b c] folds-specs]
           (let [train-positions (mapv (comp vector first) (concat a b))
                 train-instances (mapv #(get-in dataset %) train-positions),
                 test-positions  (mapv (comp vector first) c)
                 test-instances (mapv #(get-in dataset %) test-positions),
                 classifier (nn/build-1-NN train-instances)
                 {:keys [correct, count]} (nn/test-classifier classifier, test-instances)] 
             (/ (double correct) count)))
      ((juxt a/mean a/standard-deviation))
      (zipmap [:mean :standard-deviation]))))



(defn add-accuracy-without-gene-selection
  [population, dataset, gene-count, train-instances, test-instances]
  (let [classifier (nn/build-1-NN (mapv #(get-in dataset %) train-instances)),
        {:keys [correct, count]} (nn/test-classifier classifier, (mapv #(get-in dataset %) test-instances))]
    (vary-meta population assoc :test-accuracy-without-selection (/ (double correct) count))))



(defn+opts run-classification-feature-selection
  [mode, seed, file-list | {client-config nil, export-filename nil, batch-size 1, save-all-populations false,
                            pre-defined-folds nil, quiet false} :as options]
  (when-not (== 1 (count file-list))
            (throw (IllegalArgumentException. "Only one csv files allowed for the classification feature selection!")))

  (let [[dataset-fn, load-fn, file-spec] [(if pre-defined-folds `ds/csv-dataset-sequential `ds/csv-dataset-by-class),
                                          ds/load-csv-dataset, (first file-list)],
        gene-count-set (set (map #(alength ^doubles (:gene-expressions %)) (:samples (load-fn file-spec))))
        gene-count (cond 
                     (= (count gene-count-set) 1) (first gene-count-set)
                     (zero? (count gene-count-set)) (throw (IllegalArgumentException. (format "You have specified a file with a wrong format! Given file: %s" file-spec)))
                     :else (throw (IllegalArgumentException. (format "Given datasets have differing number of genes! (%s)" (str/join ", " (sort gene-count-set))))))
        rand-fn (ga/create-rand-fn seed),
        ds ((resolve dataset-fn) file-spec),
        {:keys [train-instances, test-instances]}
          (if pre-defined-folds
            (f/cross-validation-setup-given-folds rand-fn, pre-defined-folds)
            (f/cross-validation-setup rand-fn, (f/replace-data-with-coordinates ds))),
        ^java.io.Closeable client (when (= mode :distributed)
                                    (if (nil? client-config)
                                      (throw (IllegalArgumentException. "A config file containing the client configuration for distributed execution is needed!"))
                                      (c/create-client-from-config client-config)))
        evaluation-fn (case mode
                        :local (partial f/local-evaluation dataset-fn, file-spec, train-instances),
                        :distributed (partial r/distributed-evaluation client, batch-size, (atom -1), dataset-fn,
                                       ; only filenames for remote execution (files are remotely in the same directory or on the classpath)
                                       (if (sequential? file-spec)
                                         (mapv #(-> % io/file .getName) file-spec)
                                         (-> file-spec io/file .getName)),
                                       train-instances)),
        test-fn (case mode
                  :local (partial f/local-test dataset-fn, file-spec, train-instances, test-instances),
                  :distributed (partial r/distributed-test client, batch-size, dataset-fn,
                                 ; only filenames for remote execution (files are remotely in the same directory or on the classpath)
                                 (if (sequential? file-spec)
                                   (mapv #(-> % io/file .getName) file-spec)
                                   (-> file-spec io/file .getName)),
                                 train-instances,
                                 test-instances))
        freezer (when export-filename (ff/create-freezer export-filename))]
    (try 
      (-> (ga/genetic-algorithm (when save-all-populations freezer), rand-fn, evaluation-fn, gene-count, options)
        test-fn
        (add-accuracy-without-gene-selection ds, gene-count, train-instances, test-instances)
        (a/maybe-freeze -1, freezer)
        (cond-> (not quiet) (print-result load-fn, (if (sequential? file-spec) (first file-spec) file-spec), options)))
      (finally
        (when client
          (.close client))
        (when freezer
          (.close freezer))))))



(defn exit
  [code]
  (when-not *in-repl*
    (shutdown-agents)
    (System/exit code)))



(def ^:private cli-options
  [["-h" "--help" "Show help." :default false]
   ["-P" "--progress" "Show progress" :default false]
   ["-M" "--mode M" "Specifies how to run the algorithm: local or distributed" :parse-fn keyword :validate [#{:local, :distributed} "Must be one of \"local\" or \"distributed\"!"]]
   ["-g" "--generations G" "Determines the number of generations for the genetic algorithm." :parse-fn #(Integer/parseInt %) :validate [#(<= 1 %) "Must be greater than or equal to 1!"] :default 100]
   ["-p" "--population-size P" "Determines the population size for the genetic algorithm." :parse-fn #(Integer/parseInt %) :validate [#(< 1 %) "Must be greater than 1!"] :default 20]
   ["-s" "--seed S" "Specifies the seed used for the PRNG." :parse-fn #(Integer/parseInt %)]
   ["-1" "--flip-one-probability P"  "Determines the probability to flip a 1 to 0 in the genetic algorithm." :parse-fn #(Double/parseDouble %) :default 0.5]
   ["-0" "--flip-zero-probability P" "Determines the probability to flip a 0 to 1 in the genetic algorithm." :parse-fn #(Double/parseDouble %) :default 0.5]   
   ["-c" "--crossover-probability P" "Determines the crossover probability for the genetic algorithm." :parse-fn #(Double/parseDouble %) :default 0.05]
   ["-o" "--one-probability P" "Determines the probability of a 1 in the construction of initial solutions for the genetic algorithm."   :parse-fn #(Double/parseDouble %) :default 0.01]
   ["-b" "--keep-best B" "Determines the portion of best solutions that is selected for the next generation of the genetic algorithm."   :parse-fn #(Double/parseDouble %) :default 0.01]
   ["-C" "--client-config URL" "Specifies the client configuration to be used for remote execution."]
   ["-F" "--folds URL" "Specifies a file containing the folds for the repeated cross validation in the fitness function of the genetic algorithm." :default nil]
   ["-B" "--batch-size BS" "Specifies the number of evaluations put into one task (usefull for short running evaluations)." :parse-fn #(Integer/parseInt %) :validate [#(<= 1 %) "Must be greater than or equal to 1!"] :default 1]
   ["-N" "--output-best-solutions N" "Specifies the number of best solutions that is printed." :parse-fn #(Integer/parseInt %) :validate [#(<= 1 %) "Must be greater than or equal to 1!"] :default 10]
   ["-r" "--rescale-factor F" "Specifies the rescale factor (if any) used in the selection of the genetic algorithm, usually (1,5]."   :parse-fn #(Double/parseDouble %)]
   ["-E" "--export-data FILE" "Specifies the file to export the population data to."]
   ["-A" "--save-all-populations" "Specifies that every population needs to be save to the given export file. Otherwise, only the final tested population is saved." :default false]
   ["-R" "--repeat N" "Specifies the number of repetitions for the experiment. (Applies only when no --folds are given.)" :parse-fn #(Integer/parseInt %) :validate [#(<= 1 %) "Must be greater than or equal to 1!"] :default 1]
   ["-Q" "--quiet" "Disables the printing of the final population."]
   ["-L" "--log-level LEVEL" "Specifies the log level: trace, debug, info, warn, error, fatal"
    :parse-fn keyword :validate [#{:trace, :debug, :info, :warn, :error, :fatal} "Must be one of: trace, debug, info, warn, error, fatal"]
    :default :info]])


(defn- print-help
  ([banner]
    (print-help banner, nil))
  ([banner, msg]
    (when msg
      (println msg "\n"))
    (println
      (format
        (str "Usage: java -jar feature-selection-<VERSION>.jar run <OPTIONS> <FILES>\n\n"
          "Options:\n"
          "%s")
        banner))
    (flush)))


(defn print-errors
  [errors, summary]
  (doseq [err-msg errors]
    (println err-msg))
  (print-help summary)
  (exit 1))


(defn feature-selection-main
  [& args]
  (let [{:keys [options, arguments, summary, errors]} (cli/parse-opts args, cli-options),
        ; get params
        {:keys [help, mode, generations, population-size, seed, folds,
                flip-one-probability, flip-zero-probability, crossover-probability, one-probability, #_modify-flip-one-probability,
                keep-best, progress, rescale-factor, client-config, batch-size, output-best-solutions,
                export-data, save-all-populations, repeat, quiet, log-level]} options,
        seed-fn (if seed (constantly seed) #(java.lang.System/currentTimeMillis))]
    (l/configure-logging :filename "client.log" :log-level log-level)    
    (let [result (cond
                   help (print-help summary)
                   errors (print-errors errors, summary)
                   :else (do
                           (println (format "Given arguments:\n\n%s\n" (str/join " " args)))
                           (println "\nUsing parameters:\n")
                           (println (str/join "\n" (map (fn [[k v]] (str k ": " v)) (sort-by key options))) "\n")
                           (let [folds-specs (when folds (ds/load-folds folds))
                                 repetitions (if folds-specs (count folds-specs) repeat)]
                             (when (and folds-specs repeat (not= (count folds-specs) repeat))
                               (println "WARNING: --repeat and --folds where specified but the number of fold specifications is not equal to the given repetition count!"))                             
                             (dotimes [i repetitions]
                               (let [seed (seed-fn),
                                     folds (when folds-specs (folds-specs i)),
                                     i (inc i),]
                                 (println (format "\nRun %3d/%3d  -  used seed %d" i, repetitions, seed))
                                 (run-classification-feature-selection mode, seed, arguments,
                                   :population-size population-size, :generation-count generations,                                              
                                   :crossover-probability crossover-probability,
                                   :one-probability one-probability, :keep-best keep-best, :progress progress,
                                   :client-config client-config, :batch-size batch-size, :output-best-solutions output-best-solutions,
                                   :export-filename (when export-data (str i "-" export-data)),
                                   :save-all-populations save-all-populations,
                                   :quiet quiet
                                   (->option-map 
                                     (into {} (remove (comp nil? second)
                                                [[:rescale-factor rescale-factor]
                                                 [:pre-defined-folds folds]
                                                 [:flip-one-probability flip-one-probability], [:flip-zero-probability flip-zero-probability]])))))))))]      
      (when-not *in-repl*
        (t/wait-for-finished-output)
        (System/exit 0))
      result)))




(defn- print-main-help
  ([banner]
    (print-main-help banner, nil))
  ([banner, msg]
    (when msg
      (println msg "\n"))
    (println
      (format
        (str "Feature Selection\n"
          "Usage: java -jar feature-selection-<VERSION>.jar [-h] [setup|run] <OPTIONS> <FILES>\n\n"
          "Modes:\n"
          "- Setup server and workers for distributed computation:\n"
          "    java -jar feature-selection-<VERSION>.jar setup\n"
          "- Run the feature selection with the given options and files:\n"
          "    java -jar feature-selection-<VERSION>.jar run <OPTIONS> <FILES>\n\n"
          "Options:\n"
          "%s")          
        banner))
      (flush)))


(defn- execute-main
  [ns, arguments]
  (require ns)
  (eval `(~(symbol (name ns) "-main") ~@arguments)))


(defn -main
  [& args]
  (let [{:keys [options, arguments, summary, errors]} (cli/parse-opts args, [["-h" "--help" "Show help." :default false]], :in-order true),
        {:keys [help]} options,
        mode (keyword (first arguments))]
    (cond
      errors (do
               (doseq [err-msg errors] (println err-msg))
               (print-main-help summary)
               (System/exit 1)),
      help (do (print-main-help summary) (System/exit 0)),
      (= mode :run) (apply feature-selection-main (rest arguments)),
      (= mode :setup) (execute-main 'sputnik.control.gui, (rest arguments)),
      :else (print-main-help summary, "Mode must be one of: setup or run!"))))

(defmacro main
  [& args]
  `(binding [*in-repl* true]
     (-main ~@(map str args))))


(defmacro main-args
  [& args]
  (->> args (map str) (str/join " ") println))