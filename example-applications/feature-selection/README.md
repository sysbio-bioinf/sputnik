# Sputnik Usage Example: Feature Selection

This project implements a **feature selection** for a **nearest neighbor classifier** (1-NN) based on a **genetic algorithm**
using a so-called *Merit* measure as fitness function.
An individual represents a feature combination where the selection state of a gene is encoded as one bit (1 = selected, 0 = not selected).
The genetic algorithm evaluates those individuals using the Merit measure on 2/3 of the given samples.
The final population is tested using the 1-NN classifier on the remaining 1/3 of the given samples.
The evaluation of each population is parallelized among the available workers of the [Sputnik](../../) cluster.


## Download

The runnable feature selection program can be downloaded from [here](../../releases/download/v0.3.1/feature-selection-1.0.0.jar).


## Build

The project can be built from command line in the project root directory with [Leiningen](http://leiningen.org) as follows.
```bash
$ lein uberjar
```


## Sputnik setup

The Sputnik cluster can be configured and started from the included graphical user interface which is also
able to generated a matching client configuration file.
The graphical user interface is started with the following command.
```bash
$ java -jar feature-selection-1.0.0.jar setup
```
The documentation for the setup can be found in the [Sputnik project](../../doc/ConfigurationDeployment.md).


## Command line options (including algorithm parameters)

All available command line options can be listed via the following.
```bash
$ java -jar feature-selection-1.0.0.jar run -h

Usage: java -jar feature-selection-<VERSION>.jar run <OPTIONS> <FILES>

Options:
  -h, --help                            Show help.
  -P, --progress                        Show progress
  -M, --mode M                          Specifies how to run the algorithm: local or distributed
  -g, --generations G            100    Determines the number of generations for the genetic algorithm.
  -p, --population-size P        20     Determines the population size for the genetic algorithm.
  -s, --seed S                          Specifies the seed used for the PRNG.
  -1, --flip-one-probability P   0.5    Determines the probability to flip a 1 to 0 in the genetic algorithm.
  -0, --flip-zero-probability P  0.5    Determines the probability to flip a 0 to 1 in the genetic algorithm.
  -c, --crossover-probability P  0.05   Determines the crossover probability for the genetic algorithm.
  -o, --one-probability P        0.01   Determines the probability of a 1 in the construction of initial solutions for the genetic algorithm.
  -b, --keep-best B              0.01   Determines the portion of best solutions that is selected for the next generation of the genetic algorithm.
  -C, --client-config URL               Specifies the client configuration to be used for remote execution.
  -F, --folds URL                       Specifies a file containing the folds for the repeated cross validation in the fitness function of the genetic algorithm.
  -B, --batch-size BS            1      Specifies the number of evaluations put into one task (usefull for short running evaluations).
  -N, --output-best-solutions N  10     Specifies the number of best solutions that is printed.
  -r, --rescale-factor F                Specifies the rescale factor (if any) used in the selection of the genetic algorithm, usually (1,5].
  -E, --export-data FILE                Specifies the file to export the population data to.
  -A, --save-all-populations            Specifies that every population needs to be save to the given export file. Otherwise, only the final tested population is saved.
  -R, --repeat N                 1      Specifies the number of repetitions for the experiment. (Applies only when no --folds are given.)
  -Q, --quiet                           Disables the printing of the final population.
  -L, --log-level LEVEL          :info  Specifies the log level: trace, debug, info, warn, error, fatal
```

## Experiments

This section describes how the experiments in the paper were run (TODO: add paper citation).

### Runtime Experiments

The runtime experiments were started as follows: (TODO: datasets and folds under directory "data"?)
```bash
$ java -jar feature-selection-1.0.0.jar run data/west.csv --folds data/west_folds.txt \
--one-probability 0.01 --flip-one-probability 0.5 --flip-zero-probability 0.5 --crossover-probability 0.01 \
--keep-best 0.005 --population-size 10000 --generations 5 --rescale-factor 4 --seed 4711 \
--client-config <PATH/TO/client.cfg> --mode distributed --batch-size 5 \
--progress --quiet
```
Replace ```<PATH/TO/client.cfg>``` with the actual path to your client configuration file which you have generated via:
```bash
$ java -jar feature-selection-1.0.0.jar setup
```

### Feature Selection Experiments

The feature selection experiments were started as follows: (TODO: datasets and folds under directory "data"?)
```bash
$ java -jar feature-selection-1.0.0.jar run data/<DATASET>.csv --folds data/<DATASET>_folds.txt \
--one-probability 0.01 --flip-one-probability 0.5 --flip-zero-probability 0.5 --crossover-probability 0.01 \
--keep-best 0.005 --population-size 10000 --generations 200 --rescale-factor 4 \
--client-config ~/sputnik-config/client.cfg --mode distributed --batch-size 5 \
--progress --quiet --export-data <DATASET>.data
```
Where ```<DATASET>``` is one of: ```armstrong```, ```golub```, ```SHIPP2002_rma```, ```west```.

## License

Copyright © 2014 Gunnar Völkel

This project is distributed under the Eclipse Public License.
