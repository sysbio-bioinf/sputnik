# Comparison Sputnik vs. JPPF

The projects in this folder where resurrected for comparing [Sputnik](/README.md) with [JPPF](http://www.jppf.org) using the feature selection application.
**The projects in this folder have been an early prototype for [Sputnik](/README.md) based on [JPPF](http://www.jppf.org).
It is not recommended to use them for anything else apart from the experiment comparing the runtimes of the feature selection application using Sputnik and JPPF.**


## Comparison experiment with JPPF

The setup of JPPF-based implementation is more complicated since it has only been an early prototype which was abandoned
in favor of Sputnik. 

### Build

As the first step all projects must be built via the ```build-all.sh```.
```bash
$ ./build-all.sh
```

### Deployment

This description assumes that [key-based SSH authentication}(../../doc/SSH.md) is set up already.
Then you have to startup a REPL in the project jppfnik-control via leiningen.

```bash
$ cd jppfnik-control
$ lein repl
```
```clojure
jppfnik-control.main=> (require 'jppfnik-control.launch)
jppfnik-control.main=> (in-ns 'jppfnik-control.launch)
jppfnik-control.launch=> (launch "resources/example_config.clj" :start-client false :deploy-client false)
```

After that the server and the workers are running on the machines specified in ```example_config.clj```.
Additionally, there is a folder ```jppfnik-control/sputnik-payload``` with all needed files to run the server, the workers or the client application.

### Run feature selection

Switch to the folder ```jppfnik-control/sputnik-payload``` which has been created in the previous deployment step.

```bash
$ cd sputnik-payload
```

In that folder you can run the runtime experiment with the feature selection application via:

```bash
$ java -cp jppfnik-client-0.0.1-standalone.jar:clojure-1.6.0.jar:feature-selection-jppf-0.0.1.jar \
-Dlog4j.configuration=log4j.properties -Djava.util.logging.config.file=logging.properties \
feature_selection_jppf.main run west.csv --folds west_folds.txt \
--one-probability 0.01 --flip-one-probability 0.5 --flip-zero-probability 0.5 --crossover-probability 0.01 \
--keep-best 0.005 --population-size 10000 --generations 5 --rescale-factor 4 --seed 4711 \ 
--client-config jppfnik-client-<MACHINE>.properties --mode distributed --batch-size 5 --progress --quiet
```

The placeholder ```<MACHINE>``` refers to the machine name that is specified for the client application in the ``example_config.clj```.

Running the experiment with the modified calculation runtimes is possible by adding the options
```--sleep-duration <SLEEP> --sleep-frequency 5``` where ```<SLEEP>``` is the sleep duration in milliseconds.

## License

Copyright © 2014 Gunnar Völkel

The following projects contained in this folder are distributed under the Eclipse Public License:

* feature-selection-jppf
* jppfnik-client
* jppfnik-control
* jppfnik-node
* jppfnik-server
* jppfnik-tools
* lein-jppfnik

JPPF and its components are released under the Apache 2.0 license. (see http://www.jppf.org)
