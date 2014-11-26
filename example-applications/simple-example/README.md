# Simple example using Sputnik

This simple example demonstrates how Sputnik can be used for parallelization.
This example implements a Monte Carlo algorithm to estimate the number *pi* (ratio of a circle's circumference to its diameter).

The idea is as follows. A number N of points from the square (-1,-1) to (1,1) are drawn uniformly at random.
The number of points M that are within the unit circle are counted.
The number *pi* is estimated as 4*M/N.
The algorithm uses T tasks where each task draws P points. The T tasks can be computed in parallel.

## Download & Run

The program can be downloaded from [here](../../../../releases/download/v0.4.0/simple-example-0.1.2.jar).

The application can be run by just specifying a Sputnik client configuration, e.g. ```client.cfg``` as below.
```bash
$ java -jar simple-example-0.1.2.jar --config client.cfg 
```

## Build & Run

The example can be built from command line in the project root directory with [Leiningen](http://leiningen.org) as follows.
```bash
$ lein uberjar
```

The built application can be run by just specifying a Sputnik client configuration, e.g. ```client.cfg``` as below.
```bash
$ java -jar target/simple-example-0.1.2.jar --config client.cfg 
```

## Sputnik setup

The Sputnik cluster can be configured and started from the included graphical user interface which is also
able to generated a matching client configuration file.
The graphical user interface is started with the following command.
```bash
$ java -jar target/simple-example-0.1.2.jar --setup
```
The documentation for the setup can be found in the [Sputnik project](../../doc/ConfigurationDeployment.md).


### Example setup

An example setup running server, worker and client on localhost can be found in the directory ```example```.
The subdirectory ```configuration``` contains the following files:

 * ```sputnik-cluster.cfg``` the conguration file for the deployment GUI
 * ```client.cfg``` configuration for the client program
 * keystores and truststores for client, worker and server
 
There are two subdirectories ```worker-payload``` and ```server-payload``` which contain all files
needed to start the worker and the server, respectively.

The example is run by opening the ```example``` folder on the command line and then starting server and worker via ```./start-worker-server.sh```.
When those are running start the computation via ```./run-computation.sh```.


## Command line options

All available command line options can be listed via the following.
```bash
$ java -jar target/simple-example-0.1.2.jar -h

Usage: java -jar simple-example-<VERSION>.jar run <OPTIONS> <FILES>

Options:
  -h, --help                            Show help.
  -c, --config URL                      URL to Sputnik client configuration file.
  -s, --seed S           1400164503300  Seed for the PRNG.
  -t, --task-count T     1000           Number of computational tasks.
  -p, --point-count P    100000000      Number of points per tasks.
  -P, --progress-report                 Specifies whether a progress report is printed.
      --setup                           Start the Sputnik GUI to setup the Sputnik cluster.
```

## License

Copyright © 2014 Gunnar Völkel

Distributed under the Eclipse Public License.
