# Sputnik

[![Clojars Project](https://img.shields.io/clojars/v/sputnik.svg)](https://clojars.org/sputnik)

Sputnik is a Clojure library for parallelization of computations to distributed computation nodes.
Sputnik does not only handle the distribution and execution of tasks but also the configuration and deployment of the server and the workers.

<p align="center">
<img src="doc/images/scenario.png" alt="Distributed computation scenario of Sputnik" title="Distributed computation scenario of Sputnik" align="center" />
<br>
Distributed computation scenario of Sputnik.
</p>



## Main Features

* ad hoc setup of Sputnik clusters (automatic deployment)
* no permanent setup needed (no administrator rights required)
* secure communication and client authentication
* fault-tolerant task distribution
* optional data compression
* on-the-fly adjustment of worker thread count per worker
* easy configuration and usage
* web user interface on the running server:
  * progress report
  * remaining duration estimation
  * used thread count configuration for each worker
* graphical user interface (client):
  * configuration of workers, server, communication, ... 
  * deployment of necessary files
  * start of server and workers
* Multiple workers per host, each assigned to a different NUMA node
* Sputnik client REST service for access from different programming platforms (e.g. tuning via irace from R)

## Project Maturity

The core library has been used in internal projects for more than a year running parallel experiments lasting from several hours to multiple days.
The graphical user interface for configuration of the Sputnik nodes and deployment is rather new.
Sputnik has been used in production on Java 7 and Java 8.

## Requirements

**It is recommended to use Java 7 (or newer versions of Java) for Sputnik.**
For security reasons Sputnik only allows the usage of TLSv1.1 and TLSv1.2 for
the encryption of the internal communication and the web user interface of the server.
Java 7 is the first release to support both TLSv1.1 and TLSv1.2.

Since it supports TLSv1.1, Java 6 might be usable for Sputnik as well (but is not extensively tested). 


## Releases

The latest release is [Sputnik 0.5.4](../../releases/tag/v0.5.4).

## Install

For [Leiningen](http://leiningen.org) add the following to your dependency vector in your project.clj:

```clojure
[sputnik "0.5.4"]
```

## Documentation

* [Configuration and Deployment](doc/ConfigurationDeployment.md)
* [Key-based SSH Authentication](doc/SSH.md)
* [Client usage](doc/ClientUsage.md)


## Example applications

The Sputnik project provides two example applications.

* [Simple example](example-applications/simple-example): A simple example application that computes an estimation of *pi*.
* [Feature selection](example-applications/feature-selection): A feature selection for a classifier using a genetic algorithm.

## Comparison with JPPF

The implementation using the [JPPF framework](http://www.jppf.org) to compare it to Sputnik for the parallelization of the feature selection application can be found in the folder [comparison/jppfnik](comparison/jppfnik).

## License

Copyright © 2014-2018 Gunnar Völkel

Sputnik is distributed under the Eclipse Public License.
