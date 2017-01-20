# Changes from 0.5.2 to 0.5.3

* Messaging Client: Fix try-connect error handling to retry on all exceptions.

# Changes from 0.5.1 to 0.5.2

* Server Web UI: Ignore disconnected workers for calculations
* Scheduling: Fix missing :capacity on unknown worker thread count

# Changes from 0.4.0 to 0.5.1

* Communication implemented via Netty (instead of Java RMI)
* Configuration of multiple Sputnik workers, one for each NUMA node, on a single host
* Worker reconnect strategy after server failure
* Sputnik client REST service  (initially for parallelizing tuning with irace from R)
* Equal-load scheduling (new default)
* Parallelization using `sputnik.api/with-sputnik` and `sputnik.api/future` (experimental)
* Bug fixes

# Changes from 0.3.2 to 0.4.0

* Faster scheduling of tasks
* Setup GUI: Additional options to specify scheduling strategies
* Server Web UI: access to server log files
* SSL: Allow only TLSv1.1 and TLSv1.2 for RMI and Web UI
* Sputnik client: built-in grouping of tasks to batches (optional, configurable batch size)
* Code for comparison between Sputnik and JPPF
	* Source of initial Sputnik prototype that uses JPPF added
	* Special comparison scenario added to application **feature-selection**
	