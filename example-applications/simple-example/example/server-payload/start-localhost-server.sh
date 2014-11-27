#!/bin/sh

nohup java -cp .:simple-example-0.1.2.jar:keystore.ks:truststore.ks  clojure.main -e "(use 'sputnik.satellite.main) (sputnik.satellite.main/-main \"-t\" \"server\" \"localhost-sputnik-server.cfg\")" > localhost.server-output.log 2> localhost.server-error.log &
