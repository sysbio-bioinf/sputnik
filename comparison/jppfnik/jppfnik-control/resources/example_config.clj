(defuser sshusername "user")


(defhost server-pc,  "server", "1.2.3.0", :cpus 4)

(defhost worker0-pc, "node0", "1.2.3.1", :cpus 10)
(defhost worker1-pc, "node1", "1.2.3.2", :cpus 10)
(defhost worker2-pc, "node2", "1.2.3.3", :cpus 10)
(defhost worker3-pc, "node3", "1.2.3.4", :cpus 10)
(defhost worker4-pc, "node4", "1.2.3.5", :cpus 10)

(defhost client-pc, "client", "1.2.9.5", :cpus 2)

(defjar clojure-1-6-0 "path/to/clojure-1.6.0.jar" :project-name "org.clojure/clojure" :version "1.6.0")
(defjppfnik-client-jar jppfnik-client  "../jppfnik-client/target/jppfnik-client-0.0.1-standalone.jar")
(defjppfnik-node-jar   jppfnik-node    "../jppfnik-node/target/jppfnik-node-0.0.1-standalone.jar")
(defjppfnik-server-jar jppfnik-server  "../jppfnik-server/target/jppfnik-server-0.0.1-standalone.jar")

(defjar feature-selection-jppf "../feature-selection-jppf/target/feature-selection-jppf-0.0.1.jar")

(deffile west-csv "../feature-selection-jppf/data/west.csv")
(deffile west-folds "../feature-selection-jppf/data/west_folds.txt")

(defserver server, server-pc, :jppf.server.port 11111)

(defnode worker0, worker0-pc,
  :jppf.server
  {:host "1.2.3.0"
   :port 11111})

(defnode worker1, worker1-pc,
  :jppf.server
  {:host "1.2.3.0"
   :port 11111})

(defnode worker2, worker2-pc,
  :jppf.server
  {:host "1.2.3.0"
   :port 11111})

(defnode worker3, worker3-pc,
  :jppf.server
  {:host "1.2.3.0"
   :port 11111})

(defnode worker4, worker4-pc,
  :jppf.server
  {:host "1.2.3.0"
   :port 11111})


(defclient client, client-pc,
  :jppf.drivers "myserver"
  :myserver
  {:jppf.server
   {; host name, or ip address, of the host the JPPF driver is running on
    :host "1.2.3.0"
    ; server port number
    :port 11111}    
    ; priority given to the driver connection
    :priority 0})

(defpayload "test-application"    
  clojure-1-6-0
  ;example
  feature-selection-jppf
  west-csv
  west-folds
  jppfnik-client
  jppfnik-node
  jppfnik-server
)

(defcluster "mycluster", server, worker0, worker1, worker2, worker3, worker4, client, sshusername)
