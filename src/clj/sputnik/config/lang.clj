; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.config.lang
  (:require
    [clojure.string :as string]
    [clojure.options :refer [defn+opts, defn+opts-]]
    [sputnik.config.meta :as meta]
    [sputnik.tools.file-system :as fs]))


(defn- resolve-symbol
  [s]
  (let [^clojure.lang.Var v (resolve s)] 
    (symbol (-> v .ns ns-name name) (-> v .sym name))))

(defmacro ^:private create-config-def
  "Creates a \"defconfig-fn\" macro for a given function `config-fn` and the explicit args.
  The given function `config-fn` has to be created via `defn+opts`.
  The given `config-type` will be set as metadata on the variable."
  [config-fn, config-type]
  (let [config-fn-meta (-> config-fn resolve meta),
        explicit-args (->> config-fn-meta :mandatory-parameters),
        doc-str (str 
                  (format "Same as (def name (%s %s <options>)).\n  " 
                    config-fn (string/join " " explicit-args)) 
                  (:doc config-fn-meta))]    
	 `(defmacro ~(with-meta (->> config-fn name (str "def") symbol) {:sputnik/config true})
	    ~doc-str
	    [~'name, ~@explicit-args, ~'&, ~'options]
	   `(def ~(meta/with-config-type ~config-type ~'name) (~'~(resolve-symbol config-fn) ~~@explicit-args ~@~'options)))))




(defn+opts ^:sputnik/config user
  "Creates a user account configuration with the given username.
  Usually, it is a good idea to just add the key via \"ssh-add\"."
  [username | {password nil, public-key-path nil, private-key-path nil, passphrase nil, no-sudo true, sudo-password nil} :as options]
  (meta/with-config-type :sputnik/user 
    (into {:username username} options)))

(create-config-def user :sputnik/user)

(defmacro ^:sputnik/config with-user
  "Adds the user as parameter to all the given top-level forms."
  [user & body]
 `(do
    ~@(for [form body]
        (if (#{'node 'defnode} (first form))
          (concat form [:user user])
          form))))


(defn+opts ^:sputnik/config node
  "Defines a remote node with its host-name, host-address and additional options.
  <group>Group that is used for pallet's group selection mechanism.</>
  <ssh-port>Specifies a custom ssh port to use.</>
  <cpus>Number of cpus of the node which is used as default for the number of worker thread of a worker node.</>
  <user>Specifies the user that is used to log into the node.</>
  <alive-check-timeout>Timeout (in msec) after which remote nodes are checked whether they are still alive.</>
  <registry-port>Specifies the port where the RMI registry will listen.</>
  <node-port>Specifies the port where the node will listen. This is useful when port forwarding is needed.</>
  <log-level>Specifies the log level that is used for the node.</>
  <sputnik-jvm>Specifies the path of a custom Java Virtual Machine to use.</>
  <max-log-size>Maximum size of the log file in MB.</>
  <log-backup-count>Number of log file backups to use.</>
  <custom-log-levels>Map of custom log level specifications mapping class name to log level, e.g. {:org.eclipse.jetty :info}.</>"
  [host-name, host-address | {group "default", ssh-port nil, cpus nil, user nil,
                              alive-check-timeout 10000, registry-port 10099, node-port nil,
                              sputnik-jvm nil,
                              log-level (choice :info :trace, :debug, :warn, :error, :fatal),
                              max-log-size 100, log-backup-count 10, custom-log-levels {}} :as options]
  (when (not= (meta/config-type user) :sputnik/user)
    (throw (IllegalArgumentException. (format "Node \"%s\" has no user associated! Instead the following value was given: %s" (some-> host-name name) user))))
  (let [node-name (some-> host-name name)]
    (meta/with-config-type :sputnik/node
	    {:host {:name node-name, :address host-address, :id node-name, :user user, :group group, :ssh-port ssh-port},
       :sputnik-jvm sputnik-jvm,
	     :options (-> options (with-meta nil) (dissoc :user :group :ssh-port :sputnik-jvm) (assoc :nodename node-name, :hostname host-address))})))

(create-config-def node :sputnik/node)



(defn ^:sputnik/config payload-entity
  "Creates a payload entity for the given url which can be a file or directory."
  [url]
  (meta/with-config-type :sputnik/payload-entity
    {:url url}))

(create-config-def payload-entity :sputnik/payload-entity)


(defn+opts find-one
  "Searches for one configuration of the given type in the given collection.
  Throws an exception when none or more than one configuration is found.
  <need-one>When set to false, it is allowed to return nil if no configuration is found.</need-one>"
  [filter-fn, desc-str, coll | {need-one true}]
  (let [configs (filter filter-fn coll)]
    (cond
      (-> configs count (= 1))
        (first configs)
      (empty? configs)
        (when need-one
          (throw (IllegalArgumentException. (format "No configuration found for \"%s\"!" desc-str))))
      :else
        (throw (IllegalArgumentException. (format "More than one configuration found for \"%s\"!" desc-str))))))


(def ^:private sputnik-jar-pattern #".*sputnik-\d+\.\d+\.\d+\.jar")

(defn create-payload
  [name, payload-coll]
  (let [sputnik (find-one #(re-matches sputnik-jar-pattern (:url %)), "sputnik jar", payload-coll),
        files (filter
               (fn [{:keys [url]}]
                 (and
                   (fs/exists? url)
                   (fs/file? url)             
                   (not (re-matches sputnik-jar-pattern url))))
               payload-coll),
        directories (filter
                      (fn [{:keys [url]}]
                        (and
                          (fs/exists? url)
                          (fs/directory? url)))
                      payload-coll)]
    (meta/with-config-type :sputnik/payload
      {:name name, :sputnik sputnik, :directories (vec directories), :files (vec files)})))


(defmacro ^:sputnik/config defpayload 
  [payload-name, & payload-coll]
  `(def ~(meta/with-config-type :sputnik/payload 'payload) 
     (create-payload ~(name payload-name), ~(vec payload-coll))))


(defn ^:sputnik/config apply-role
  [{:keys [options, sputnik-jvm-opts, sputnik-jvm] :as role}, node]
  (-> (meta/with-config-type (meta/config-type role) node)
    (update-in [:sputnik-jvm-opts] #(or sputnik-jvm-opts %))
    (update-in [:sputnik-jvm] #(or sputnik-jvm %))
    (update-in [:options] #(merge %, options))))


(defn+opts- role
  "Creates a role configuration for a node.
  <sputnik-jvm-opts>Additional options to pass to the JVM of the sputnik process.</>
  <sputnik-jvm>Specifies the path of a custom Java Virtual Machine to use.</>"
  [node-type | {sputnik-jvm-opts nil, sputnik-jvm nil} :as options]
  (meta/with-config-type node-type
    {:options (with-meta (dissoc options :sputnik-jvm-opts :sputnik-jvm) nil),
     :sputnik-jvm-opts sputnik-jvm-opts,
     :sputnik-jvm sputnik-jvm}))


(defn+opts ^:sputnik/config server-role
  "Creates a server role.
  <admin-user>Username for the admin user in the web user interface of the server.</>
  <admin-password>Password for the admin user in the web user interface of the server.</>
  <min-port>Minimal port number for the web user interface. First unused port will be used by the web UI.</>
  <max-port>Maximal port number for the web user interface. First unused port will be used by the web UI.</>
  <scheduling-timeout>Specifies the duration of the pauses between scheduling actions.</>
  <scheduling-performance-interval-duration>Specifies the duration in milliseconds of the intervals
  in which the performance of workers and jobs is tracked. (used for scheduling decisions and UI)</>
  <scheduling-performance-interval-count>Specifies the number of the intervals
  in which the performance of workers and jobs is tracked. (used for scheduling decisions and UI)</>
  <max-task-count-factor>Specifies the factor that determines the maximum number of tasks of a worker (`max-task-count-factor` * thread-count).</>
  <worker-task-selection>Specifies the filter function that decides which workers get tasks in the current scheduling run.</>
  <worker-ranking>Specifies the function that ranks the workers - better ranked workers get new tasks first</>
  provided that the worker-task-selection decided to send them any tasks.</>
  <task-stealing>Specifies a function that selects already assigned tasks that are send to other workers. (No task stealing is an option as well.)</>
  <task-stealing-factor>Similar to the `max-task-count-factor` but applies only to task stealing.</>"
  [| {admin-user nil, admin-password nil, min-ui-port 8080, max-ui-port 18080,
      scheduling-timeout 100, max-task-count-factor 2, worker-task-selection nil, worker-ranking nil,
      task-stealing true, task-stealing-factor 2,
      scheduling-performance-interval-duration (* 1000 60 5), scheduling-performance-interval-count 12} :as options]
  (role :sputnik/server options))

(create-config-def server-role :sputnik/server)


(defn+opts ^:sputnik/config worker-role
  "Creates a worker role.
  <send-result-timeout>Timeout (in msec) that the worker will wait for additional results before sending them back to the server.</>
  <worker-threads>Initial number of threads that are used for task execution. If not set, the number of cpus of the host will be used.</>
  <max-results-to-send>Maximum number of results that are sent at once. This allows faster responses if the result data is large.</>
  <send-results-parallel>Specifies whether the results are serialized and sent in different parallel node threads.</>"
  [| {send-result-timeout 1000, max-results-to-send nil, send-results-parallel true, worker-threads nil} :as options]
  (role :sputnik/worker options))

(create-config-def worker-role :sputnik/worker)


(defn+opts ^:sputnik/config client-role
  "Creates a client role.
  <job>Specifies the job the client shall execute.</>"
  [| {job nil} :as options]
  (role :sputnik/client options))

(create-config-def client-role :sputnik/client)


(defn+opts ^:sputnik/config rest-client-role
  "Creates a REST client role.
  <rest-port>Specifies the port of the REST API for task submission.</>"
  [| {rest-port nil} :as options]
  (role :sputnik/rest-client options))

(create-config-def rest-client-role :sputnik/rest-client)



(defn ^:sputnik/config job
  "Creates a job configuration with the given job creation function.
  The given key value pairs are part of the map that is passed to the job-creation function.
  The intention is that other parameters can be added to the job map and the job creation function
  returns a map with keys :job-setup-fn and :job-setup-args."
  [job-creation-fn & keyvals]
  (when-not (and (list? job-creation-fn) (= (first job-creation-fn) 'fn))
    (throw (IllegalArgumentException. "job-creation-fn must be a function definition!")))
  (meta/with-config-type :sputnik/job
    (apply hash-map :creation-fn job-creation-fn keyvals)))


(defmacro ^:sputnik/config defjob
  [job-name, job-creation-fn & keyvals]
  `(def ~(meta/with-config-type :sputnik/job job-name)
     (job '~job-creation-fn ~@keyvals)))

;(create-config-def job :sputnik/job)


(defn+opts ^:sputnik/config communication
  "Creates a given ssl configuration referring to the given directory which has to contain
  the keystore and truststore files.
  <keystore>Specifies the filepath of the keystore that contains the private ssl key that is used for this commonication config. Each node can have his own key.</>
  <keystore-password>Password needed to access the keystore.</>
  <truststore>Specifies the filepath of the truststore that contains the public keys which are allowed to connect to a node using this config.</>
  <truststore-password>Password needed to access the truststore.</>
  <protocol>Name of the secure protocol to use. Details can be found in the Java API for SSL.</>
  <ssl-enabled>Specifies whether SSL shall be used.</>
  <thread-count>Number of threads that are used for communication (message processing).</>
  <buffer-init>Initial size of the buffer used for serializing message objects.</>
  <buffer-max>Maximum size of the buffer used for serializing message objects.</>
  <compressed>Specifies whether compression is used for message serialization.</>
  <no-wrap> If true then the ZLIB header and checksum fields will not be used in order to support the compression format used in both GZIP and PKZIP.</>
  <compression-level>Specifies the level of compression 0-9 with 9 for best compression.</>"
  [| {keystore "keystore.ks", keystore-password nil, 
      truststore "truststore.ks", truststore-password nil,
      protocol "TLSv1.2", ssl-enabled true,
      thread-count 2, buffer-init nil, buffer-max nil,
      compressed true, no-wrap true, compression-level 9} 
   :as options]
  (meta/with-config-type :sputnik/communication 
    (-> options (with-meta nil) (dissoc :thread-count) (assoc :comm-thread-count thread-count))))

(create-config-def communication :sputnik/communication)


(defn create-cluster
  [name, config-coll]
  (let [server  (find-one #(= (meta/config-type %) :sputnik/server), "sputnik server", config-coll)
        client  (find-one #(= (meta/config-type %) :sputnik/client), "sputnik client", config-coll)        
        communication (find-one #(= (meta/config-type %) :sputnik/communication), "sputnik communication", config-coll, :need-one false)
        workers (filter #(= (meta/config-type %) :sputnik/worker) config-coll)]
    (meta/with-config-type :sputnik/cluster
      {:name name, :communication communication, :server server, :client client, :workers (vec workers)})))


(defmacro ^:sputnik/config defcluster
  [cluster-name, & config-coll]
  `(def ~(meta/with-config-type :sputnik/cluster 'cluster)
     (create-cluster ~(name cluster-name), ~(vec config-coll))))