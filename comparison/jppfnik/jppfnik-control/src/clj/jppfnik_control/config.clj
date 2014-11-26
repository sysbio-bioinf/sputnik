; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-control.config
  (:require 
    [pallet.compute :as compute]
    [pallet.utils :as utils]
    [clojure.java.io :as io]
    [clojure.string :as string]
    [jppfnik-control.properties :as props]
    [jppfnik-control.defaults :as defaults])
  (:use
    [clojure.options :only [defn+opts, defn+opts-]]))


(defmacro with-type
  [type, expr]
 `(vary-meta ~expr assoc :type ~type))

(defmacro with-config-type
  [type, expr]
 `(vary-meta ~expr assoc :sputnik/config-type ~type))


(defn config-type
  [x]
  (-> x meta :sputnik/config-type))

(defn config-type?
  [x]
  (let [t (config-type x)]
    (and t (-> t namespace (= "sputnik")))))


(def this-ns *ns*)

(defn config-commands
  []
  (->> this-ns ns-publics (filter #(-> % val meta ::config)) keys vec))


(defmacro create-config-def
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
	 `(defmacro ~(with-meta (->> config-fn name (str "def") symbol) {::config true})
	    ~doc-str
	    [~'name, ~@explicit-args, ~'&, ~'options]
	   `(def ~(with-config-type ~config-type ~'name) (~'~config-fn ~~@explicit-args ~@~'options)))))


(defn+opts ^::config user
  "Creates a user account configuration."
  [username | {password nil, public-key-path ".ssh/id_rsa.pub", private-key-path ".ssh/id_rsa", passphrase nil, no-sudo true, sudo-password nil} :as options]
  (with-type :sputnik/user 
    (into {:username username} options)))

(create-config-def user :sputnik/user)


(defn+opts ^::config host
  "Creates a host configuration."
  [host-name, host-address | {group "default", ssh-port nil, local-address nil, jvm nil} :as options]
  (with-type :sputnik/host
    (into {:name (name host-name), :address host-address :id (name host-name)} options)))

(create-config-def host :sputnik/host)




(defn+opts ^::config jar
  "Creates a payload configuration for a jar file."
  [url | [project-name, version] :as options]
  (with-type :sputnik/jar-payload
    (into {:url url} options)))

(create-config-def jar :sputnik/jar-payload)


(defmacro create-jar-config
  [name, type, project-name]
 `(do
    (defn+opts ~(with-meta name {::config true}) 
      ~(format "Creates a %s payload configuration." project-name)
      ~['url '| :as 'options]
      (with-type ~type
        (jar ~'url :project-name ~project-name ~'options)))
    (create-config-def ~name ~type)))


(create-jar-config jppfnik-client-jar, :sputnik/jppfnik-client-jar-payload, "jppfnik-client")
(create-jar-config jppfnik-node-jar,   :sputnik/jppfnik-node-jar-payload,   "jppfnik-node")
(create-jar-config jppfnik-server-jar, :sputnik/jppfnik-server-jar-payload, "jppfnik-server")




(defn+opts ^::config directory
  "Creates a payload configuration for a directory."
  [url]
  (with-type :sputnik/directory-payload
    {:url url}))

(create-config-def directory :sputnik/directory-payload)


(defn+opts ^::config file
  "Creates a payload configuration for a single file."
  [url]
  (with-type :sputnik/file-payload
    {:url url}))

(create-config-def file :sputnik/file-payload)


(defn+opts find-one
  "Searches for one configuration of the given type in the given collection.
  Throws an exception when none or more than one configuration is found.
  <need-one>When set to false, it is allowed to return nil if no configuration is found.</need-one>"
  [config-type, coll | {need-one true}]
  (let [configs (filter #(= (type %) config-type) coll)]
    (cond
      (-> configs count (= 1))
        (first configs)
      (empty? configs)
        (when need-one
          (throw (IllegalArgumentException. (format "No configuration of type \"%s\" found!" config-type))))
      :else
        (throw (IllegalArgumentException. (format "More than one configuration of type \"%s\" found!" config-type))))))

(defn create-payload
  [name, payload-coll]
  (let [jppfnik-client (find-one :sputnik/jppfnik-client-jar-payload, payload-coll),
        jppfnik-node   (find-one :sputnik/jppfnik-node-jar-payload,   payload-coll),
        jppfnik-server (find-one :sputnik/jppfnik-server-jar-payload, payload-coll),
        jars (filter #(= (type %) :sputnik/jar-payload) payload-coll),
        directories (filter #(= (type %) :sputnik/directory-payload) payload-coll),
        files (filter #(= (type %) :sputnik/file-payload) payload-coll)]
    (with-type :sputnik/payload
      {:name name,
       :jppfnik-client jppfnik-client, :jppfnik-node jppfnik-node, :jppfnik-server jppfnik-server,
       :jars (vec jars), 
       :directories (vec directories), :files (vec files)})))

(defmacro ^::config defpayload 
  [payload-name, & payload-coll]
  `(def ~(with-config-type :sputnik/payload 'payload) 
     (create-payload ~(name payload-name), ~(vec payload-coll))))


(def option-mappings
  {
   :sputnik/server [[:management-port :jppf.management.port], 
                    [:local-address   :jppf.management.host], 
                    [:address         :jppf.management.host]]
   :sputnik/node   [[:management-port :jppf.management.port], 
                    [:local-address   :jppf.management.host], 
                    [:address         :jppf.management.host]
                    [:cpus            :processing.threads]]
   :sputnik/client {}
  })


(defn build-jppf-options
  [node-type, host, jppf-options]
  (let [mapping (option-mappings node-type)
        default-options (defaults/default-config node-type)]
    (merge (props/map->properties-map default-options)
	    (props/map->properties-map
		    (reduce
		      (fn [options, [src, dest]]
		        (let [v (get host src)]
		          ; if v is not nil and the option map does not contain the destination option
		          (if (and (-> v nil? not) (nil? (get options dest)))
		            (assoc options dest v)
		            options)))
		      jppf-options
		      mapping)))))


(defn+opts- cluster-node
  "<sputnik-jvm-opts>Additional options to pass to the JVM of the sputnik process.</sputnik-jvm-opts>"
  [node-type, host | {sputnik-jvm-opts nil} :as options]
  (with-type node-type
    {:host host, 
     :jppf-options (build-jppf-options node-type, host, (dissoc options :sputnik-jvm-opts)),
     :sputnik-jvm-opts sputnik-jvm-opts}))

(defn+opts ^::config server
  [host | {} :as options]
  (cluster-node :sputnik/server, host, options))

(create-config-def server :sputnik/server)

(defn+opts ^::config node
  [host | {} :as options]
  (cluster-node :sputnik/node, host, options))

(create-config-def node :sputnik/node)

(defn+opts ^::config client
  [host | {} :as options]
  (cluster-node :sputnik/client, host, options))

(create-config-def client :sputnik/client)



(defn+opts ^::config ssl
  "Creates a given ssl configuration referring to the given directory which has to contain
  the keystore and truststore files.
  "
  [ssl-directory | {keystore "keystore.jks", keystore-password nil, 
                    truststore "truststore.jks", truststore-password nil,
                    config "ssl.properties"} :as options]
  (let [non-jppf-keys [:keystore :keystore-password :truststore :truststore-password :config]] 
    (with-type :sputnik/ssl 
    (assoc (select-keys options non-jppf-keys)
      :ssl-directory ssl-directory
      :jppf-options
      (merge 
	      (props/map->properties-map (defaults/default-config :sputnik/ssl))
	      (props/map->properties-map (apply dissoc options non-jppf-keys)))))))

(create-config-def ssl :sputnik/ssl)


(defmulti add-server-data (fn [node, server] (type node)))

(defmethod add-server-data :sputnik/node
  [node, {:keys [host, jppf-options]}]
  (let [server-address (:address host)
        server-port (:jppf.server.port jppf-options)
        recovery-port (:jppf.recovery.server.port jppf-options)]    
    (update-in node [:jppf-options] assoc 
      :jppf.server.host server-address, 
      :jppf.server.port server-port,
      :jppf.recovery.server.port recovery-port)))

(defmethod add-server-data :sputnik/client
  [client, {:keys [host, jppf-options]}]
  (let [server-address (:address host)
        server-port (:jppf.server.port jppf-options)
        driver-name (:name host),
        management-port (:jppf.management.port jppf-options)]
    (update-in client [:jppf-options] merge 
      (props/map->properties-map
	      {
	       :jppf.drivers driver-name,          
          
         (keyword driver-name)
         {        
	         :jppf
				   {
					   :server
					   {
					     :host server-address,
		           :port server-port
					   }
				      
				     :management
	           {
               :host server-address
	             :port management-port
	           }
	         }           
	        }
	      }))))



(defn add-jppf-ssl-data
  [node, {:keys [config] :as ssl}]
  (update-in node [:jppf-options] merge
    (props/map->properties-map
      {
        :jppf.ssl
		    {
		      :enabled true 
		      :configuration.file config
		    }
      })))


(defn add-jppf-management-ssl-data
  [{:keys [jppf-options], :as node}]
  (let [enabled? (:jppf.management.enabled jppf-options), 
        port (:jppf.management.port jppf-options)] 
    (update-in node [:jppf-options] merge
	    (props/map->properties-map
	      {
	        :jppf.management
			    {
			      :enabled false
	          :port -1
	          
	          :ssl
	          {
	            :enabled enabled?
	            :port port
	          }
			    }
	      }))))


(defmulti add-ssl-data (fn [node, ssl] (type node)))


(derive :sputnik/client :sputnik/node)

(defmethod add-ssl-data :sputnik/node
  [node, ssl]
  (if ssl
    (add-jppf-ssl-data node, ssl)
    node))

(defmethod add-ssl-data :sputnik/server
  [{:keys [jppf-options] :as server}, {:keys [jppf-opt] :as ssl}]
  (if ssl
    (let [server (-> server (add-jppf-ssl-data ssl) add-jppf-management-ssl-data), ; it does not matter that the property :jppf.ssl.enabled is set to true since the server should not query it (even if, there would be no problem)
          port (:jppf.server.port jppf-options)]
      (update-in server [:jppf-options] merge
        (props/map->properties-map
          {
            :jppf.server.port -1
            :jppf.ssl.server.port port
            :jppf.peer.ssl.enabled true
          })))
    server))



(defn create-cluster
  [name, config-coll]
  (let [server (find-one :sputnik/server, config-coll)
        client (find-one :sputnik/client, config-coll)
        user   (find-one :sputnik/user,   config-coll)
        ssl    (find-one :sputnik/ssl,    config-coll, :need-one false)
        nodes (filter #(= (type %) :sputnik/node) config-coll)]
    (with-type :sputnik/cluster
      {:name name, :user user, :ssl ssl
       :server (add-ssl-data server, ssl)
       ; server data first, then ssl data (since server port info is needed)
       :client (-> client (add-server-data server) (add-ssl-data ssl)), 
       ; server data first, then ssl data (since server port info is needed)
       :nodes (vec (map #(-> % (add-server-data server) (add-ssl-data ssl)) nodes))})))


(defmacro ^::config defcluster
  [cluster-name, & config-coll]
  `(def ~(with-config-type :sputnik/cluster 'cluster)
     (create-cluster ~(name cluster-name), ~(vec config-coll))))


(defn extract-fn+args
  [spec]
  (vector
    (if (vector? spec) (first spec) spec)
    (when (vector? spec) (subvec spec 1))))

(defn serialize->string
  [x]
  (binding [*print-dup* true]
    (pr-str x)))

(defn parse-result-options
  [opts-coll]
  (when (seq opts-coll)
    (apply hash-map opts-coll)))

(defmacro ^::config defwork
  "Defines how sputnik can access the jobs it has to complete.
  job-fn ... Clojure function (or static Java function) which returns a collection of jobs.
  args ... arbitrary number of parameters that are passed to the job-fn.
  
  Examples:
  (defwork \"example jobs\" 
	  [\"example-application.jobs/get-jobs\" \"hello world\"]
	  [\"example-application.jobs/task-finished\" :async true])

  (defwork \"great experiment\"
    \"example-application.jobs/get-jobs\" 
    \"example-application.jobs/task-finished\")

  Currently, jobs and tasks are maps with the following structure:

  job:
  {:name \"job1\", :tasks [task1, task2, ...]}

  task:
  {:id 4711, :task-fn \"my-application/compute\" :task-data <some-data-for the task-fn>}
  "
  [work-name, job-fn-spec, result-fn-spec]
  (let [[job-fn, job-args] (extract-fn+args job-fn-spec)
        [result-fn, result-options] (extract-fn+args result-fn-spec)]
	 `(def ~(with-config-type :sputnik/work 'work)
      (merge
        {:name ~(name work-name), 
         :job-fn ~(str job-fn), :job-args (serialize->string (vec ~job-args))
         :result-fn ~(str result-fn)}
         (parse-result-options ~result-options)))))



(defn+opts load-config
  "Loads a sputnik configuration file and returns all define configuration data in map from symbol to value."
  [url | {namespace "config"}]
  (locking load-config
    (let [config-ns-symb (gensym namespace)
	        config-ns (create-ns config-ns-symb)]
	    (binding [*ns* config-ns]
	      (clojure.core/refer-clojure)
	      (use ['jppfnik-control.config :only (config-commands)])
	      (try (load-file url)
	        (catch Exception e
	           (throw (Exception. (format "Error loading configuration \"%s\"!" url) e)))))
	    (let [config-map (->> config-ns 
	                       ns-publics
	                       (filter #(-> % val config-type?))
	                       (reduce 
	                         (fn [m, [k v]] 
	                           (assoc! m (with-type (config-type v) k) (var-get v))) 
	                         (transient {}))
	                       persistent!)]
	      (remove-ns config-ns-symb)
	      config-map))))


(defn select-configs
  "Returns the config values of the given type."
  [config-type, config-map]
  (->> config-map
    vals
    (filter #(= (type %) config-type))))


(defn pallet-user
  "Creates a pallet user object from the given sputnik user configuration."
  [user-map]
  (apply utils/make-user (:username user-map)
    ; remove :username and then flatten the key-value-pair sequence to be used as options
    (-> user-map (dissoc :username) seq flatten)))

(defn pallet-hosts
  [host-coll]
  (vec 
    (for [{:keys [name group address] :as host} host-coll] 
      (into [name group address :ubuntu] (apply concat (dissoc  host :name :group :address))))))

(defn pallet-compute-service
  "Creates a pallet compute service for the given collection of sputnik host configurations."
  [host-coll]
  (compute/compute-service "node-list" :node-list (pallet-hosts host-coll)))
