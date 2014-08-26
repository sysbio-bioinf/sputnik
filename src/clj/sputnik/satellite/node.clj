; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.node
  (:import
    sputnik.satellite.Node
    java.rmi.server.UnicastRemoteObject
    java.util.UUID
    (java.rmi.registry LocateRegistry Registry)  
    (javax.rmi.ssl SslRMIClientSocketFactory SslRMIServerSocketFactory)
    (java.util.concurrent ThreadPoolExecutor LinkedBlockingQueue TimeUnit ExecutorService))
  (:require
    [frost.byte-freezer :as freezer]
    [sputnik.satellite.ssl :as ssl])
  (:use
    [clojure.options :only [defn+opts]]
    [clojure.tools.logging :only [trace, debug, error]]
    [clojure.stacktrace :only [print-stack-trace, print-cause-trace]]))



(defn thread-local*
  [init-fn]
  (let [local (proxy [ThreadLocal] [] (initialValue [] (init-fn)))]
    (reify
      clojure.lang.IDeref
      (deref [_] (.get local)))))


(defmacro thread-local
  "Defines a thread local constant."
  [& init]
 `(thread-local* (fn* [] ~@init)))


(defn+opts ^ThreadPoolExecutor create-thread-pool
  [| {thread-count 1}]
  (ThreadPoolExecutor. thread-count, thread-count, 0, TimeUnit/MILLISECONDS, (LinkedBlockingQueue.)))



(defonce id-map (atom {}))

(defn ^UUID create-id
  ([]
    (UUID/randomUUID))
  ([remote-host, remote-name, remote-port]
    (let [nodekey (str remote-name "@" remote-host ":" remote-port)] 
      (-> id-map
        (swap! #(if (contains? % nodekey) 
                  % 
                  (assoc % nodekey (UUID/nameUUIDFromBytes (.getBytes nodekey)))))
        (get nodekey)))))


(defonce ^{:private true} rmi-registries (ref {}))

(defn+opts ^Registry create-rmi-registry
  "Creates a new rmi"
  [port | {ssl-enabled false}]
  (let [create (fn [] 
                 (if ssl-enabled
                   (LocateRegistry/createRegistry (int port), (SslRMIClientSocketFactory.), (SslRMIServerSocketFactory.))
                   (LocateRegistry/createRegistry (int port))))
        params {:port port, :ssl-enabled ssl-enabled}] 
    (dosync
      (alter rmi-registries #(if (get % params) % (assoc % params (create))))
      (get @rmi-registries params))))


(defn+opts ^Registry connect-to-rmi-registry
  [^String host, port | {ssl-enabled false}]
  (if ssl-enabled 
    (LocateRegistry/getRegistry host, (int port), (SslRMIClientSocketFactory.))
    (LocateRegistry/getRegistry host, (int port))))


(defn lookup-node-with-retries
  [^Registry registry, nodename]
  (loop []
    (if-let [reg (try 
                   (.lookup registry nodename)
                   (catch java.rmi.ConnectException e
                     (error
                       (format "Connection to node %s failed with exception:\n%s", nodename, (with-out-str (print-stack-trace e))))
                     nil))]
      reg
      (do
        ; sleep 1 sec and try again
        (Thread/sleep 1000)
        (recur)))))


(defprotocol IShutdown
  (shutdown [this, now?]))

(defprotocol INodeInfo
  (node-info [this])
  (short-node-info [this]))

(defprotocol ISputnikNode
  (get-remote-node [this, id])
  (remote-nodes [this])
  (remote-node-unreachable [this, id])
  (connect-to [this, hostname, nodename, port])
  (disconnect [this, id])
  (execute [this, action])
  (freeze [this, obj])
  (defrost [this, obj-bytes])
  (hostname [this])
  (nodename [this])
  (set-rmi-node [this, rmi-node])
  (set-logout-handler [this, handler])
  (set-node-check-future [this, f])
  (user-data [this])
  (user-data! [this, value]))


(defprotocol ISputnikRemoteNode
  (login [this, hostname, nodename, port])
  (send-message [this, msg])
  (logout [this])
  (node-id [this])
  (set-data [this, key, data]) 
  (get-data [this, key])
  (active! [this] "Mark the remote node as active.")
  (last-activity [this] "Return timestamp of the last activity.")
  (ping [this]))


; byte-freezer is a thread-local constant so it must be dereferenced
(deftype SputnikRemoteNode 
  [^Node rmi-stub, byte-freezer, ^{:volatile-mutable true} remote-id, remote-hostname, remote-nodename, remote-port, local-id, data, timestamp-atom]
  
  ISputnikRemoteNode
  
  (login [this, hostname, nodename, port]
    (when (nil? remote-id)      
      (set! remote-id :connecting)
      (let [rid (.login rmi-stub hostname, nodename, port)]
        (debug (format "Login to remote node %s@%s:%s returned remote-id = %s." remote-nodename, remote-hostname, remote-port, rid))
        (set! remote-id rid))))
  
  (send-message [this, msg]
    (.sendMessage rmi-stub, remote-id, ^bytes (freezer/freeze @byte-freezer, msg)))
  
  (logout [this]
    (when remote-id
      (.logout rmi-stub, remote-id)
      (set! remote-id nil))
    nil)
  
  (node-id [this]
    local-id)
  
  (set-data [this, key, val]
    (swap! data assoc key val)
    nil)
  
  (get-data [this, key]
    (get @data key))
  
  (active! [this]
    (reset! timestamp-atom (System/currentTimeMillis)))
  
  (last-activity [this]
    @timestamp-atom)
  
  (ping [this]
    (try
      (trace (format "Pinging %s ..." (node-info this)))
      (.ping rmi-stub)
      (active! this)
      (trace (format "Ping to %s succeeded." (node-info this)))
      true
      (catch Exception e
        (trace (format "Ping to %s failed with exception:\n%s" (node-info this) (with-out-str (print-cause-trace e))))
        false)))
  
  INodeInfo
  
  (node-info [this]
    (format "%s@%s:%s [%s]" remote-nodename, remote-hostname, remote-port, local-id))
  
  (short-node-info [this]
    (format "%s@%s" remote-nodename, remote-hostname)))


(defn+opts create-remote-node
  [rmi-stub, remote-id, remote-host, remote-name, remote-port, local-id, thread-local-byte-freezer]
   (SputnikRemoteNode. 
      rmi-stub,
      thread-local-byte-freezer,
      remote-id,
      remote-host,
      remote-name,
      remote-port,
      local-id,
      (atom {})
      (atom 0)))



(defn already-connected?
  "Checks whether there is already a remote node registered for the given local id.
  If so, it needs to respond to a ping to be considered as connected."
  [remote-node-map, local-id]
  (when-let [remote-node (get remote-node-map local-id)]
    (ping remote-node)))

(defn maybe-connect-to-remote
  [remote-nodes, local-id, remote-hostname, remote-nodename, remote-port, ssl-enabled?, thread-local-byte-freezer]
  (if (already-connected? remote-nodes local-id) 
    remote-nodes
    (let [remote-registry (connect-to-rmi-registry remote-hostname, remote-port, :ssl-enabled ssl-enabled?),
          rmi-stub ^Node (lookup-node-with-retries remote-registry remote-nodename),
          rnode (create-remote-node rmi-stub, nil, remote-hostname, remote-nodename, remote-port, local-id, thread-local-byte-freezer)]      
      (assoc remote-nodes local-id rnode))))


(defn shutdown-thread-pool
  [^ThreadPoolExecutor thread-pool, now?]
  (if now?
    (.shutdownNow thread-pool)
    (.shutdown thread-pool))
  ; await termination
  (loop []
    (let [terminated? (try (.awaitTermination thread-pool 1, TimeUnit/SECONDS) (catch InterruptedException e false))]
      (when-not terminated?
        (recur)))))


; byte-freezer is a thread-local constant so it must be dereferenced
(deftype SputnikNode [^Registry rmi-registry, ^{:volatile-mutable true} logout-handler, ^{:volatile-mutable true} rmi-node, ^{:volatile-mutable true} node-check-future, ^ThreadPoolExecutor thread-pool, byte-freezer, remote-nodes, hostname, nodename, port, ssl-enabled?, custom-user-data]
  
  ISputnikNode
  (get-remote-node [this, id]
    (get @remote-nodes id))
  
  (remote-nodes [this]
    (vals @remote-nodes))
  
  (remote-node-unreachable [this, id]
    (logout-handler id, :unreachable))
  
  (connect-to [this, remote-hostname, remote-nodename, remote-port]
    (debug (format "%s => Connection to %s@%s:%s requested." (node-info this) remote-nodename remote-hostname remote-port))
    (let [local-id (create-id remote-hostname, remote-nodename, remote-port)]
      (-> (swap! remote-nodes maybe-connect-to-remote 
            local-id, remote-hostname, remote-nodename, remote-port, ssl-enabled?, byte-freezer) 
        (get local-id)
        (doto 
          (login hostname, nodename, port)))))
  
  (disconnect [this, id]
    (swap! remote-nodes dissoc id))
  
  (execute [this, action]
    (.submit thread-pool ^Callable action))
  
  (freeze [this, obj]
    (freezer/freeze @byte-freezer, obj))
  
  (defrost [this, obj-bytes]
    (freezer/defrost @byte-freezer, obj-bytes))
  
  (hostname [this] hostname)
  (nodename [this] nodename)
  
  (user-data [this] custom-user-data)
  
  (set-rmi-node [this, new-rmi-node]
    (set! rmi-node new-rmi-node))
  
  (set-node-check-future [this, f]
    (set! node-check-future f))
  
  (set-logout-handler [this, handler]
    (set! logout-handler handler))
  
  IShutdown
  
  (shutdown [this, now?]
    (debug (format "Shutting down (now? = %s)" now?))
    ; first give the user data instance time to shut down ...
    (when (satisfies? IShutdown custom-user-data)
      (shutdown custom-user-data now?))
    ; ... then shutdown the thread pool ...
    (shutdown-thread-pool thread-pool, now?)
    ; ... finally logout at every remote node
    (doseq [[_ remote-node] @remote-nodes]
      (trace (format "Logging out at %s" (node-info remote-node)))
      (try (logout remote-node)
        ; do not care if logout fails (we tried our best to inform that node)
        (catch Throwable t nil)))
    ; free remote nodes
    (reset! remote-nodes {})
    ; unbind the rmi node to be able to reuse the name later
    (.unbind rmi-registry nodename))
  
  INodeInfo
  
  (node-info [this]
    (format "%s@%s:%s" nodename, hostname, port))
  
  (short-node-info [this]
    (format "%s@%s" nodename, hostname)))


(defmacro safe-execute
  [sputnik-node & body]
 `(execute ~sputnik-node
    (fn []
      (try
        ~@body
        (catch Throwable t#
          (error (with-out-str (print-cause-trace t#))))))))


(defn+opts create-sputnik-node
  "<user-data>Additional user data associated with the node.</user-data>"
  [hostname, nodename, port, rmi-registry, ssl-enabled? | {user-data nil} :as options]
  (SputnikNode. 
    rmi-registry,
    nil,
    nil,
    nil,
    (create-thread-pool options),
    (thread-local (freezer/create-byte-freezer options)),
    (atom {}),
    hostname,
    nodename,
    port,
    ssl-enabled?,
    user-data))



(defmacro rmi-node-proxy
  [login-handler, message-handler, logout-handler, args]
 `(proxy [UnicastRemoteObject, Node] ~args
      (login[remote-host#, remote-name#, remote-port#]
        (~login-handler remote-host#, remote-name#, remote-port#))
      (sendMessage [id#, message#]
        (~message-handler id#, message#))
      (logout [id#]
        (~logout-handler id#, :disconnect))
      (ping []
        (trace "Ping received."))))


(defn+opts create-rmi-node
  [login-handler, message-handler, logout-handler | {node-port 0, ssl-enabled false} :as options]
  (if ssl-enabled
    (rmi-node-proxy login-handler, message-handler, logout-handler, 
      [node-port, (SslRMIClientSocketFactory.), (SslRMIServerSocketFactory.)])
    (rmi-node-proxy login-handler, message-handler, logout-handler, 
      [node-port])))


(defn login-handler-wrapper
  [^SputnikNode this-node, login-handler-fn, remote-host, remote-name, remote-port]
  (debug (format "%s => Login handler for remote node %s@%s:%s called." (node-info this-node) remote-name remote-host remote-port))
  (let [remote-node (connect-to this-node remote-host, remote-name, remote-port)]    
    (when login-handler-fn
      (safe-execute this-node (login-handler-fn this-node, remote-node)))
    (node-id remote-node)))
  


(defn logout-handler-wrapper
  [^SputnikNode this-node, logout-handler-fn, id, reason]
  (when logout-handler-fn
    (when-let [remote-node (get-remote-node this-node id)]
      (safe-execute this-node (logout-handler-fn this-node, remote-node, reason))))
  (disconnect this-node, id)
  nil)


(defonce defrost-error (Object.))



(defn message-handler-wrapper
  [this-node, message-handler, id, message]
  (safe-execute this-node
    (let [msg (try 
                (defrost this-node, message)
                (catch Throwable t
                  (error
                    (format "Exception when unfreezing message.\n%s" (with-out-str (print-cause-trace t))))
                  defrost-error))
          remote-node (get-remote-node this-node id)]
      (if remote-node
        (do
          (active! remote-node)
	        (when-not (= msg defrost-error)
	          (try
			        (message-handler this-node, remote-node, msg)
			        (catch Throwable t
			          (error 
			            (format "Exception when handling message from %s:\n%s" 
			              (node-info remote-node)
			              (if (.getStackTrace t)
                      (with-out-str (print-cause-trace t))
                      (format "%s (NO STACKTRACE)" (.getMessage t)))))))))
        (error (format "message-handler: No remote node found for ID \"%s\"." id)))))
  nil)


(defn remote-node-check 
  [this-node, alive-check-timeout]
  (debug "Remote node check thread has started.")
  (let [sleep-duration (quot alive-check-timeout 2)]
    (loop []
	    (try
	      ; sleep until next check
	      (Thread/sleep sleep-duration)
	      ; TODO: complete transaction for node-data-list access and updates? 
	      ; in principle: data will be changed only if a node is not reachable anymore and then missed updates to its data do not matter.
	      (let [remote-node-list (remote-nodes this-node),
	            now (System/currentTimeMillis)]        
	        (doseq [remote-node remote-node-list]
	          (when (> (- now (last-activity remote-node)) alive-check-timeout)
	            (trace (format "Node %s was not active in the last %sms." (node-info remote-node) alive-check-timeout))
	            (if (ping remote-node)
	              (trace (format "Node %s is still alive." (node-info remote-node)))
	              (do
	                (error (format "Node %s did not respond to ping!" (node-info remote-node)))
	                (remote-node-unreachable this-node, (node-id remote-node)))))))
	      (catch Throwable t
	        (error (format "Exception caught in the node check thread:\n%s" (with-out-str (print-cause-trace t))))))
	    (recur))))


(defn+opts start-remote-node-check-thread
  "
  <alive-check-timeout>Timeout (in msec) after which all remote nodes are checked whether they are still alive.</>"
  [this-node | {alive-check-timeout 500}]
  (let [f (future (remote-node-check this-node, alive-check-timeout))]
    (doto this-node
      (set-node-check-future f))))


(defn+opts start-node
  "Starts a node which can receive messages over network.
   message-handler is a function with parameters [this-node, remote-node, msg]
  <login-handler>Function with parameters [this-node, remote-node] that is called when a node logs in.</>
  <logout-handler>Function with parameters [this-node, remote-node, reason] that is called when a node logs out
  due to one of the following reasons: :disconnect, :unreachable.</>"
  [hostname, nodename, port, message-handler | {ssl-enabled false, login-handler nil, logout-handler nil} :as options]
  (when ssl-enabled (ssl/setup-ssl options))
  ; set hostname otherwise RMI might pick something like 127.0.1.1 from /etc/hosts 
  (System/setProperty "java.rmi.server.hostname" hostname)
  (let [rmi-registry (create-rmi-registry port, options),        
        sputnik-node (create-sputnik-node hostname, nodename, port, rmi-registry, ssl-enabled, options),
        logout-handler (partial logout-handler-wrapper sputnik-node, logout-handler),        
        rmi-node (create-rmi-node
                   (partial login-handler-wrapper sputnik-node, login-handler),
                   (partial message-handler-wrapper sputnik-node, message-handler),
                   logout-handler,
                   options)]
    (set-rmi-node sputnik-node rmi-node)
    (set-logout-handler sputnik-node logout-handler)
    (.bind rmi-registry, nodename, rmi-node)
    (start-remote-node-check-thread sputnik-node, options)))



(defn test-message-handler
  [this-node, remote-node, msg] 
  (println "message received!")
  (println "remote node =" (node-info remote-node))
  (println msg) 
  (flush))