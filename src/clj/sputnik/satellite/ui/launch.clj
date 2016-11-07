; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.ui.launch
  (:require 
    [ring.adapter.jetty :as jetty]
    [sputnik.satellite.ui.routes :as routes]
    [sputnik.tools.file-system :as fs]
    [cemerick.friend :as friend]
    [clojure.tools.logging :refer [errorf, infof, debugf, tracef]]
    [clojure.options :refer [defn+opts]])
  (:import
    org.eclipse.jetty.server.Server
    org.eclipse.jetty.server.ssl.SslSelectChannelConnector))


(defn use-only-tls
  [^Server jetty]
  (let [protocols (into-array String ["TLSv1.1" "TLSv1.2"])]
    (doseq [^SslSelectChannelConnector con (->> jetty
                                             .getConnectors
                                             (filter #(instance? SslSelectChannelConnector %)))]
      (.setIncludeProtocols (.getSslContextFactory con) protocols))))


(defn create-jetty-instance-ssl
  [server-node, port, admin-user, admin-password, ssl-port, keystore, keystore-password]
  (binding [friend/*default-scheme-ports* {:http port :https ssl-port}]
    (jetty/run-jetty (routes/app server-node, admin-user, admin-password, :ssl-port ssl-port) 
		                   {:port port
		                    :ssl? true
		                    :ssl-port ssl-port
		                    :keystore keystore
		                    :key-password keystore-password
                        :configurator use-only-tls
                        ; do not block
                        :join? false})))

(defn create-jetty-instance
  [server-node, port, admin-user, admin-password]
  (jetty/run-jetty (routes/app server-node, admin-user, admin-password) 
	                   {:port port
                      ; do not block
                      :join? false}))


(defn+opts launch
  "Launch Jetty server for the web user interface."
  [server-node, min-port, max-port, admin-user, admin-password | {ssl-enabled false, keystore nil, keystore-password nil}]
  (let [create-fn (if (and ssl-enabled (fs/find-file keystore))
                    (fn [server-node, port, admin-user, admin-password]
                      (create-jetty-instance-ssl server-node, port, admin-user, admin-password, (inc port), keystore, keystore-password))
                    create-jetty-instance),
        step (if ssl-enabled 2 1),
        success-fn (if ssl-enabled
                     (fn [port]
                       (infof "Server running: http port = %d, https port = %d", port, (inc port))
                       (println (format "Server Web UI running: http port = %d, https port = %d", port, (inc port))))
                     (fn [port]
                       (infof "Server running: http port = %d", port)
                       (println (format "Server Web UI running: http port = %d", port))))
        ] 
    (loop [port min-port]
	    (if (> (inc port) max-port)
	      (throw (Exception. (format "Could not find unused ports in the range [%d, %d]!" min-port max-port)))
	      (let [ssl-port (inc port)]
		      (if-let [jetty-instance (try 
		                                (create-fn server-node, port, admin-user, admin-password)
					                          (catch org.eclipse.jetty.util.MultiException me
					                            (let [[e1 e2 & others] (.getThrowables me)]
					                              (if (and (zero? (count others)) (some #(instance? java.net.BindException %) [e1 e2]))
					                                ;(debugf "Port %s or %s already in use!" port (inc port))
                                          (do
                                            (debugf e1 "Port already in use")
                                            (debugf e2 "Port already in use"))
		                                      (throw me)))))]
		        (do
		          (success-fn port)
		          jetty-instance)
            (recur (+ port step))))))))