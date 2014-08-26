; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.control.remote
  (:require
    [pallet.core :as pallet]
    [pallet.phase :as phase]
    [pallet.action.exec-script :as exec-script]
    [pallet.action.directory :as directory]
    [pallet.action.remote-file :as remote-file]
    [pallet.action.rsync :as rsync]    
    [pallet.stevedore :as stevedore]
    [pallet.execute :as execute]
    [clojure.string :as string] 
    [clojure.java.io :as io]
    [sputnik.config.pallet :as cfg-pallet]
    [sputnik.tools.file-system :as fs])
  (:use
    [clojure.options :only [defn+opts]]))



(defn make-sequential
  [x]
  (if (sequential? x) x [x]))


(defn+opts execute
  "Executes operations via pallet on the given hosts using the given user.
  There has to be given one function or a sequence of functions in `fn-coll` which only take a session as argument
  and returns the altered session afterwards.
  <group>Specifies the group of the hosts that shall be used.</>
  <raise-on-error>Specifies that an exception is thrown when an error occurs.</>"
  [node-coll, fn-coll | {group "default", raise-on-error false}]
  (let [exec-fn-coll (fn [session]
	                     (reduce 
	                       (fn [session, f] (phase/check-session (f session), (format "The function \"%s\" in the function collection" (class f)))) 
	                       session 
	                       (make-sequential fn-coll))),
        middleware [pallet/translate-action-plan execute/execute-with-ssh],
        middleware (if raise-on-error (conj middleware pallet/raise-on-error) middleware)]
    ; no ssh-user-credentials middleware  
    (binding [pallet/*middleware* middleware]
      (->> 
	      ; group by user since there hosts do not have their users associated
	      (group-by (comp :user :host) (make-sequential node-coll))
	      (mapv 
	        (fn [[user nodes]]
	          (future
	            (pallet/lift 
		            (pallet/group-spec group :phases {:configure (phase/phase-fn exec-fn-coll)})
		            :user (cfg-pallet/pallet-user user)
		            :compute (cfg-pallet/pallet-compute-service nodes)))))
	      (mapv deref)))))

(defmacro exec-fn
  [& forms]
 `(fn [session#] ))

(defn+opts execute-script
  "Executes a script via pallet on the given hosts using the given user."
  [node-coll, script-fn | :as options]
  (let [result-map-coll (execute node-coll, script-fn, options)]
    (when (seq result-map-coll)
      (->> result-map-coll
        (map :results)
        (reduce
          #(reduce-kv (fn [m, k, v] (assoc! m k (-> v :configure first))) % %2)
          (transient {}))
        persistent!))))


(defmacro script
  "Creates a function that wraps a bash script for usage with `exec-script`."
  [& forms]
 `(fn [session#] (exec-script/exec-checked-script session#, ~(string/join " " forms), ~@forms)))

(defmacro unchecked-script
  "Creates a function that wraps a bash script for usage with `exec-script`."
  [& forms]
 `(fn [session#] (exec-script/exec-script session#, ~@forms)))


(defn upload-files
  [session, source-file-coll, target-dir-name]
  (reduce
    (fn [session, file-path]
      (remote-file/remote-file session, (str target-dir-name (fs/filename file-path)), :local-file file-path))
    session
    source-file-coll))

(defn+opts upload-to-directory
  [host-coll, source-file-coll, target-dir-name | :as options]
  (when (seq source-file-coll)
    (execute host-coll, 
      (phase/phase-fn
        (directory/directory target-dir-name :action :create :mode "700")
        (upload-files source-file-coll, (fs/assure-directory-name target-dir-name))),
      options)))


(defn+opts delete-directory
  [host-coll, dir-name | :as options]
  (execute host-coll, 
    (phase/phase-fn
      (directory/directory dir-name, :action :delete, :recursive true))      
    options))


(defn sync-directory
  [session, source-dirs, target-dir-name]
  (reduce
    (fn [session, dir-name]
      (rsync/rsync session, dir-name, target-dir-name, {}))
    session
    source-dirs))

(defn+opts upload-directories
  "Uploads a collection of directories to the given hosts via rsync."
  [host-coll, source-dirs, target-dir-name | :as options]
  (let [source-dirs (make-sequential source-dirs)] 
    (execute host-coll, (phase/phase-fn (sync-directory source-dirs, target-dir-name)), options)))


(defn ping
  ([ip, port]
    (ping ip, port, 1000))
  ([ip, port, timeout]
    (try
      (let [addr (java.net.InetSocketAddress. ^String ip, (int port))]
        (with-open [s (java.net.Socket.)]
          (if timeout
            (.connect s, addr, (int timeout))
            (.connect s, addr))))
      true
      (catch Exception e
        false))))


(defn+opts filter-available-nodes
  "Pings the given nodes and returns a map of :available-nodes and :offline-nodes.
  <timeout>Determines the timeout for the ping</>"
  [nodes-coll | {timeout 1000}]
  (->> nodes-coll
    (mapv
      (fn [node]
        (future
          (let [{:keys [address ssh-port]} (:host node)]
            [(ping address, (or ssh-port 22), timeout) node]))))
    (mapv deref)
    (group-by first)
    (reduce-kv
      (fn [res, available?, node-list]
        (assoc res (if available? :available-nodes :offline-nodes) (mapv second node-list)))
      {})))