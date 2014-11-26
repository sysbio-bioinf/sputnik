; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-control.remote
  (:require
    [pallet.core :as pallet]
    [pallet.phase :as phase]
    [pallet.action.exec-script :as exec-script]
    [pallet.action.directory :as directory]
    [pallet.action.remote-file :as remote-file]
    [pallet.action.rsync :as rsync]    
    [pallet.stevedore :as stevedore]
    [clojure.string :as string] 
    [clojure.java.io :as io]
    [jppfnik-control.config :as config]
    [jppfnik-control.file-system :as fs])
  (:use
    [clojure.options :only [defn+opts]]))


(set! *warn-on-reflection* true)


(defn make-sequential
  [x]
  (if (sequential? x) x [x]))

(defn+opts execute
  "Executes operations via pallet on the given hosts using the given user.
  There has to be given one function or a sequence of functions in `fn-coll` which only take a session as argument
  and returns the altered session afterwards.
  <group>Specifies the group of the hosts that shall be used</group>"
  [host-coll, user, fn-coll | {group "default"}]
  (let [exec-fn-coll (fn [session]
	                     (reduce 
	                       (fn [session, f] (phase/check-session (f session), (format "The function \"%s\" in the function collection" (class f)))) 
	                       session 
	                       (make-sequential fn-coll)))]
    (pallet/lift 
		  (pallet/group-spec group
		    :phases {:configure (phase/phase-fn exec-fn-coll)})
		    :user (config/pallet-user user)
		    :compute (config/pallet-compute-service (make-sequential host-coll)))))

(defmacro exec-fn
  [& forms]
 `(fn [session#] ))

(defn+opts execute-script
  "Executes a script via pallet on the given hosts using the given user."
  [host-coll, user, script-fn | :as options]
  (->> (execute host-coll, user, script-fn, options)    
    :results
    (reduce-kv 
      (fn [m, k, v] (assoc! m k (-> v :configure first)))
      (transient {}))
    persistent!))


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
  [host-coll, user, source-file-coll, target-dir-name | :as options]
  (when (seq source-file-coll)
    (execute host-coll, user, 
      (phase/phase-fn
        (directory/directory target-dir-name :action :create :mode "700")
        (upload-files source-file-coll, (fs/assure-directory-name target-dir-name))),
      options)))


(defn+opts delete-directory
  [host-coll, user, dir-name | :as options]
  (execute host-coll, user, 
    (phase/phase-fn
      (directory/directory dir-name, :action :delete, :recursive true))      
    options))


(defn+opts sync-directory
  [session, source-dirs, target-dir-name | {port 22} :as options]
  (reduce
    (fn [session, dir-name]
      (rsync/rsync session, dir-name, target-dir-name, options))
    session
    source-dirs))

(defn+opts upload-directories
  "Uploads a collection of directories to the given hosts via rsync."
  [host-coll, user, source-dirs, target-dir-name | :as options]
  (let [source-dirs (make-sequential source-dirs)
        ; group into host with port and those without
        {hosts-with-port true, regular-hosts false} (group-by #(-> % :ssh-port boolean) (make-sequential host-coll))] 
    (execute host-coll, user, (phase/phase-fn (sync-directory source-dirs, target-dir-name)), options)
    (doseq [[ssh-port hosts] (group-by :ssh-port hosts-with-port)]
      (execute hosts, user, (phase/phase-fn (sync-directory source-dirs, target-dir-name, :port ssh-port)), options))))