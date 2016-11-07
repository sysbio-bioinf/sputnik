; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.role-based-messages
  (:require
    [clojure.tools.logging :as log]
    [sputnik.satellite.messaging :as msg]
    [sputnik.satellite.protocol :as protocol]
    [sputnik.satellite.error :as error]))


(protocol/defmessage role-request :all role)
(protocol/defmessage role-granted :all role)

(defprotocol IRoleStorage
  (role! [storage, remote-node, role])
  (role  [storage, remote-node]))

(defn set-role*
  [role-map-atom, remote-node, role]
  (swap! role-map-atom assoc (msg/id remote-node) role))

(defn get-role*
  [role-map-atom, remote-node]
  (get (deref role-map-atom) (msg/id remote-node)))


(defn handle-role-request
  [handle-role-assigned-fn, this-node, remote-node, {:keys [role] :as msg}]
  (log/infof "Node %s requested role %s." (msg/address-str remote-node) role)
  (role! this-node, remote-node, role)
  (msg/send-message remote-node (role-granted-message role))
  (when handle-role-assigned-fn
    (handle-role-assigned-fn this-node, remote-node, role))
  nil)


(defn handle-message-checked
  ([handle-message-fn, this-node, remote-node, msg]
    (handle-message-checked handle-message-fn, this-node, remote-node, msg, nil))
  ([handle-message-fn, this-node, remote-node, msg, handle-role-assigned-fn]
	  (if (= (type msg) :role-request)
	    (handle-role-request handle-role-assigned-fn, this-node, remote-node, msg)
	    (let [role (role this-node, remote-node)]
		    (if (protocol/message-allowed? role, msg)
		      (handle-message-fn this-node, remote-node, msg)
		      (error/send-error this-node, remote-node, :message-not-allowed, :wrong-node-role, 
		        (format "The node with role \"%s\" is not allowed to send messages of type \"%s\"!" 
		          (pr-str role) (type msg))))))))