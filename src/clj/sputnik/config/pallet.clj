; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.config.pallet
  (:require
    [pallet.compute :as compute]
    [pallet.utils :as utils]))


(defn pallet-user
  "Creates a pallet user object from the given sputnik user configuration."
  [user-map]
  (apply utils/make-user (:username user-map)
    ; remove :username and then flatten the key-value-pair sequence to be used as options
    (-> user-map (dissoc :username) seq flatten)))

(defn pallet-hosts
  "Create pallet host configuration from the given list of node configurations."
  [node-coll]
  (vec 
    (for [{{:keys [name group address] :as host} :host} node-coll] 
      (into [name group address :ubuntu] (apply concat (dissoc  host :name :group :address :user))))))

(defn pallet-compute-service
  "Creates a pallet compute service for the given collection of sputnik host configurations."
  [node-coll]
  (compute/compute-service "node-list" :node-list (pallet-hosts node-coll)))