; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.error
  (:require
    [sputnik.satellite.node :as node]
    [sputnik.satellite.protocol :as protocol]
    [clojure.tools.logging :as log]))


(protocol/defmessage error :all kind reason message)

(defn send-error
  "Sends a message containing error information to the given `remote-node`."
  [this-node, remote-node, kind, reason, message]
  (log/error (format "Send error to %s (type = %s reason = %s): %s" (node/node-info remote-node), type, reason, message))
  (let [error-msg (error-message kind, reason, message)]
    (println "\nsend-error\nerror message =" error-msg)
    (println "meta =" (meta error-msg))
    (node/send-message remote-node error-msg)))