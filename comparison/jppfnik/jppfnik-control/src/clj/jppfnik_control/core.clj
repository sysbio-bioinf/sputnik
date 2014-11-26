; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-control.core
  (:import
    (org.jppf.server.protocol JPPFTask) 
    (org.jppf.client JPPFJob JPPFClient)
    org.jppf.client.concurrent.JPPFExecutorService))


(defonce *client* (JPPFClient.))
(defonce *executor-service* (JPPFExecutorService. *client*))




(defn task-call
  [f]
  (let [fut (.submit *executor-service* ^Callable f)]
    (reify
      clojure.lang.IDeref
	      (deref [_] (.get fut))
      clojure.lang.IBlockingDeref
	     (deref [_ timeout-ms timeout-val]
         (try (.get fut timeout-ms java.util.concurrent.TimeUnit/MILLISECONDS)
           (catch java.util.concurrent.TimeoutException e timeout-val)))
      clojure.lang.IPending
	     (isRealized [_] (.isDone fut))
      java.util.concurrent.Future
        (get [_] (.get fut))
	      (get [_ timeout unit] (.get fut timeout unit))
	      (isCancelled [_] (.isCancelled fut))
	      (isDone [_] (.isDone fut))
	      (cancel [_ interrupt?] (.cancel fut interrupt?)))))


(defmacro task
  [& body]
 `(task-call (^{:once true} fn* task [] ~@body)))