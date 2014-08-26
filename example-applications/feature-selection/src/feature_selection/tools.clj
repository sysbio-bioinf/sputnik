; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns feature-selection.tools
  (:require
    [clojure.java.io :as io]))


(defn- remove-meta
  [x]
  (with-meta x nil))


(defn- args-without-meta
  [args]
  (mapv remove-meta args))


(defn- class->symbol
  [^Class cls]
  (symbol (.getCanonicalName cls)))



(defmacro definline-method-fn
  "Creates an function with inline support that accesses a specified method of an object.
  When sufficient type hints are given on the parameters the implementation assures that no reflection will occur."
  {:arglists '([name, (method [params+])])}
  [fn-name, [method, arg-vec]]
  (let [obj-tag (some-> arg-vec first meta :tag),
        obj-type (when obj-tag ((ns-map *ns*) obj-tag)),
        obj-type (cond-> obj-type (class? obj-type) class->symbol)
        args (args-without-meta arg-vec),
        arg0 (first args)
        first-arg (gensym (name arg0)),
        first-arg-with-meta (with-meta first-arg {:tag obj-type})]
    `(defn ~fn-name
       {:inline (fn ~(symbol (str (name fn-name) "-inliner")) ~args
                  `(let [~'~first-arg-with-meta ~~arg0]
                     (. ~'~first-arg ~'~method ~~@(rest args))))
        :doc ~(format "Calls the method %s on the object of type %s. This function is inlined if possible." (name method) (some-> obj-tag name))
        :arglists (quote (list ~arg-vec))}
       ~arg-vec
       (. ~arg0 ~method ~@(rest args)))))


(def ^:private output-agent (agent nil))

(defn output*
  [output-fn]
  (send-off output-agent (fn [_] (output-fn)))
  nil)

(defmacro output-future
  [& body]
  `(output* (^:once fn* [] ~@body)))

(defn wait-for-finished-output
  []
  (await output-agent))