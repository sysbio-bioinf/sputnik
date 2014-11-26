; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-tools.functions
  (:require
    [jppfnik-tools.errors :as e]))


(def ^:private resolve-symbol-lock (Object.))


(defn- ns-loaded?
  [ns]
  (get (loaded-libs) ns))


(defn- provide-ns
  "Thread-safe version of (require ns)."
  [ns]
  (when-not (ns-loaded? ns)
    (locking resolve-symbol-lock
      (when-not (ns-loaded? ns)
        (require ns)))))


(defn resolve-fn
  [symbol-str]
  (if (or (string? symbol-str) (symbol? symbol-str))    
    (let [symb (symbol symbol-str)
          symb-ns (namespace symb)]
	    (when (nil? symb-ns) 
	      (e/illegal-argument "Function symbol \"%s\" must have a full qualified namespace!" symb))
	    ; load namespace if needed
	    (provide-ns (symbol symb-ns))
	    ; resolve symbol
	    (if-let [v (resolve symb)]
	      v
	      (e/illegal-argument "Function \"%s\" does not exist!" symb)))
    (if (ifn? symbol-str)
      symbol-str
      (e/illegal-argument "resolve-fn expects a symbol or a string"))))