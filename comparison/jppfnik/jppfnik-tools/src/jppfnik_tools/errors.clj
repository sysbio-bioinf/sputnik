; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-tools.errors)


(defmacro create-error-fn [name, exception]
 `(defn ~name
	  ~(format "Throws an %s with a message defined by `format` applied to msg-fmt and the given args.\n  A `Throwable` e can be provided as cause for the exception."
       (str exception))
	  {:arglists '~'([msg-fmt & args] [e msg-fmt & args])}
	  [& args#]
	  (let [e?# (->> args# first (instance? Throwable)),
	        e# (when e?# (first args#))
	        [msg-fmt# & args#] (if e?# (rest args#) args#)]
		  (throw (new ~exception (apply format msg-fmt# args#), e#)))))


(defmacro define-errors
  [& args]
  (when (-> args count (mod 2) (not= 0))
    (throw (IllegalArgumentException. "define-errors requires an even number of symbols")))
   `(do 
      ~@(for [[name exception] (partition-all 2 args)]
          `(create-error-fn ~name ~exception))))


(define-errors
  illegal-argument IllegalArgumentException)