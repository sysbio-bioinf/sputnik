; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.tools.resolve
  (:require
    [clojure.tools.logging :as log]
    [sputnik.tools.error :as e]))


(defmacro error-feedback
  [fail-on-error?, & args]
  `(if ~fail-on-error?
     (e/illegal-argument ~@args)
     (do
       (log/errorf ~@args)
       nil)))


(defn resolve-fn
  "Retrieve function specified by the given symbol or string.
  This function is only thread-safe when txload is enabled previously."
  ([symbol-str]
    (resolve-fn symbol-str, true))
  ([symbol-str, fail-on-error?]
    (if (or (string? symbol-str) (symbol? symbol-str))    
      (let [symb (symbol symbol-str)
            symb-ns (namespace symb)]
        (if (nil? symb-ns) 
          (error-feedback fail-on-error?, "Function symbol \"%s\" must have a full qualified namespace!" symb)
          (do
            ; load namespace if needed - require is thread safe thanks to txload (must be enabled at startup)
            (require (symbol symb-ns))
            ; resolve symbol
            (if-let [v (some-> symb resolve var-get)]
	             v
	             (error-feedback fail-on-error?, "Function \"%s\" does not exist!" symb)))))
      (cond
        (ifn? symbol-str)
          symbol-str
        (nil? symbol-str)
          (error-feedback fail-on-error?, "No function identifier given (argument is nil)!")
        :else
          (error-feedback fail-on-error?, "resolve-fn expects a symbol or a string")))))