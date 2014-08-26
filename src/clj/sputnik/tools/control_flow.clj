; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.tools.control-flow)


(defmacro cond-wrap
  "If condition evaluates to true the expression will be evaluated wrapped as (~@wrap-expr body-expr);
  otherwise the pure expression will be evaluated."
  [condition, wrap-expr, body-expr]
  `(if ~condition
     (~@(if (sequential? wrap-expr) wrap-expr [wrap-expr]) ~body-expr)
     ~body-expr))