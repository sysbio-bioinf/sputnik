; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.tools.threads
  (:require
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [clojure.options :refer [defn+opts]])
  (:import
    (java.util.concurrent ThreadPoolExecutor LinkedBlockingQueue TimeUnit RejectedExecutionException ThreadFactory Executors)
    java.util.concurrent.atomic.AtomicLong
    (java.lang Thread$UncaughtExceptionHandler)))



(defn+opts ^ThreadPoolExecutor create-thread-pool
  [| {thread-count 1}]
  (ThreadPoolExecutor. thread-count, thread-count, 0, TimeUnit/MILLISECONDS, (LinkedBlockingQueue.)))


(defn shutdown-thread-pool
  [^ThreadPoolExecutor thread-pool, now?]
  (if now?
    (.shutdownNow thread-pool)
    (.shutdown thread-pool))
  ; await termination
  (loop []
    (let [terminated? (try (.awaitTermination thread-pool 10, TimeUnit/SECONDS) (catch InterruptedException e false))]
      (when-not terminated?
        (recur)))))


(defn submit-action
  {:inline (fn [thread-pool, action]
             `(let [^ThreadPoolExecutor thread-pool# ~thread-pool,
                    ^Callable action# ~action]
                (.submit thread-pool# action#)))}
  [^ThreadPoolExecutor thread-pool, ^Callable action]
  (.submit thread-pool action))


(defprotocol IExecutor
  (execute [executor, action]))


(defmacro safe-execute
  [executor & body]
 (let [body-str (str/join " " (map pr-str body))]
   `(try
      (execute ~executor
        (fn []
          (try
            ~@body
            (catch Throwable t#
              (log/errorf "Exception during execution of %s:\n%s"
                ~body-str
                (with-out-str (print-cause-trace t#)))))))
      (catch RejectedExecutionException t#
        (log/warnf "Execution rejected: %s" ~body-str)))))



(defn thread*
  [f]
  (let [f (#'clojure.core/binding-conveyor-fn f),
        result (promise)
        ; wrap into function to deliver the result of the computation to the promise
        f (^:once fn* [] (deliver result (f)))]
    (doto (Thread. ^Runnable f)
      (.setDaemon false)
      .start)
    result))


(defmacro thread
  "Runs the specified body in non-daemon thread returning a promise that will contain the result of the computation."
  [& body]
  `(thread* (^:once fn* [] ~@body)))


(defn create-thread-factory
  [name-fmt]
  (let [cnt (AtomicLong. 0)]
    (reify ThreadFactory
      (newThread [_, runnable]
        (doto (Thread. runnable)
          (.setName (format name-fmt (.getAndIncrement cnt)))
          (.setDaemon true))))))


(defn replace-agent-thread-pools!
  "Replaces the default agent thread pools with new ones that construct daemon threads and hence do not prevent the JVM from terminating."
  []
  (.shutdownNow clojure.lang.Agent/pooledExecutor)
  (set! clojure.lang.Agent/pooledExecutor 
    (Executors/newFixedThreadPool
      (+ 2 (.availableProcessors (Runtime/getRuntime))),
      (create-thread-factory "clojure-agent-send-pool-%d")))
  (.shutdownNow clojure.lang.Agent/soloExecutor)
  (set! clojure.lang.Agent/soloExecutor
    (Executors/newCachedThreadPool
      (create-thread-factory "clojure-agent-send-off-pool-%d"))))


(defn setup-default-thread-exception-handler!
  []
  (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException [this, t, e]
        (log/errorf "Uncaught exception in thread %s (%s):\n%s"
          (.getName t)
          (.getId t)
          (with-out-str (print-cause-trace e)))))))


(defn add-shutdown-hook!
  [hook-fn]
  (.addShutdownHook (Runtime/getRuntime)
    (Thread. ^Runnable hook-fn)))


(defn setup-exit-monitor!
  [monitor-fn]
  (System/setSecurityManager
    (proxy [SecurityManager] []
      (checkPermission
        ([perm] #_nix)
        ([perm, obj] #_nix))
      (checkExit [status]
        (monitor-fn status)))))