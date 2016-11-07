; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.ui.routes
  (:use
    compojure.core
    [clojure.options :only [defn+opts]])
  (:require
    [clojure.tools.logging :as log]
    [clojure.stacktrace :as st]
    [compojure.route :as route]
    [compojure.handler :as handler]
    [compojure.response :as response]
    [ring.util.response :as r.u.response]
    [ring.middleware.stacktrace :as r.m.stacktrace]
    [cemerick.friend :as friend]
    (cemerick.friend
      [workflows :as workflows]
      [credentials :as creds])
    [sputnik.satellite.ui.views :as views]))


(defn admin-routes
  [server-node]
  (routes
    (GET  "/" request (views/admin-main server-node, request))
    
    (POST "/worker/:worker-id" [worker-id, thread-count]
      (views/worker-thread-setup server-node, worker-id, thread-count)
      (r.u.response/redirect "/admin"))
    
    (POST "/worker/:worker-id/shutdown" [worker-id :as request]
      (views/worker-shutdown server-node, worker-id, request)
      (r.u.response/redirect "/admin"))
    
    (POST "/shutdown-workers" request
      (views/shutdown-workers server-node, request)
      (r.u.response/redirect "/admin"))
    
    (POST "/clear-finished-jobs" []
      (views/clear-finished-jobs server-node)
      (r.u.response/redirect "/admin"))
    
    (GET "/exceptions" request (views/exceptions server-node))
    
    (GET "/logs" request (views/server-logs-list server-node))
    
    (GET "/logs/:logname" [logname]
      (or
        (views/server-log server-node, logname)
        (r.u.response/redirect "/admin/logs")))))


(defn main-routes
  [server-node]
  (routes
    (context "/admin" request (friend/wrap-authorize (admin-routes server-node) #{::admin}))
    (GET "/" [] (views/public-index server-node))
    (route/resources "/")
    (GET "/login" request (views/login-page request))
    (friend/logout (ANY "/logout" request (ring.util.response/redirect "/")))
    (route/not-found (views/page-not-found))))



(defn create-user-map
  [admin-user, admin-password]
  {admin-user {:username admin-user
               :password (creds/hash-bcrypt admin-password)
               :roles #{::admin}}})


(defn wrap-exception-log
  "Wrap a handler such that exceptions are caught, logged and rethrown."
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception e
        (log/errorf "Exception for request:\n%s\nStacktrace:\n%s"
          (pr-str request)
          (with-out-str (st/print-cause-trace e)))
        (throw e)))))


(defn+opts app
  [server-node, admin-user, admin-password | {ssl-port nil}]     
  (handler/site
    (r.m.stacktrace/wrap-stacktrace-web
      (wrap-exception-log
        (friend/authenticate 
          (if ssl-port
	           (friend/requires-scheme (main-routes server-node) :https {:https ssl-port})
	           (main-routes server-node))
          {:credential-fn (partial creds/bcrypt-credential-fn (create-user-map admin-user, admin-password))
           :workflows [(workflows/interactive-form)]})))))