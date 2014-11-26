; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.config.options)


(defn display->config
  [option-list]
  (into {}
    (for [{:keys [display-name, config-path]} option-list]
      [display-name (vec config-path)])))

(defn config->implementation
  [option-list]
  (into {}
    (for [{:keys [implementation-name, config-path]} option-list]
      [(vec config-path) implementation-name])))


(defn build-map
  [transform-fn, kv-list]
  (reduce
    (fn [m, [k, option-list]]
      (assoc m k (transform-fn option-list)))
    {}
    (partition-all 2 kv-list)))


(defmacro defoption-conversions
  [to-config-name, from-config-name & kv-list]
  `(let [kv-vec# ~(vec kv-list),
         display->config-map# (build-map display->config, kv-vec#),
         config->impl-map# (build-map config->implementation, kv-vec#)]
     (def ~to-config-name (partial to-config display->config-map#))
     (def ~from-config-name (partial from-config, config->impl-map#) )))


(defn option
  ([implementation-name]
    (option implementation-name, implementation-name, implementation-name))
  ([implementation-name, display-name]
    (option implementation-name, display-name, implementation-name))
  ([implementation-name, display-name, config-path]
    (hash-map
      :display-name display-name,     
      :implementation-name implementation-name,
      :config-path (if (sequential? config-path) (list* config-path) (list config-path)))))


(defn config-level
  [level, & options]
  (mapcat
    (fn [opt]
      (map #(update-in % [:config-path] conj level) (if (sequential? opt) opt [opt])))
    options))


(defn select-types
  [type-map, types]
  (reduce
    (fn [m, t]
      (merge m (get type-map t)))
    {}
    types))


(defn to-config
  [display->config-map, types, option-map]
  (let [types (if (sequential? types) types [types])
        config-path-map (select-types display->config-map, types)]
    (reduce-kv 
      (fn [m, k, v]
        (if-let [config-path (get config-path-map k)]
          (assoc-in m config-path v)
          (do
            (println (format "WARING! Option with key \"%s\" has no associated config-path!" k))
            (assoc m k v))))
      {}
      (or option-map {}))))


(defn contains-in?
  [m [k & ks]]
  (if (contains? m k)
    (if ks
      (recur (m k), ks)
      true)
    false))

(defn dissoc-in
  [m [k & ks]]
  (if (contains? m k)
    (if ks 
      (let [m' (dissoc-in (m k) ks)]
        (if (seq m')
          (assoc m k m')
          (dissoc m k)))
      (let [m' (dissoc m k)]
        (when (seq m')
          m')))
    m))

(defn warn-about-unused-options
  [option-map, impl-key-map]
  (let [unused-options (reduce dissoc-in option-map (keys impl-key-map))]
    (when (seq unused-options)
      (println (format "WARNING! The following options are not used: %s" (pr-str unused-options))))))

(defn from-config
  [config->impl-map, types, option-map]
  (let [types (if (sequential? types) types [types])
        impl-key-map (select-types config->impl-map, types)]
    (warn-about-unused-options option-map, impl-key-map)
    (reduce-kv 
      (fn [m, config-path, impl-key]
        (if (contains-in? option-map config-path)
          (assoc m impl-key (get-in option-map config-path))
          m))
      {}
      impl-key-map)))


; configuration of all available options for sputnik satellites
(defoption-conversions =>config, config=>

  :node (config-level :sputnik
          (config-level :node
            (option :registry-port)
            (option :node-port)
            (option :nodename)
            (option :hostname)
            (option :cpus)
            (option :alive-check-timeout)
            (config-level :server
              (option :server-hostname :server-hostname :hostname)
              (option :server-nodename :server-nodename :nodename)
              (option :server-registry-port :server-registry-port :registry-port)))),
  
  
  :logging (config-level :sputnik
             (config-level :logging
               (option :log-level, :log-level, :level)
               (option :max-size, :max-log-size)
               (option :backup-count, :log-backup-count)
               (option :custom-levels, :custom-log-levels))),  

  
  :communication (config-level :sputnik
                   (config-level :communication
                     (option :comm-thread-count, :comm-thread-count, :thread-count)
                     (config-level :buffer
                       (option :init, :buffer-init)
                       (option :max,  :buffer-max))
                     (config-level :compression
                       (option :compression-enabled, :compressed, :enabled)
                       (option :nowrap,  :no-wrap)
                       (option :compression-level, :compression-level, :level))
                     (config-level :ssl
                       (option :ssl-enabled, :ssl-enabled, :enabled)
                       (option :protocol)
                       (option :keystore)
                       (option :keystore-password)
                       (option :truststore)
                       (option :truststore-password)))),
  
  
  :sputnik/server (config-level :sputnik
                    (config-level :server
                      (config-level :scheduling
                        (option :timeout :scheduling-timeout)
                        (option :max-task-count-factor)
                        (option :worker-task-selection)
                        (option :worker-ranking)
                        (option :task-stealing)
                        (option :task-stealing-factor)
                        (option :interval-duration :scheduling-performance-interval-duration)
                        (option :interval-count    :scheduling-performance-interval-count))
                      (config-level :ui
                        (option :min-port, :min-ui-port)
                        (option :max-port, :max-ui-port)
                        (config-level :admin
                          (option :admin-user,     :admin-user, :user)
                          (option :admin-password, :admin-password, :password))))), 
  
  :sputnik/worker (config-level :sputnik
                    (config-level :worker
                      (option :thread-count, :worker-threads)
                      (option :send-result-timeout)
                      (option :max-results-to-send)
                      (option :send-results-parallel))),
  
  :sputnik/client (config-level :sputnik
                    (config-level :client
                      (option :job-function, :job-setup-fn, :function)
                      (option :job-args, :job-setup-args, :args))),
  
  :sputnik/rest-client (config-level :sputnik
                         (config-level :rest-client
                           (option :rest-port)))
)