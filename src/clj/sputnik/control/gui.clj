; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.control.gui
  (:require
    [clojure.java.io :as io]
    [seesaw.core :as s]
    [seesaw.mig :as mig]
    [seesaw.bind :as b]
    [seesaw.table :as tbl]
    [seesaw.chooser :as ch]
    [sputnik.version :as v]
    [sputnik.tools.error :as e]
    [sputnik.config.api :as cfg]
    [sputnik.config.lang :as lang]
    [sputnik.config.meta :as meta]
    [sputnik.tools.file-system :as fs]
    [sputnik.tools.format :as fmt]
    [sputnik.control.launch :as launch]
    [sputnik.control.keystores :as ks]
    [clojure.string :as str]
    [clojure.java.classpath :as cp]
    [clojure.options :refer [defn+opts]])
  (:import
    javax.swing.JOptionPane))


(def ^:private ^:dynamic *debug* false)

;(s/native!)

(def ^:private node-display-properties
  ^{:config-type :sputnik/node}
  {:host-name       {:order 1, :path [:host :name],                :type :string, :additional-paths [[:sputnik/config-id]],
                     :caption "Host name"},
   :host-address    {:order 2, :path [:host :address],             :type :string, :additional-paths [[:options :hostname]],
                     :caption "Host address"},
   :user            {:order 3, :path [:host :user :username],      :type :string,
                     :caption "User account"},
   :ssh-port        {:order 4, :path [:host :ssh-port],            :type :int,
                     :caption "SSH port"},
   :cpus            {:order 5, :path [:options :cpus],             :type :int,
                     :caption "Number of available CPUs"},
   :sputnik-jvm     {:order 6, :path [:options :sputnik-jvm],      :type :string,
                     :caption "Custom Sputnik JVM"},
   :sputnik-numactl {:order 7, :path [:options :sputnik-numactl],  :type :string,
                     :caption "Custom Sputnik numactl"}})


(defn create-node-table
  []
  (s/scrollable
    (s/table
      :id :node-table
      :model
      [:columns
        [{:key :host-name,       :text "Name" }
         {:key :host-address,    :text "Address"}
         {:key :user,            :text "User account"}
         {:key :ssh-port,        :text "SSH port"}
         {:key :cpus,            :text "#CPUs"}
         {:key :sputnik-jvm,     :text "JVM"}
         {:key :sputnik-numactl, :text "numactl"}]])))




(defn select-node-configs
  [config-type, data-map]
  (->> data-map
    (cfg/select-configs config-type)
    (mapv :sputnik/config-id)
    sort))


(def ^:private server-display-properties
  ^{:config-type :sputnik/server} 
  {:server-name
     {:order 1,
      :path [:sputnik/config-id],
      :type :symbol,
      :caption "Server ID"},
   :server-node
     {:order 2,
      :path [:sputnik/role-node],
      :type :string,
      :caption "Server node",
      :choice (partial select-node-configs :sputnik/node)},
   :sputnik-port
     {:order 3,
      :path [:options :sputnik-port],
      :type :int,
      :caption "Port"},
   :admin-user
     {:order 4,
      :path [:options :admin-user],
      :type :string,
      :caption "Admin user"},
   :admin-password
     {:order 5,
      :path [:options :admin-password],
      :type :string,
      :caption "Admin password"},
   :min-ui-port
     {:order 6,
      :path [:options :min-ui-port],
      :type :int,
      :caption "Minimum Web UI HTTP port"},
   :max-ui-port
     {:order 7,
      :path [:options :max-ui-port],
      :type :int,
      :caption "Maximum Web UI HTTP port"},
   :sputnik-jvm-opts
     {:order 8,
      :path [:options :sputnik-jvm-opts],
      :type :string,
      :caption "JVM options"},
   :log-level
     {:order 9,
      :path [:options :log-level],
      :type :keyword,
      :caption "Logging level",
      :choice ["info", "trace", "debug", "warn", "error", "fatal"]}
   :scheduling-strategy
     {:order 10,
      :path [:options :scheduling-strategy],
      :type :keyword,
      :caption "Scheduling strategy"
      :choice ["equal-load" "fastest-max-tasks"]}
   :scheduling-timeout
     {:order 11,
      :path [:options :scheduling-timeout],
      :type :int,
      :caption "Scheduling timeout [ms]"} ,  
   :max-task-count-factor
     {:order 12,
      :path [:options :max-task-count-factor],
      :type :float,
      :caption "Maximum task count factor"},
   :worker-task-selection
     {:order 13,
      :path [:options :worker-task-selection],
      :type :string,
      :caption "Worker task selection"},
   :worker-ranking
     {:order 14,
      :path [:options :worker-ranking],
      :type :string,
      :caption "Worker ranking"},
   :task-stealing
     {:order 15,
      :path [:options :task-stealing],
      :type :bool,
      :caption "Task stealing",
      :choice [true false]},
   :task-stealing-factor
     {:order 16,
      :path [:options :task-stealing-factor],
      :type :float,
      :caption "Tasks stealing factor"}})


(defn create-server-table
  []
  (s/scrollable
    (s/table
      :id :server-table
      :model
      [:columns
        [{:key :server-name,        :text "Name"}
         {:key :server-node,        :text "Node"}
         {:key :sputnik-port,       :text "Port"}
         {:key :scheduling-strategy :text "Scheduling strategy"}
         {:key :task-stealing,      :text "Task stealing"}
         {:key :sputnik-jvm-opts,   :text "JVM options"}
         {:key :log-level,          :text "Log level"}]])))



(def ^:private worker-display-properties
  ^{:config-type :sputnik/worker} 
  {:worker-name         {:order 1, :path [:sputnik/config-id],            :type :symbol,
                         :caption "Worker ID"},
   :worker-node         {:order 2, :path [:sputnik/role-node],            :type :string,
                         :caption "Worker node",
                         :choice (partial select-node-configs :sputnik/node)},
   :numa-nodes          {:order 3, :path [:options :numa-nodes],          :type :int,
                         :caption "Number of NUMA nodes"},
   :worker-threads      {:order 4, :path [:options :worker-threads],      :type :int,
                         :caption "Worker threads (per NUMA node)"},
   :single-processes?   {:order 5,
                         :path [:options :single-processes?],
                         :type :bool,
                         :caption "Single Process per Thread",
                         :choice [false true]}
   :send-result-timeout {:order 6, :path [:options :send-result-timeout], :type :int,
                          :caption "Send results timeout (ms)"},
   :sputnik-jvm-opts    {:order 7, :path [:options :sputnik-jvm-opts],    :type :string,
                         :caption "JVM options"},
   :log-level           {:order 8, :path [:options :log-level],           :type :keyword,
                         :caption "Logging level",
                         :choice ["info", "trace", "debug", "warn", "error", "fatal"]}})


(defn create-worker-table
  []
  (s/scrollable
    (s/table
      :id :worker-table
      :model
      [:columns
        [{:key :worker-name,         :text "Name"}
         {:key :worker-node,         :text "Node"}
         {:key :numa-nodes,          :text "#NUMA"}
         {:key :worker-threads,      :text "#Threads"}
         {:key :single-processes?,   :text "Processes?"}
         {:key :send-result-timeout, :text "Send result timeout"}
         {:key :sputnik-jvm-opts,    :text "JVM options"}
         {:key :log-level,           :text "Log level"}]])))


(def ^:private payload-display-properties
  ^{:config-type :sputnik/payload-entity} 
  {:url {:order 2, :path [:url], :type :string, :caption "Payload URL"}
   :name {:order 1, :path [:sputnik/config-id], :type :string, :caption "Payload"}})


(defn create-payload-table
  []
  (s/scrollable
    (s/table :id :payload-table
      :model
      [:columns
        [{:key :name :text "Payload"}
         {:key :url, :text "Payload URL"}]])))




(defn convert
  "Converts a string to a value based on the given type."
  [text, type]
  (case type
    :string  (when-not (str/blank? text) text),
    :int     (Long/parseLong text),
    :float   (Double/parseDouble text)
    :symbol  (symbol text),
    :keyword (keyword text),
    :bool    (if (instance? Boolean text) text (Boolean/parseBoolean text))
    (e/illegal-argument "Unknown conversion target type \"%s\"!" type)))


(defn render
  "Renders config value to a string based on the given type."
  [value, type]
  (case type
    :keyword (some-> value name),
    :symbol  (str value),
    :bool    (if value "true" "false")
    (str value)))



(defn config->display-map
  [properties-desc-map, config-map]
  (reduce-kv
    (fn [res, key, {:keys [path, type]}]
      (assoc res key (some-> (get-in config-map path) (render type))))
    (select-keys config-map [:sputnik/config-id])
    properties-desc-map))


(defn kw->id
  [kw]
  (->> kw name (str "#") keyword))


(defn relative-location
  [frame, parent]
  (.setLocationRelativeTo ^javax.swing.JFrame frame, parent)
  frame)


(defn show-error-dialog
  [parent, message-fmt & args]
  (-> (s/dialog :type :error, :content (s/label (apply format message-fmt args)), :parent parent,
        :title "Error" :on-close :dispose, :modal? true)
    s/pack!
    (relative-location parent)
    s/show!)
  nil)


(defn show-info-dialog
  [parent, message-fmt & args]
  (-> (s/dialog :type :info, :content (s/label (apply format message-fmt args)), :parent parent,
        :title "Information" :on-close :dispose, :modal? true)
    s/pack!
    (relative-location parent)
    s/show!)
  nil)


(defn confirm-dialog
  [^java.awt.Component parent, title, message]
  (let [result (JOptionPane/showConfirmDialog parent, message, title, JOptionPane/YES_NO_OPTION, JOptionPane/QUESTION_MESSAGE)]
    (= result JOptionPane/YES_OPTION)))


(defn- create-id-symbol
  [text, config-type]
  (meta/with-config-type config-type
    (-> text (str/replace #"\s+" "-") symbol)))


(defn update-config-data
  [properties-desc-map, dialog, data-map, config-data]
  (let [old-config-id (:sputnik/config-id config-data),
        config-type (some-> properties-desc-map meta :config-type),
        config-data (reduce-kv
                      (fn [config-data, key, {:keys [path, type, additional-paths, choice]}]
                        (let [value (some-> dialog (s/select [(kw->id key)]) (s/config :text)),
                              value (when-not (str/blank? value) (convert value, type)),
                              config-data (assoc-in config-data path value)]
                          (if (empty? additional-paths)
                            config-data                            
                            (reduce #(assoc-in %1 %2 value) config-data additional-paths))))
                      (or config-data (meta/with-config-type config-type {}))
                    properties-desc-map),
       new-id-symbol (some-> config-data :sputnik/config-id (create-id-symbol config-type))
       check-overwrite? (fn [data-map, new-id-symbol]
                          (and (contains? data-map new-id-symbol)
                            (not= (meta/config-type new-id-symbol) (meta/config-type (data-map new-id-symbol)))))]
    (if (nil? new-id-symbol)
      (show-error-dialog dialog, "At least an ID or name is needed!")
      (if (check-overwrite? data-map, new-id-symbol)
        (loop [i 2]
          (let [new-id-symbol (create-id-symbol (format "%s-%s" (name new-id-symbol) i), config-type)]
            (if (check-overwrite? data-map, new-id-symbol)
              (recur (inc i))
              (assoc config-data :sputnik/config-id new-id-symbol))))  
        (assoc config-data :sputnik/config-id new-id-symbol)))))


(defn create-config-data-display
  [properties-desc-map, dialog, data-map, config-data]
  (let [config-data (when config-data (config->display-map properties-desc-map, config-data)),
        content-panel (s/select dialog [:#content-panel])]
    (s/config! content-panel :items
      (reduce
        (fn [items, [kw, {:keys [caption, choice, type]}]]
          (conj items [caption]
            [(if choice
               (let [options (cond
                               (fn? choice) (choice data-map)
                               (sequential? choice) choice
                               :else (e/illegal-argument "The :choice option in the display property map must be either a function or a list of values."))]
                 (cond-> (s/combobox :id kw, :model (mapv #(render %, type) options))
                   config-data (s/selection! (str (config-data kw)))))
               (s/text :id kw, :text (if config-data (render (config-data kw), type) "")))]))
        []
        (sort-by (comp :order val) properties-desc-map)))))


(defn default-config-map
  [config-type]
  (meta/with-config-type config-type
    (case config-type
      :sputnik/node {:host {:group "default",
                            :ssh-port 22,
                            :user {:no-sudo true}}}
      :sputnik/server {:sputnik/config-id "server",
                       :options
                       {:sputnik-port 23029,
                        :min-ui-port 8080,
                        :max-ui-port 18080,
                        :scheduling-timeout 100,
                        :scheduling-strategy :equal-load,
                        :max-task-count-factor 2,
                        :worker-task-selection 'sputnik.satellite.server.scheduling/any-task-count-selection
                        :worker-ranking 'sputnik.satellite.server.scheduling/faster-worker-ranking
                        :task-stealing true
                        :task-stealing-factor 2}},
      :sputnik/worker {:sputnik/config-id "worker",
                       :options
                       {:send-result-timeout 100,
                        :numa-nodes 1,}})))


(defn deep-merge
  [& maps]
  (apply merge-with
    (fn [x y]
      (if (and (map? x) (map? y))
        (deep-merge x y)
        y))
    maps))


(defn config-dialog
  [properties-desc-map, parent, mode, data-map, config-data]
  (let [config-type (some-> properties-desc-map meta :config-type),    
        config-data (deep-merge (default-config-map config-type) config-data)
        dialog (s/custom-dialog
                 :title (str (if (= mode :add) "Add" "Edit") " "
                          (case config-type
                            :sputnik/node "remote node"
                            :sputnik/server "server"
                            :sputnik/worker "worker")),
                 :modal? true,
                 :parent parent,
                 :on-close :dispose
                 :content (s/border-panel
                            :south (s/grid-panel :rows 1
                                     :items [(s/button :id :apply-button :text (if (= mode :add) "Add" "Apply"))
                                             (s/button :id :cancel-button :text "Cancel")])
                            :center (mig/mig-panel :id :content-panel
                                      :constraints ["wrap 2"
                                                    "[shrink 0]20px[270, grow, fill]"
                                                    "[shrink 0]5px[]"])))]
    (create-config-data-display properties-desc-map, dialog, data-map, config-data)
    (s/show!
      (doto dialog
        (-> (s/select [:#apply-button])  (s/listen :action (fn [_] 
                                                             (when-let [new-config-data
                                                                        (try
                                                                          (update-config-data properties-desc-map, dialog, data-map, config-data)
                                                                          (catch Throwable t
                                                                            (show-error-dialog parent, (str t))
                                                                            nil))]
                                                               (s/return-from-dialog dialog, new-config-data)))))
        (-> (s/select [:#cancel-button]) (s/listen :action (fn [_] (s/return-from-dialog dialog, nil))))
        s/pack!))))




(defn create-node-panel
  []
  (s/border-panel
    :north (s/label "Configuration of remote machines (nodes)")
    :south (s/grid-panel :rows 1 :items [(s/button :id :add-node :text "Add remote node") (s/button :id :edit-node :text "Edit remote node") (s/button :id :remove-node :text "Remove remote node")])
    :center (create-node-table)))


(defn choose-file
  [frame, mode]
  (ch/choose-file frame, :type mode, :selection-mode :files-only, :remember-directory? true))


(defn process-config
  [config-map]
  (let [comm-configs (cfg/select-configs :sputnik/communication config-map)]
    (reduce
      (fn [config-map, additional-comm-config]
        (dissoc config-map (:sputnik/config-id additional-comm-config)))
      config-map
      (rest comm-configs))))


(defn move-attributes-to-options
  [config-map]
  (let [attributes [:sputnik-jvm, :sputnik-jvm-opts, :sputnik-numactl, :numa-nodes]]
    (persistent!
      (reduce-kv
        (fn [config-map, k, attribute-map]
          (assoc! config-map k (reduce
                                 (fn [attribute-map, attr]
                                   (cond-> attribute-map
                                     (and
                                       (contains? attribute-map attr)
                                       (not (contains? (:options attribute-map) attr)))
                                     (->
                                       (dissoc attr)
                                       (assoc-in [:options, attr] (get attribute-map attr)))))
                                 attribute-map
                                 attributes)))
        (transient {})
        config-map))))


(defn load-config
  [frame, data-map-atom]
  (when-let [file (choose-file frame, :open)]
    (let [config (move-attributes-to-options (process-config (cfg/load-config file, :add-config-id true)))]
      (reset! data-map-atom config))))


(defn save-config
  [frame, data-map-atom]
  (when-let [file (choose-file frame, :save)]
    (cfg/save-config file, @data-map-atom)))


(defn remove-table-entry
  [frame, data-map-atom, table-id]
  (let [table (s/select frame [table-id]),
        selected-node-positions (s/selection table {:multi? true})]
    (swap! data-map-atom
      #(reduce
         (fn [data-map, node-pos]
           (let [node-id (:sputnik/config-id (tbl/value-at table, node-pos))]
             (dissoc data-map node-id)))
         %
         selected-node-positions))))


(defn edit-table-entry
  [frame, data-map-atom, table-id, display-properties]
  (let [table (s/select frame [table-id]),
        selected-config-id (some->> (s/selection table) (tbl/value-at table) :sputnik/config-id)]
    (when selected-config-id
      (swap! data-map-atom
        (fn [data-map]
          (let [config-data (data-map selected-config-id),
                new-config-data (config-dialog display-properties, frame, :edit, data-map, config-data),
                new-config-id (:sputnik/config-id new-config-data)]
            (if (or (nil? new-config-data) (= config-data new-config-data))
              data-map
              (-> data-map
                (cond->
                  (not= selected-config-id new-config-id) (dissoc selected-config-id))
                (assoc new-config-id new-config-data)))))))))


(defn add-table-entry
  [frame, data-map-atom, display-properties]
  (swap! data-map-atom
    (fn [data-map]
      (if-let [new-config-data (config-dialog display-properties, frame, :add, data-map, nil)]
        (assoc data-map (:sputnik/config-id new-config-data) new-config-data)
        data-map))))




(defn create-server-panel
  []
  (s/border-panel
    :north (s/label "Configuration of server nodes")
    :south (s/grid-panel :rows 1 :items [(s/button :id :add-server :text "Add server") (s/button :id :edit-server :text "Edit server") (s/button :id :remove-server :text "Remove server")])
    :center (create-server-table)))


(defn create-worker-panel
  []
  (s/border-panel
    :north (s/label "Configuration of worker nodes")
    :south (s/grid-panel :rows 1 :items [(s/button :id :add-worker :text "Add worker") (s/button :id :edit-worker :text "Edit worker") (s/button :id :remove-worker :text "Remove worker")])
    :center (create-worker-table)))


(defn- choose-keystore-file
  [panel, text-id]
  (let [text (s/select panel [text-id])]
    (when-let [chosen (ch/choose-file panel, :type :open, :selection-mode :files-only, :remember-directory? true,
                        :success-fn (fn [fc file] (.getAbsolutePath ^java.io.File file)))]
      (s/config! text :text chosen))
    nil))


(defn create-keystore-panel
  ([]
    (create-keystore-panel nil))
  ([title]
    (let [panel (mig/mig-panel :border title, :id :ssl-panel
                  :constraints ["wrap 2"
                                "[shrink 0]20px[100, grow, fill]"
                                "[shrink 0]5px[]"]
                  :items [["Key Alias"] [(s/text :id :key-alias)]
                          ["Keystore filename"]   [(s/border-panel
                                                     :center (s/text :id :keystore)
                                                     :east (s/button :id :choose-keystore, :text "..."))],
                          ["Keystore password"]   [(s/text :id :keystore-password)],
                          ["Truststore filename"] [(s/border-panel
                                                     :center (s/text :id :truststore)
                                                     :east (s/button :id :choose-truststore, :text "..."))],
                          ["Truststore password"] [(s/text :id :truststore-password)]])]
      (doto panel
        (-> (s/select [:#choose-keystore])   (s/listen :action (fn [_] (choose-keystore-file panel, :#keystore))))
        (-> (s/select [:#choose-truststore]) (s/listen :action (fn [_] (choose-keystore-file panel, :#truststore)))))      
      panel)))


(defn create-communication-panel
  []
  (s/border-panel
    :north (s/label "Configuration of the communication between the nodes")
    :south (s/grid-panel :rows 1
             :items [(s/button :text "Apply" :id :apply-communication) (s/button :text "Reset" :id :reset-communication)])
    :center (s/grid-panel :rows 1 
              :items [(s/border-panel
                        :north (s/scrollable
                                 (mig/mig-panel :id :general-communication-panel :border "General configuration"
                                   :constraints ["wrap 2"
                                                 "[shrink 0]20px[100, grow, fill]"
                                                 "[shrink 0]5px[]"]
                                   :items [["Number of communication threads"] [(s/text :id :comm-thread-count)],
                                           ["Use compression?"]  [(s/checkbox :id :compressed)]
                                           ["Initial Buffer Size (Bytes)"] [(s/text :id :initial-buffer)]
                                           ["Maximal Buffer Size (Bytes)"] [(s/text :id :max-buffer)]])
                                 :border 0)
                        :center (s/border-panel
                                  :north (s/checkbox :id :ssl-enabled, :text "enable SSL?", :selected? true)
                                  :center (s/scrollable
                                            (create-keystore-panel "SSL configuration"),
                                            :border 0)))
                      (s/border-panel :border "Keystore and Truststore Generation", :id :keystore-panel
                        :north (s/button :id :create-cluster-stores :text "Create keystore and truststore for worker/server.")
                        :center (s/border-panel :border "Server Truststore Entries"
                                  :center (s/scrollable
                                            (s/listbox :id :server-truststore-entries)
                                            :border 0))
                        :south (s/button :id :create-client-stores :text "Create keystore and truststore for the client."))])))


(def ^:private communication-properties
  {:ssl-enabled :bool,
   :key-alias :string,
   :keystore :string,
   :truststore :string,
   :keystore-password :string,
   :truststore-password :string,
   :comm-thread-count :int,
   :compressed :bool,
   :initial-buffer :int,
   :max-buffer :int})


(defn refresh-truststore-entries
  [frame, {:keys [truststore, truststore-password] :as comm-config}]
  (when (fs/exists? truststore)
    (let [trusted-aliases (ks/truststore-entries truststore, truststore-password)
          trustore-lb (s/select frame [:#server-truststore-entries])]
      (s/config! trustore-lb :model (vec trusted-aliases)))))


(defn refresh-communication-config
  [frame, data-map]
  (when-let [comm-config (first (cfg/select-configs :sputnik/communication, data-map))]
    (doseq [[config-kw, type] communication-properties]
      (let [id (kw->id config-kw)]
        (-> frame (s/select [id]) (s/config! (if (= type :bool) :selected? :text) (render (comm-config config-kw), type)))))
    (refresh-truststore-entries frame, comm-config)))


(defn extract-communication-config
  [frame]
  (reduce-kv
    (fn [result-map, config-kw, type]
      (let [id (kw->id config-kw),
            new-value (some-> frame (s/select [id]) (s/config (if (= type :bool) :selected? :text)))
            new-value (cond-> new-value (and (not= type :bool) (not (str/blank? new-value))) (convert type))]
        (assoc result-map, config-kw new-value)))
    {}
    communication-properties))


(defn- update-communication-config
  [frame, data-map-atom]
  (try
    (swap! data-map-atom
      (fn [data-map]
        (if-let [comm-config (first (cfg/select-configs :sputnik/communication, data-map))]
          (->> (merge comm-config (extract-communication-config frame))          
            (assoc data-map (:sputnik/config-id comm-config)))
          data-map)))
    (catch Throwable t
      (show-error-dialog frame, (str t)))))


(defn comm-config-changes?
  [frame, comm-config]
  (not=
    (select-keys comm-config (keys communication-properties))
    (extract-communication-config frame)))


(defn existing-files
  [& filenames]
  (filterv fs/exists? filenames))


(defn confirm-overwrite
  [frame, files]
  (or (empty? files)
    (confirm-dialog frame, "Overwrite?",
      (format "Overwrite the following existing files?\n  %s" (str/join "\n  " files)))))


(defn create-directories-if-needed
  [& filenames]
  (doseq [dir (->> filenames (map fs/filepath) distinct)]
    (fs/create-directory dir)))


(defn create-cluster-keystores
  [frame, data-map-atom]
  (let [comm-config (first (cfg/select-configs :sputnik/communication, @data-map-atom))] ;(inspect comm-config) (inspect (extract-communication-config frame))
    (if (comm-config-changes? frame, comm-config)
      (show-info-dialog frame, "The communication configuration has temporary changes. These need to be applied first!")
      ; check whether files exist already, continue only on confirmation dialog
      (let [{:keys [keystore, truststore, keystore-password, truststore-password, key-alias]} comm-config]
        (when (confirm-overwrite frame, (existing-files keystore, truststore))
          (create-directories-if-needed keystore, truststore)
          (ks/create-ssl-stores keystore, keystore-password, truststore, truststore-password, key-alias)
          true)))))



(defn keyword->caption
  [kw]
  (-> kw name (str/replace "-" " ")))


(defn checked-return
  [parent, e, panel]
  (let [config-map (extract-communication-config panel)]
    (if-let [invalid-fields (seq 
                              (filter
                                (comp str/blank? config-map)
                                [:keystore, :truststore, :keystore-password, :truststore-password]))]
      (show-error-dialog parent, "<html>Value for the following fields needed!<p>&nbsp;&nbsp;%s</html>"
        (str/join "<br>&nbsp;&nbsp;" (map keyword->caption invalid-fields)))
      (s/return-from-dialog e, config-map))))


(defn set-text
  [panel, & id-text-pairs]
  (doseq [[id text] (partition-all 2 id-text-pairs)]
    (some-> panel (s/select [id]) (s/config! :text text)))
  panel)


(defn query-client-keystore-config
  [parent]
  (let [panel (set-text (create-keystore-panel)
                :#keystore "client-keystore.ks"
                :#truststore "client-truststore.ks"
                :#key-alias (str "client " (rand-int java.lang.Integer/MAX_VALUE)))]
    (-> (s/custom-dialog :parent parent, :title "Client SSL keystore and truststore configuration" :on-close :dispose, :modal? true,
          :content (s/border-panel
                     :preferred-size [450 :by 250]
                     :center panel
                     :south (s/grid-panel :rows 1
                              :items [(s/button :text "Create"
                                        :listen [:action (fn [e] (s/return-from-dialog e, (extract-communication-config panel)))])
                                      (s/button :text "Cancel"
                                        :listen [:action (fn [e] (s/return-from-dialog e, nil))])])))
      s/pack!
      (relative-location parent)
      s/show!)))


(defn create-client-keystores
  [frame, data-map-atom]
  (let [comm-config (first (cfg/select-configs :sputnik/communication, @data-map-atom))] ;(inspect comm-config) (inspect (extract-communication-config frame))
    (if (comm-config-changes? frame, comm-config)
      (show-info-dialog frame, "The communication configuration has temporary changes. These need to be applied first!")
      ; check whether files exist already, continue only on confirmation dialog
      (let [{:keys [keystore, keystore-password, truststore, truststore-password, key-alias]} comm-config,]
        (when-let [config-map (query-client-keystore-config frame)]
          (let [{client-keystore :keystore, client-keystore-password :keystore-password,
                 client-truststore :truststore, client-truststore-password :truststore-password,
                 client-key-alias :key-alias} config-map]
            (when (confirm-overwrite frame, (existing-files client-keystore, client-truststore))
              (create-directories-if-needed client-keystore, client-truststore)
              (ks/create-client-ssl-stores
                client-keystore, client-keystore-password, client-truststore, client-truststore-password, client-key-alias,
                keystore, keystore-password, truststore, truststore-password, key-alias)
              true)))))))



(defn create-launch-panel
  []
  (s/border-panel
    :north (s/label "Configure payload and launch nodes (server and worker)")    
    :center (s/top-bottom-split
              (s/left-right-split
                (s/border-panel
                  :center (s/border-panel :border "1. Select Payload"
                            :south (s/grid-panel :rows 1
                                     :items [(s/button :id :add-payload,     :text "Add payload")
                                             (s/button :id :add-sputnik-jar, :text "Add Sputnik jar")
                                             (s/button :id :remove-payload,  :text "Remove payload")])
                            :center (create-payload-table))
                  :south (s/grid-panel :rows 1 :border "2. Remote directory"
                           :items [(s/label "Remote payload directory")
                                   (s/text :id :payload-directory
                                     :text (format "sputnik-payload-%s" (fmt/datetime-filename-format (System/currentTimeMillis))))]))
                (s/border-panel
                  :north (s/grid-panel
                           :items [(s/grid-panel :rows 1 :border "3. Launch server"
                                     :items [(s/combobox :id :server-selection) (s/button :id :launch-server, :text "Launch server")])] )
                  :center (s/border-panel :border "4. Launch workers"
                            :center (s/scrollable (s/box-panel :vertical, :id :worker-selection, :background :white), :border 0)
                            :south (s/button :id :launch-workers, :text "Launch workers"))
                  :south (s/grid-panel :rows 1 :border "5. Create client config for selected server"
                           :items [(s/button :id :create-client-config, :text "Create client config")]))
                :divider-location 1/2,
                :resize-weight 1/2),
              (s/border-panel
                :north (s/label "Process output:")                
                :center (s/scrollable (s/text :id :console-output :multi-line? true :editable? false :background :black :foreground :white))),
              :divider-location 8/10
              :resize-weight 8/10)))


(def ^:private chararray (Class/forName "[C"))

(defn char-array?
  [x]
  (instance? chararray x))

(defn create-console-output-stream
  [frame]
  (let [^javax.swing.JTextArea console-text (s/select frame [:#console-output])
        append-fn (fn append [^String s] (.append console-text s))]
    (proxy [java.io.Writer] []
      
      (write
        ([x]
          (cond
            (string? x) (.write ^java.io.Writer this, (.toCharArray ^String x), 0, (.length ^String x))
            (char-array? x) (.write ^java.io.Writer this, ^chars x, 0, (alength ^chars x))))
        ([^chars char-buffer, ^Integer offset, ^Integer length]
          (append-fn (String/valueOf char-buffer, offset, length))))
      
      (flush [] #_Nothing)
      (close [] #_Nothing))))


(defmacro with-console-out
  [frame, & body]
  `(binding [*out* (create-console-output-stream ~frame)]
     ~@body))


(defn create-payload
  [^java.io.File file]
  (assoc (lang/payload-entity (.getAbsolutePath file)) :sputnik/config-id (create-id-symbol (.getName file), :sputnik/payload-entity)))


(defn add-payload
  [frame, data-map-atom]
  (when-let [files (seq (ch/choose-file frame, :type :open, :selection-mode :files-and-dirs, :remember-directory? true, :multi? true))]
    (let [id-payload-pairs (mapv #(let [payload (create-payload %)] [(:sputnik/config-id payload) payload]) files)]
      (swap! data-map-atom into id-payload-pairs))))


(defn java-classpath
  []
  (str/split (System/getProperty "java.class.path") #":"))


(defn add-sputnik-jar
  [frame, data-map-atom] 
  (if-let [sputnik-jar (or
                         ; when the GUI is running and there is only one classpath entry it must contain sputnik
                         (let [cp (java-classpath)]
                           (when (= 1 (count cp))
                             (io/file (first cp))))
                         ; search for sputnik jar
                         (first
                          (filter
                            (fn [^java.io.File file]
                              (re-find #"sputnik-(\d\.){2}\d(-SNAPSHOT)?\.jar" (.getName file)))
                            (cp/classpath))))]
    (let [payload (create-payload sputnik-jar)]
      (swap! data-map-atom assoc (:sputnik/config-id payload) payload))
    (show-error-dialog frame, "<html>Sputnik jar was not found on the classpath!<br><br>Do you really run Sputnik as standalone application?</html>")))


(defn refresh-server-selection
  [frame, data-map]
  (let [server-ids (select-node-configs :sputnik/server data-map),
        server-selection (s/select frame [:#server-selection])]
    (s/config! server-selection :model (mapv #(render %, type) server-ids))))


(defn refresh-worker-selection
  [frame, data-map]
  (let [worker-ids (select-node-configs :sputnik/worker data-map),
        worker-selection (s/select frame [:#worker-selection])]
    (s/config! worker-selection
      :items
      (mapv
        (fn [wid]
          (let [worker-node-id (some-> wid data-map :sputnik/role-node),
                worker-node (some-> worker-node-id symbol data-map),
                enabled? (boolean (and worker-node-id worker-node))]
            (s/checkbox :opaque? false,
              :id (render wid, :symbol),
              :enabled? enabled?,
              :text (format
                      (if enabled? "%s (%s)" "<html><i>%s&nbsp;&nbsp;&nbsp;(%s)</i></html>"),
                      wid,
                      (cond
                        worker-node worker-node-id,
                        worker-node-id (format "assigned node \"%s\" does not exist!" worker-node-id),
                        :else "no node assigned!")))))
        worker-ids))))


(defn refresh-table
  "Refreshes the node table."
  [frame, data-map, table-id, config-type, sort-key, display-properties]
  (let [table (s/select frame [table-id])]
    (tbl/clear! table)
    (some->> data-map
      (cfg/select-configs config-type)
      seq
      (map (partial config->display-map display-properties))
      (sort-by sort-key)
      (interleave (repeat 0))
      (apply tbl/insert-at! table))))



(defn refresh
  "Refreshes the whole window."
  [frame, data-map-atom]
  (let [data-map @data-map-atom]
    (refresh-table frame, data-map, :#node-table,   :sputnik/node,   :host-name, node-display-properties)
    (refresh-table frame, data-map, :#server-table, :sputnik/server, :server-name, server-display-properties)
    (refresh-table frame, data-map, :#worker-table, :sputnik/worker, :worker-name, worker-display-properties)
    (refresh-communication-config frame, data-map)
    (refresh-table frame, data-map, :#payload-table, :sputnik/payload-entity, :url, payload-display-properties)
    (refresh-server-selection frame, data-map)
    (refresh-worker-selection frame, data-map)))



(defn enable-children
  [frame, comp-id, enabled?]
  (let [panel (s/select frame [comp-id])]
    (doseq [c (s/select panel [:*]) :when (instance? java.awt.Component c)]
      (.setEnabled ^java.awt.Component c, enabled?))))


(defn server-node
  [parent, data-map, server-role-id]
  (let [server-role (data-map server-role-id),
        server-node-id (some-> server-role :sputnik/role-node symbol),
        server-node (data-map server-node-id)]
    (cond 
      (nil? server-node-id) (show-error-dialog parent, "No remote node found for server configuration \"%s\"!" (str server-role-id))
      (nil? server-node) (show-error-dialog parent, "Remote node \"%s\" for server configuration \"%s\" does not exist!" (str server-node-id) (str server-role-id))
      :success (lang/apply-role server-role, server-node))))


(defn selected-server
  [frame, data-map]
  (server-node frame, data-map, (some-> frame (s/select [:#server-selection]) (s/config :text) symbol)))


(defn get-payload
  [frame, data-map]
  (let [payload-list (cfg/select-configs :sputnik/payload-entity data-map)]
    (if (empty? payload-list)
      (show-error-dialog frame, "No payload found! At least the Sputnik jar needs to be added as payload!")
      payload-list)))


(defn get-communication
  [frame, data-map]
  (let [comm (first (cfg/select-configs :sputnik/communication data-map))]
    (if comm
      comm
      (show-error-dialog frame, "No communication configuration found!"))))


(defn get-payload-directory
  [frame]
  (let [dir (-> frame (s/select [:#payload-directory]) (s/config :text))]
    (if (str/blank? dir)
      (show-error-dialog frame, "You must specify a remote directory where all files will be copied!")
      dir)))


(defn remove-sputnik-keys
  "Remove keys with namespace :sputnik/."
  [m]
  (when m
    (reduce-kv
      (fn [m, k, _]
        (if (and (keyword? k) (or (= (namespace k) "sputnik") (= k :key-alias)))
          (dissoc m k)
          m))    
      m
      m)))


(defn launch-server
  [frame, data-map-atom]
  (let [data-map @data-map-atom,
        server (remove-sputnik-keys (selected-server frame, data-map)),
        communication (remove-sputnik-keys (get-communication frame, data-map)),
        payload-directory (get-payload-directory frame),
        payload-list (mapv remove-sputnik-keys (get-payload frame, data-map))
        launch-server-button (s/select frame [:#launch-server])]
    (when (and server communication payload-directory payload-list)
      (s/config! launch-server-button :enabled? false)
      (future
        (try 
          (with-console-out frame
            (launch/launch-nodes [server], communication, payload-list, (str payload-directory "-server"), :verbose true, :raise-on-error true))
          (show-info-dialog frame, "Server start finished.")
          (catch Throwable t
            (show-error-dialog frame, "<html>The following error occured while trying to launch the specified server:<br><br>%s</html>" (.getMessage t)))
          (finally
            (s/config! launch-server-button :enabled? true)))))))


(defn no-passwords-intentionally?
  [parent, empty-pw-fields]
  (-> (s/dialog
        :content (format "Did you leave the following passowrd fields blank intentionally? %s"
                   (str/join ", " (map keyword->caption empty-pw-fields)))
        :option-type :yes-no)
     s/pack!
    (relative-location parent)
    s/show!
    (= :success)))


(defn get-data-from-panel
  [panel, id-parse-fn-map]
  (reduce-kv
    (fn [m, id, parse-fn]
      (let [[kw id] (if (sequential? id) id [id (kw->id id)])]
        (assoc m kw (some-> panel (s/select [id]) (s/config :text) parse-fn))))
    {}
    id-parse-fn-map))

(defn- client-config-return
  [e, panel]
  (let [config-map (extract-communication-config panel),
        additional-values (get-data-from-panel panel,
                            {[:server-role-id :#server-selection] symbol}),
        config-map (merge config-map additional-values),
        filter-blank (fn [ks] (seq (filter #(str/blank? (config-map %)) ks))),
        empty-store-fields (filter-blank [:keystore, :truststore]),
        empty-pw-fields (filter-blank [:keystore-password, :truststore-password])]
    (if (or empty-store-fields empty-pw-fields)
      (if empty-store-fields
        (show-error-dialog panel, "You must specify the files in the following fields: %s"
          (str/join ", " (map keyword->caption empty-store-fields)))
        (when (no-passwords-intentionally? panel, empty-pw-fields)
          (s/return-from-dialog e, config-map)))      
      (s/return-from-dialog e, config-map))))


(defn query-client-config
  [parent, server-roles, nodes]
  (let [panel (mig/mig-panel
                  :constraints ["wrap 2"
                                "[shrink 0]20px[100, grow, fill]"
                                "[shrink 0]5px[]"]
                  :items [["Server node"]         [(s/combobox :id :server-selection, :model (mapv #(render %, type) server-roles))]
                          ["Keystore filename"]   [(s/border-panel
                                                     :center (s/text :id :keystore)
                                                     :east (s/button :id :choose-keystore, :text "..."))],
                          ["Keystore password"]   [(s/text :id :keystore-password)],
                          ["Truststore filename"] [(s/border-panel
                                                     :center (s/text :id :truststore)
                                                     :east (s/button :id :choose-truststore, :text "..."))],
                          ["Truststore password"] [(s/text :id :truststore-password)]])]
    (doto panel
      (-> (s/select [:#choose-keystore])   (s/listen :action (fn [_] (choose-keystore-file panel, :#keystore))))
      (-> (s/select [:#choose-truststore]) (s/listen :action (fn [_] (choose-keystore-file panel, :#truststore)))))
      
    (-> (s/custom-dialog :parent parent, :title "Client configuration" :on-close :dispose, :modal? true,
          :content (s/border-panel
                     :preferred-size [450 :by 200]
                     :center panel
                     :south (s/grid-panel :rows 1
                              :items [(s/button :text "Save as ..."
                                        :listen [:action (fn [e] (client-config-return e, panel))])
                                      (s/button :text "Cancel"
                                        :listen [:action (fn [e] (s/return-from-dialog e, nil))])])))
      s/pack!
      (relative-location parent)
      s/show!)))


(defn create-client-config
  [frame, data-map-atom]
  (let [data-map @data-map-atom,
        communication (remove-sputnik-keys (get-communication frame, data-map))]
    (when-let [client-setup (query-client-config frame,
                              (select-node-configs :sputnik/server data-map)
                              (select-node-configs :sputnik/node data-map))]
      (when-let [file (choose-file frame, :save)]
        (let [client-communication (merge communication
                                     (select-keys client-setup [:keystore, :keystore-password, :truststore, :truststore-password])),
              {:keys [client-node-id, server-role-id]} client-setup,
              server (remove-sputnik-keys (server-node frame, data-map, server-role-id))
              client (remove-sputnik-keys (data-map client-node-id))
              client-config (->> client (cfg/use-server server) (cfg/use-communication client-communication) cfg/node-config)]
          (launch/create-config-file client-config, file))))))


(defn selected-workers
  [frame, data-map]
  (let [worker-ids (->> (s/select frame [:#worker-selection :JCheckBox])
                     (filter #(s/config % :selected?))
                     (map #(symbol (name (s/config % :id)))))]
    (loop [worker-ids worker-ids, worker-without-role {}, ready-workers []]
      (if (seq worker-ids)
        (let [wid (first worker-ids),
              worker (data-map wid),
              worker-node-id (:sputnik/role-node worker), 
              worker-node (when worker-node-id (data-map (symbol worker-node-id)))]          
          (recur (rest worker-ids),
            (cond
              worker-node
                worker-without-role,
              worker-node-id
                (assoc worker-without-role wid worker-node-id),
              :else (assoc worker-without-role wid nil)),
            (cond-> ready-workers
              worker-node
              (conj (lang/apply-role worker, worker-node)))))
        (if (seq worker-without-role)
          (show-error-dialog frame, "<html>There are configuration problems with the following workers:<br><br><ul><li>%s</li></ul></html>"
            (str/join "</li><li>"
              (map
                (fn [[worker role-node]]
                  (if role-node
                    (format "Worker \"%s\" has node \"%s\" associated which does not exist" worker role-node)
                    (format "Worker \"%s\" has no node associated" worker)))
                worker-without-role)))
          ready-workers)))))


(defn launch-workers
  [frame, data-map-atom]
  (let [data-map @data-map-atom,
        worker-list (mapv remove-sputnik-keys (selected-workers frame, data-map)),
        communication (remove-sputnik-keys (get-communication frame, data-map)),
        payload-directory (get-payload-directory frame),
        payload-list (mapv remove-sputnik-keys (get-payload frame, data-map))
        server (remove-sputnik-keys (selected-server frame, data-map)),        
        worker-list (when server (mapv (partial cfg/use-server server) worker-list)),
        launch-workers-button (s/select frame [:#launch-workers])]
    (when (and (seq worker-list), server, communication, payload-directory, payload-list)
      (s/config! launch-workers-button :enabled? false)
      (future
        (try
          (with-console-out frame
            (launch/launch-nodes worker-list, communication, payload-list, (str payload-directory "-worker"), :verbose true, :raise-on-error true))
          (show-info-dialog frame, "Worker start finished!")
          (catch Throwable t
            (show-error-dialog frame, "<html>The following error occured while trying to launch the specified workers:<br><br>%s</html>" (.getMessage t)))
          (finally
            (s/config! launch-workers-button :enabled? true)))))))



(defn- inspect-config
  [frame, data-map-atom]
  (try
    (require 'debug.inspect)
    (let [inspect (resolve 'debug.inspect/inspect)]
      (inspect @data-map-atom))    
    (catch java.io.FileNotFoundException t
      (show-error-dialog frame, "Namespace debug.inspect was not found on the classpath!"))))


(defn add-behavior
  [frame, data-map-atom]
  (swap! data-map-atom
    (fn [data-map]
      (if-let [comm (first (cfg/select-configs :sputnik/communication data-map))]
        data-map
        ; add default communication object
        (let [default-comm (assoc (lang/communication)
                             :sputnik/config-id (create-id-symbol "sputnik-comm", :sputnik/communication),
                             :key-alias "sputnik nodes")]
          (assoc data-map (:sputnik/config-id default-comm) default-comm)))))
  (let [refresh #(refresh frame, data-map-atom)]
    (doto frame
      (-> (s/select [:#load-config]) (s/listen :action (fn [_] (load-config frame, data-map-atom) (refresh))))
      (-> (s/select [:#save-config]) (s/listen :action (fn [_] (save-config frame, data-map-atom))))
      (-> (s/select [:#remove-node]) (s/listen :action (fn [_] (remove-table-entry frame, data-map-atom, :#node-table) (refresh))))
      (-> (s/select [:#edit-node])   (s/listen :action (fn [_]
                                                         (edit-table-entry frame, data-map-atom, :#node-table, node-display-properties)
                                                         (refresh))))
      (-> (s/select [:#node-table])  (s/listen :mouse-clicked (fn [^java.awt.event.MouseEvent e]
                                                                (when (= 2 (.getClickCount e))
                                                                  (edit-table-entry frame, data-map-atom, :#node-table, node-display-properties)
                                                                  (refresh)))))
      (-> (s/select [:#add-node])    (s/listen :action (fn [_]
                                                         (add-table-entry frame, data-map-atom, node-display-properties)
                                                         (refresh))))
      (-> (s/select [:#remove-server]) (s/listen :action (fn [_] (remove-table-entry frame, data-map-atom, :#server-table) (refresh))))
      (-> (s/select [:#edit-server])   (s/listen :action (fn [_]
                                                           (edit-table-entry frame, data-map-atom, :#server-table, server-display-properties)
                                                           (refresh))))
      (-> (s/select [:#server-table])  (s/listen :mouse-clicked (fn [^java.awt.event.MouseEvent e]
                                                                  (when (= 2 (.getClickCount e))
                                                                    (edit-table-entry frame, data-map-atom, :#server-table, server-display-properties)
                                                                    (refresh)))))
      (-> (s/select [:#add-server])    (s/listen :action (fn [_]
                                                           (add-table-entry frame, data-map-atom, server-display-properties)
                                                           (refresh))))
      (-> (s/select [:#remove-worker]) (s/listen :action (fn [_] (remove-table-entry frame, data-map-atom, :#worker-table) (refresh))))
      (-> (s/select [:#edit-worker])   (s/listen :action (fn [_] 
                                                           (edit-table-entry frame, data-map-atom, :#worker-table, worker-display-properties)
                                                           (refresh))))
      (-> (s/select [:#worker-table])  (s/listen :mouse-clicked (fn [^java.awt.event.MouseEvent e]
                                                                  (when (= 2 (.getClickCount e))
                                                                    (edit-table-entry frame, data-map-atom, :#worker-table, worker-display-properties)
                                                                    (refresh)))))
      (-> (s/select [:#add-worker])    (s/listen :action (fn [_]
                                                           (add-table-entry frame, data-map-atom, worker-display-properties)
                                                           (refresh))))
      (-> (s/select [:#ssl-enabled]) (s/listen :item-state-changed (fn [e]
                                                                     (doseq [id [:#ssl-panel, :#keystore-panel]]
                                                                       (enable-children frame, id, (s/config e :selected?))))))
      (-> (s/select [:#reset-communication]) (s/listen :action (fn [_] (refresh-communication-config frame, @data-map-atom))))
      (-> (s/select [:#apply-communication]) (s/listen :action (fn [_] (update-communication-config frame, data-map-atom))))
      (-> (s/select [:#remove-payload]) (s/listen :action (fn [_] (remove-table-entry frame, data-map-atom, :#payload-table) (refresh))))
      (-> (s/select [:#add-payload]) (s/listen :action (fn [_] (add-payload frame, data-map-atom) (refresh))))
      (-> (s/select [:#add-sputnik-jar]) (s/listen :action (fn [_] (add-sputnik-jar frame, data-map-atom) (refresh))))
      (-> (s/select [:#launch-server]) (s/listen :action (fn [_] (launch-server frame, data-map-atom))))
      (-> (s/select [:#launch-workers]) (s/listen :action (fn [_] (launch-workers frame, data-map-atom))))
      (-> (s/select [:#create-client-config]) (s/listen :action (fn [_] (create-client-config frame, data-map-atom))))
      (-> (s/select [:#create-cluster-stores]) (s/listen :action (fn [_] (when (create-cluster-keystores frame, data-map-atom) (refresh)))))
      (-> (s/select [:#create-client-stores]) (s/listen :action (fn [_] (when (create-client-keystores frame, data-map-atom) (refresh)))))
      (some-> (s/select [:#inspect-config]) (s/listen :action (fn [_] (inspect-config frame, data-map-atom)))))
    (refresh)
    frame))



(defn create-main-window
  ([data-map-atom]
    (create-main-window data-map-atom, :dispose))
  ([data-map-atom, mode]
    (let [debug? *debug*]
      (s/invoke-now
        (binding [*debug* debug?]
          (doto ^javax.swing.JFrame (s/frame
                                      :on-close mode
                                      :title (format "Sputnik Control (Sputnik %s)" (v/sputnik-version))
                                      :menubar (s/menubar
                                                 :items (cond-> [(s/menu :text "Configuration"
                                                                   :items [(s/menu-item :id :save-config :text "Save")
                                                                           (s/menu-item :id :load-config :text "Load")])]
                                                          *debug*
                                                          (conj (s/menu :text "Debug"
                                                                  :items [(s/menu-item :id :inspect-config :text "Inspect Config")]))))
                                      :content (s/tabbed-panel :placement :top
                                                 :tabs [{:title "Remote nodes" :content (create-node-panel)}
                                                        {:title "Server configurations", :content (create-server-panel)},
                                                        {:title "Worker configurations", :content (create-worker-panel)},
                                                        {:title "Communication configuration", :content (create-communication-panel)},
                                                        {:title "Launch", :content (create-launch-panel)}])
                                      :size [800 :by 600])
            (add-behavior data-map-atom)
            (.setLocationRelativeTo nil)
            s/show!))))))



(defn- set-look-and-feel
  []
  (try
    (when-let [^javax.swing.UIManager$LookAndFeelInfo nimbus-lf (some #(when (= "Nimbus" (.getName ^javax.swing.UIManager$LookAndFeelInfo %)) %) (javax.swing.UIManager/getInstalledLookAndFeels))]
      (javax.swing.UIManager/setLookAndFeel (.getClassName nimbus-lf)))
    (catch Exception e)))


(defn -main
  [& args]
  (set-look-and-feel)
  (create-main-window (atom nil), :exit))