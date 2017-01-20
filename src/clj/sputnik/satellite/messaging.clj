; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.messaging
  (:require
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [clojure.options :refer [defn+opts, ->option-map]]
    [frost.quick-freeze :as qf]
    [sputnik.satellite.ssl :as ssl]
    [sputnik.satellite.netty :as netty])
  (:import
    java.util.UUID
    (java.io Closeable InputStream)
    java.nio.ByteBuffer
    java.net.InetSocketAddress
    javax.net.ssl.SSLEngine
    javax.security.cert.X509Certificate
    io.netty.handler.ssl.SslHandler
    (io.netty.buffer ByteBuf Unpooled)
    (io.netty.channel
      Channel
      ChannelId
      ChannelHandler
      ChannelHandlerContext
      ChannelOption
      ChannelPipeline
      ConnectTimeoutException)
    io.netty.bootstrap.Bootstrap
    io.netty.handler.codec.ByteToMessageDecoder
    io.netty.util.concurrent.PromiseAggregator
    (io.netty.handler.timeout IdleStateHandler IdleStateEvent IdleState)
    sputnik.satellite.io.MessageBufferInputStream
    java.util.concurrent.TimeUnit))



(defonce ^:private ^:const long-size (quot Long/SIZE Byte/SIZE))

(defn long->bytebuf
  ^ByteBuf [^long x]
  (doto (Unpooled/buffer long-size)
    (.writeLong x)))


(defn message-byte-count
  ^long [^MessageBufferInputStream mbis]
  (let [buffer (ByteBuffer/allocate long-size)]
    (.setNextMessageLength mbis long-size)
    (.read mbis (.array buffer), 0, long-size)
    (.getLong buffer)))


(definterface IDecodeState
  (setCurrentMessageSize [^long size])
  (^long getCurrentMessageSize []))


(deftype DecodeState [^:volatile-mutable ^long current-message-size]
  IDecodeState
  (setCurrentMessageSize [this, size]
    (set! current-message-size size)
    this)
  (getCurrentMessageSize [this]
    current-message-size))



(defn+opts consume-stream
  [^java.util.List out, ^DecodeState decode-state, ^MessageBufferInputStream mbis | :as options]
  (loop [out out, decode-state decode-state, mbis mbis] #_(println "available:" (.available mbis))
    (let [current-message-size (.getCurrentMessageSize decode-state)] 
      ; if no message is handled right now, ...
      (if (== current-message-size -1)
        ; ... then check whether we have enough bytes to deserialize the number of bytes
        (when (>= (.available mbis) long-size)
          (let [message-size (message-byte-count mbis)]
            #_(println "receiving message with" message-size "bytes")
            (.setCurrentMessageSize decode-state message-size)
            ; limit reads to message size
            (.setNextMessageLength mbis message-size)
            ; attempt to read the message next
            (recur out, decode-state, mbis)))
        ; ... else check whether there are enough bytes to deserialize the message
        (when (>= (.available mbis) current-message-size)
          (let [message (qf/quick-stream-defrost mbis, options)]
            (.add out message)
            (.setCurrentMessageSize decode-state -1)
            ; attemp to read the next message beginning with its size
            (recur out, decode-state, mbis)))))))


(defn copy-bytes
  [^ByteBuf buf]
  (let [n (.readableBytes buf)]
    (when (pos? n)
      (let [copy (byte-array n)]
        (.readBytes buf copy)
        copy))))


(defn+opts ^ByteToMessageDecoder create-message-decoder
  [| :as options]
  (let [decode-state (DecodeState. -1),
        mbis (MessageBufferInputStream.)]
    (proxy [ByteToMessageDecoder] []
      (decode [ctx, in, out]
        (when-let [data (copy-bytes in)]
          ; add data to input stream
          (.add mbis data))
        (consume-stream out, decode-state, mbis, options)))))


(defn+opts ^ByteToMessageDecoder create-message-encoder
  [| :as options]
  (netty/channel-handler
    :write
    ([_, channel-context, message, promise]
      (let [data (qf/quick-byte-freeze message, options)
            n (alength data)
            promise-1 (.newPromise channel-context),
            promise-2 (.newPromise channel-context),
            promise-aggregator (doto (PromiseAggregator. promise)
                                 (.add (into-array [promise-1 promise-2])))]
        (.write channel-context, (long->bytebuf n), promise-1)
        (.write channel-context, (Unpooled/wrappedBuffer data), promise-2)))))


(defprotocol IMessenger
  (send-message [messenger, message]))


(defprotocol IEndpoint
  (id [endpoint])
  (info [endpoint])
  (address-str [endpoint])
  (connected? [endpoint])
  (ping [endpoint])
  (disconnect [endpoint]))


(defprotocol IClientEndpoint
  (connect [endpoint])
  (shutdown [endpoint]))


(defprotocol IServer
  (start [server])
  (running? [server])
  (lookup-endpoint [server, channel-id])
  (stop [server]))


(defn channel-id
  [channel-or-context]
  (let [^Channel
        channel (if (instance? ChannelHandlerContext channel-or-context)
                  (.channel ^ChannelHandlerContext channel-or-context)
                  channel-or-context)]
    (.. channel id asLongText)))


(defn reconnect-wait-duration
  ^long [^long attempt]
  (cond
    (<= attempt 4) 500
    (<= attempt 12) 1000
    (<= attempt 22) 2000
    :else 5000))


(defn send-ping
  [endpoint-or-server, ^ChannelHandlerContext context]
  (let [endpoint (if (satisfies? IEndpoint endpoint-or-server)
                   endpoint-or-server
                   (lookup-endpoint endpoint-or-server, (channel-id context)))]
    (send-message endpoint, ::ping)))


(defn ^ChannelHandler create-ping-handler
  [endpoint-or-server]
  (netty/channel-handler
    :user-event-triggered
    ([_, channel-context, event]
      (try
        (if (instance? IdleStateEvent event)
          (send-ping endpoint-or-server, channel-context)
          ; propagate other events
          (.fireUserEventTriggered channel-context event))
        (catch Throwable t
          (log/errorf "Exception during ping-handler:\n%s" (with-out-str (print-cause-trace t))))))))


(defn ^ChannelHandler create-exception-handler
  []
  (netty/channel-handler
    :exception-caught
    ([_, context, cause]
      (log/errorf "Uncaught exception in Netty pipeline:\n%s" (with-out-str (print-cause-trace cause))))))


(defn ^ChannelHandler create-message-handler
  [message-handler, connect-handler, disconnect-handler]
  (netty/channel-handler
    :channel-active
    ([_ channel-context]
      (connect-handler channel-context))
    
    :channel-inactive
    ([_ channel-context]
      (disconnect-handler channel-context))

    :channel-read
    ([_ channel-context message]
      (message-handler channel-context message))))



(defn+opts setup-pipeline
  "Setup pipeline with exception handler, ssl handler (if specified) and alive-check.
  "
  [endpoint-or-server, message-handler, connect-handler, disconnect-handler, ^ChannelPipeline p
   | {ssl-context nil, ssl-mode (choice :client :server), alive-check-timeout 1000} :as options]
  (try
    (when ssl-context
      (.addLast p "ssl-handler" (SslHandler. (ssl/ssl-engine-from-context ssl-context :mode ssl-mode))))
    (.addLast p "idle-state-handler" (IdleStateHandler. 0, 0, (int alive-check-timeout), TimeUnit/MILLISECONDS))
    (.addLast p "ping-handler" (create-ping-handler endpoint-or-server))
    (.addLast p "message-encoder" (create-message-encoder options))
    (.addLast p "message-decoder" (create-message-decoder options))
    (.addLast p "message-handler" (create-message-handler message-handler, connect-handler, disconnect-handler))
    (.addLast p "exception-handler" (create-exception-handler))
    (catch Throwable t
      (log/errorf "Pipeline setup failed with exception:\n%s" (with-out-str (print-cause-trace t))))))


(defn try-connect
  [bootstrap, server-address, server-port]
  (try
    (netty/connect-client bootstrap, server-address, server-port)
    (catch Throwable t
      (log/tracef "Reconnecting to %s:%s failed due to: %s" server-address, server-port, (.getMessage t))
      nil)))


(defn connect-with-retries
  [bootstrap, server-address, server-port]
  (try
    (log/debugf "Starting connection attempts to %s:%s." server-address, server-port)
    (loop [attempt 1]
      (log/tracef "Connection attempt #%s starts ...", attempt)
      (if-let [channel (try-connect bootstrap, server-address, server-port)]
        (do
          (log/tracef "Connection attempt #%s succeeded.", attempt)
          channel)
        (do
          (log/tracef "Connection attempt #%s failed. Going to sleep now ...", attempt)
          (Thread/sleep (reconnect-wait-duration attempt))
          (log/tracef "Connection attempt #%s awakes.", attempt)
          (recur (inc attempt)))))
    (catch Throwable t
      (log/errorf "Connection attempts to %s:%s failed with exception:\n%s"
        server-address, server-port, (with-out-str (print-cause-trace t)))
      nil)))





(deftype ClientEndpoint [server-address, server-port, bootstrap-atom, channel-atom, id-atom]
  
  IClientEndpoint
  
  (connect [this]
    (try
      (locking this
        (when-not (connected? this)
          (if-let [bootstrap @bootstrap-atom]
            (do
              (log/debugf "Connecting to %s." (address-str this))
              (let [; connect to server and wait for stream to be realized
                    channel (connect-with-retries bootstrap, server-address, server-port)]
                (reset! channel-atom channel)
                true))
            (throw (Exception. "Client bootstrap not set!")))))
      (catch Throwable t
        (log/errorf "Connection to %s failed with exception:\n%s" (address-str this), (with-out-str (print-cause-trace t)))
        false)))
  
  (shutdown [this]
    (locking this
      (disconnect this)
      (when-let [bootstrap @bootstrap-atom]
        (netty/shutdown-event-loop-group bootstrap)
        (reset! bootstrap-atom nil))))
  
  IEndpoint 
  
  (id [this]
    @id-atom)
  
  (info [this]
    {:remote-address server-address, :remote-port server-port})
  
  (address-str [this]
    (str server-address ":" server-port))
  
  (connected? [this]
    (some? @channel-atom))
  
  (disconnect [this]
    (locking this
      (when (connected? this)
        (let [channel @channel-atom]
          (reset! channel-atom nil)
          (reset! id-atom nil)
          (netty/close-channel channel))))
    this)
  
  IMessenger
  
  (send-message [this, message]
    (when-let [channel @channel-atom]
      (netty/write-to-channel channel message)
      this))
  
  Closeable
  
  (close [this]
    (shutdown this))
  
  Object
  
  (toString [this]
    (format "%s [%s:%s]" @id-atom, server-address, server-port)))


(defn client-internal-message-handler
  [endpoint, external-message-handler, channel-context, message]
  (when-not (= message ::ping)
    (if (= (type message) ::greeting)
      (let [endpoint-id (:endpoint-id message)]
        (-> ^ClientEndpoint endpoint .id-atom (reset! endpoint-id)))
      (try
        (log/tracef "Received message of type %s." (type message))
        (external-message-handler endpoint, message)
        (catch Throwable t
          (log/errorf "Exception in external message handler of the client:\n%s"
            (with-out-str (print-cause-trace t))))))))


(defn client-internal-connect-handler
  [endpoint, external-connect-handler, channel-context]
  (log/infof "Connection to server %s established!" (address-str endpoint))
  (when external-connect-handler
    (try
      (external-connect-handler endpoint)
      (catch Throwable t
        (log/errorf "Exception in the external connect handler of the client:\n%s"
          (with-out-str (print-cause-trace t)))))))


(defn client-internal-disconnect-handler
  [endpoint, external-disconnect-handler, channel-context]
  (let [server-disconnected? (when (connected? endpoint) (disconnect endpoint) true)]
    (when server-disconnected?
      (log/warnf "Server %s disconnected!" (address-str endpoint))
      (when external-disconnect-handler
        (try
          (external-disconnect-handler endpoint)
          (catch Throwable t
            (log/errorf "Exception in the external disconnect handler of the client:\n%s"
              (with-out-str (print-cause-trace t)))))))))


(defn+opts create-client
  "
  <alive-check-timeout>Idle time after which the server connection is checked</>
  <connect-handler>Called when client is connected to server. (fn [endpoint] ...)</>
  <disconnect-handler>Called when client is disconnected from server. (fn [endpoint] ...)</>"
  [server-address, server-port, message-handler | {connect-handler nil, disconnect-handler nil, ssl-enabled false} :as options]
  (let [client-endpoint (ClientEndpoint. server-address, server-port, (atom nil), (atom nil), (atom nil))
        ssl-context (when ssl-enabled (ssl/create-ssl-context options)),
        bootstrap (netty/create-client-bootstrap
                    (fn [pipeline]
                      (setup-pipeline
                        client-endpoint,
                        #(client-internal-message-handler client-endpoint, message-handler, %1, %2),
                        #(client-internal-connect-handler client-endpoint, connect-handler, %),
                        #(client-internal-disconnect-handler client-endpoint, disconnect-handler, %),
                        pipeline,
                        :ssl-mode :client,
                        :ssl-context ssl-context,
                        options))
                    options)]
    (doto client-endpoint
      (-> .bootstrap-atom (reset! bootstrap)))))



(deftype ServerEndpoint [channel-id, client-address, client-port, channel-atom]
  
  IEndpoint 
  
  (id [this]
    channel-id)
  
  (info [this]
    {:remote-address client-address, :remote-port client-port})
  
  (address-str [this]
    (str client-address ":" client-port))
  
  (connected? [this]
    (some? @channel-atom))
  
  (disconnect [this]
    (locking this
      (when (connected? this)
        (let [channel @channel-atom]
          (reset! channel-atom nil)
          (netty/close-channel channel))))
    this)
  
  IMessenger
  
  (send-message [this, message]
    (when-let [channel @channel-atom]
      (netty/write-to-channel channel, message)))
  
  Closeable
  
  (close [this]
    (disconnect this))
  
  Object
  
  (toString [this]
    (format "%s [%s:%s]" channel-id, client-address, client-port)))


(declare establish-connection)

(deftype MessageServer [server-address, server-port, endpoint-map, bootstrap-atom, server-channel-atom]
  
   IServer
   ; server can't be restarted after a stop
  (start [this]
    (locking this
      (when-not (running? this)
        (if-let [bootstrap @bootstrap-atom]
          (let [server-channel (netty/start-server bootstrap, server-address, server-port)]
            (reset! server-channel-atom server-channel))
          (throw (Exception. "Server bootstrap not set!")))))
    this)
  
  (running? [this]
    (some? @server-channel-atom))
  
  (lookup-endpoint [this, channel-id]
    (get (deref endpoint-map) channel-id))
  
  (stop [this]
    (locking this
      (when (running? this)
        (when-let [server-channel @server-channel-atom]
          (netty/close-server-channel server-channel)          
          (reset! server-channel-atom nil))
        (when-let [bootstrap @bootstrap-atom]
          (netty/shutdown-event-loop-group bootstrap))
        (reset! endpoint-map {})))
    this)
  
  Closeable
  
  (close [this]
    (stop this)))


(defn endpoint-map
   [^MessageServer message-server]
   (.endpoint-map message-server))


(defn certificate-subject
  [^SSLEngine ssl-engine]
  (when-let [cert (some-> ssl-engine .getSession .getPeerCertificateChain (aget 0))]
    (.getSubjectDN ^X509Certificate cert)))


(defn connection-info
  [^Channel channel]
  (let [^InetSocketAddress address (.remoteAddress channel)]
    (format "%s:%s [%s]" (.. address getAddress getHostAddress) (.getPort address) (channel-id channel))))


(defn register-endpoint
  [message-server, ^ChannelHandlerContext channel-context]
  (let [channel (.channel channel-context),
        ^InetSocketAddress address (.remoteAddress channel),
        client-address (.. address getAddress getHostAddress),
        client-port (.getPort address),
        channel-id (channel-id channel),
        endpoint (ServerEndpoint. channel-id, client-address, client-port, (atom channel))]
    (swap! (endpoint-map message-server) assoc channel-id endpoint)
    endpoint))


(defn remove-endpoint
  [message-server, endpoint-id]
  (swap! (endpoint-map message-server) dissoc endpoint-id))


(defn server-internal-message-handler
  [message-server, external-message-handler, channel-context, message]
  (when-not (= message ::ping)
    (if-let [remote-endpoint (lookup-endpoint message-server (channel-id channel-context))]
      (try
        (log/tracef "Received message of type %s." (type message))
        (external-message-handler message-server, remote-endpoint, message)
        (catch Throwable t
          (log/errorf "Exception in external message handler of the server:\n%s"
            (with-out-str (print-cause-trace t)))))
      (log/errorf "No endpoint found for %s"))))


(defn server-internal-connect-handler
  [message-server, external-connect-handler, channel-context]
  (let [remote-endpoint (register-endpoint message-server, channel-context)]
    (log/infof "Connection established from %s!", (address-str remote-endpoint))
    (netty/write-later-to-channel
      (.channel ^ChannelHandlerContext channel-context)
      (with-meta {:endpoint-id (id remote-endpoint)} {:type ::greeting}))
    (when external-connect-handler
      (try
        (external-connect-handler message-server, remote-endpoint)
        (catch Throwable t
          (log/errorf "Exception in the external connect handler of the server:\n%s"
            (with-out-str (print-cause-trace t))))))))


(defn server-internal-disconnect-handler
  [message-server, external-disconnect-handler, channel-context]
  (when (running? message-server)
    (let [channel-id (channel-id channel-context),
          remote-endpoint (lookup-endpoint message-server, channel-id)]
      (log/infof "Connection from %s closed!", (address-str remote-endpoint))
      (when external-disconnect-handler
        (try
          (external-disconnect-handler message-server, remote-endpoint)
          (catch Throwable t
            (log/errorf "Exception in the external disconnect handler of the server:\n%s"
              (with-out-str (print-cause-trace t))))))
      (remove-endpoint message-server, channel-id))))


(defn+opts create-server
  "
  connect-handler: (fn [message-server, endpoint])
  disconnect-handler: (fn [message-server, endpoint])
  message-handler: (fn [message-server, endpoint, message])"
  [server-address, server-port, message-handler | {connect-handler nil, disconnect-handler nil, ssl-enabled false, alive-check-timeout 5000,
                                                   compressed false, no-wrap true, compression-level (choice 9 0 1 2 3 4 5 6 7 8), compression-algorithm (choice :snappy :gzip)} :as options]
  (let [message-server (MessageServer. server-address, server-port, (atom {}), (atom nil), (atom nil)),
        ssl-context (when ssl-enabled (ssl/create-ssl-context options))
        server-bootstrap (netty/create-server-bootstrap
                           (fn [pipeline]
                             (setup-pipeline
                               message-server,
                               #(server-internal-message-handler message-server, message-handler, %1, %2),
                               #(server-internal-connect-handler message-server, connect-handler, %),
                               #(server-internal-disconnect-handler message-server, disconnect-handler, %),
                               pipeline,
                               :ssl-mode :server,
                               :ssl-context ssl-context,
                               options))
                           options)]
    (doto message-server
      (-> .bootstrap-atom (reset! server-bootstrap)))))




(comment
  
  
  (def server (doto
                (create-server "127.0.0.1" 4711
                  (fn [_, endpoint, {:keys [ping] :as msg}]
                    (println "server received:" msg)
                    (when (and ping (pos? ping))
                      (send-message endpoint {:pong (dec ping)})))
                  :disconnect-handler (fn [_, {:keys [remote-address, remote-port]}]
                                        (println "SERVER: winke winke" remote-address remote-port))
                  :compressed true
                  :compression-algorithm :gzip
                  :alive-check-timeout 3000)
                start))
 
  (time (def client (doto (create-client "127.0.0.1" 4711
                            (fn [endpoint, {:keys [pong] :as m}]
                              (println "client received:" m)
                              (when (and pong (pos? pong))
                                (send-message endpoint {:ping (dec pong)})))
                            :disconnect-handler (fn [& args] (println "Server disconnected"))                            
                            :alive-check-timeout 1000
                            :compressed true
                            :compression-algorithm :gzip)
                      connect)))
  
  (send-message client {:ping 5})
  
)