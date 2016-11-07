; This namespace contains the needed macros and functions from aleph.netty with modifications as needed
;
; The code from aleph is licensed under The MIT License (MIT):
;
; Copyright (c) 2014 Zachary Tellman
; Permission is hereby granted, free of charge, to any person obtaining a copy
; of this software and associated documentation files (the "Software"), to deal
; in the Software without restriction, including without limitation the rights
; to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
; copies of the Software, and to permit persons to whom the Software is
; furnished to do so, subject to the following conditions:
;
; The above copyright notice and this permission notice shall be included in
; all copies or substantial portions of the Software.
;
; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
; IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
; FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
; AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
; LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
; OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
; THE SOFTWARE.
;
; All modifications and all additional code are licensed und the Eclipse Public License (EPL):
;
; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.netty
  (:require
    [clojure.tools.logging :as log]
    [clojure.options :refer [defn+opts]])
  (:import
    (io.netty.bootstrap
      Bootstrap
      ServerBootstrap)
    (io.netty.channel
      Channel
      ChannelFuture
      ChannelHandler
      ChannelInboundHandler
      ChannelOutboundHandler
      ChannelOption
      EventLoopGroup)
    (io.netty.channel.epoll
      Epoll
      EpollEventLoopGroup
      EpollServerSocketChannel
      EpollSocketChannel)
    (io.netty.channel.socket
      ServerSocketChannel)
    (io.netty.channel.socket.nio
      NioServerSocketChannel
      NioSocketChannel)
    (io.netty.channel.nio
      NioEventLoopGroup)
    (io.netty.util.concurrent
      DefaultThreadFactory)
    (java.net
      InetSocketAddress)))



; take from aleph.netty/channel-handler
(defmacro channel-handler
  "Create a channel handler with the specified methods while using default implementation for all other methods."
  [& {:as handlers}]
  `(reify
     ChannelHandler
     ChannelInboundHandler
     ChannelOutboundHandler

     (handlerAdded
       ~@(or (:handler-added handlers) `([_ _])))
     (handlerRemoved
       ~@(or (:handler-removed handlers) `([_ _])))
     (exceptionCaught
       ~@(or (:exception-caught handlers)
           `([_ ctx# cause#]
              (.fireExceptionCaught ctx# cause#))))
     (channelRegistered
       ~@(or (:channel-registered handlers)
           `([_ ctx#]
              (.fireChannelRegistered ctx#))))
     (channelUnregistered
       ~@(or (:channel-unregistered handlers)
           `([_ ctx#]
              (.fireChannelUnregistered ctx#))))
     (channelActive
       ~@(or (:channel-active handlers)
           `([_ ctx#]
              (.fireChannelActive ctx#))))
     (channelInactive
       ~@(or (:channel-inactive handlers)
           `([_ ctx#]
              (.fireChannelInactive ctx#))))
     (channelRead
       ~@(or (:channel-read handlers)
           `([_ ctx# msg#]
              (.fireChannelRead ctx# msg#))))
     (channelReadComplete
       ~@(or (:channel-read-complete handlers)
           `([_ ctx#]
              (.fireChannelReadComplete ctx#))))
     (userEventTriggered
       ~@(or (:user-event-triggered handlers)
           `([_ ctx# evt#]
              (.fireUserEventTriggered ctx# evt#))))
     (channelWritabilityChanged
       ~@(or (:channel-writability-changed handlers)
           `([_ ctx#]
              (.fireChannelWritabilityChanged ctx#))))
     (bind
       ~@(or (:bind handlers)
           `([_ ctx# local-address# promise#]
              (.bind ctx# local-address# promise#))))
     (connect
       ~@(or (:connect handlers)
           `([_ ctx# remote-address# local-address# promise#]
              (.connect ctx# remote-address# local-address# promise#))))
     (disconnect
       ~@(or (:disconnect handlers)
           `([_ ctx# promise#]
              (.disconnect ctx# promise#))))
     (close
       ~@(or (:close handlers)
           `([_ ctx# promise#]
              (.close ctx# promise#))))
     (read
       ~@(or (:read handlers)
           `([_ ctx#]
              (.read ctx#))))
     (write
       ~@(or (:write handlers)
           `([_ ctx# msg# promise#]
              (.write ctx# msg# promise#))))
     (flush
       ~@(or (:flush handlers)
           `([_ ctx#]
              (.flush ctx#))))))


; take from aleph.netty/pipeline-initializer
(defn pipeline-initializer
  "Setup pipeline via the given function at the time a channel is registered."
  [pipeline-builder]
  (channel-handler
    :channel-registered
    ([this ctx]
      (let [pipeline (.pipeline ctx)]
        (try
          ; remove pipeline-initializer since it is only needed for setup
          (.remove pipeline this)
          (pipeline-builder pipeline)
          (.fireChannelRegistered ctx)
          (catch Throwable e
            (log/warn e "Failed to initialize channel")
            (.close ctx)))))))


; take from aleph.netty/epoll-available?
(defn epoll-available? []
  (Epoll/isAvailable))


; adapted from aleph.netty/{epoll-client-group, nio-client-group, get-default-event-loop-threads}
(defn+opts event-loop-group
  [thread-pool-name, epoll? | {netty-thread-count nil}]
  (let [thread-count (max 1 (or netty-thread-count (* 2 (.availableProcessors (Runtime/getRuntime))))),
        thread-factory (DefaultThreadFactory. ^String thread-pool-name, true)]
      (if epoll?
        (EpollEventLoopGroup. (long thread-count), thread-factory)
        (NioEventLoopGroup. (long thread-count) thread-factory))))


; adapted from aleph.netty/create-client
(defn+opts create-client-bootstrap
  "
  <epoll?>Specifies whether epoll is used when available.</>"
  [pipeline-builder | {epoll? false, connect-timeout 3000, bootstrap-transform identity} :as options]
  (let [epoll? (and epoll? (epoll-available?)),
        ^Class
        channel-class (if epoll?
                        EpollSocketChannel
                        NioSocketChannel),
        ^EventLoopGroup
        group (event-loop-group "sputnik-tcp-client-event-pool", epoll?, options)]
    (doto (Bootstrap.)
      (.option ChannelOption/SO_REUSEADDR true)
      (.option ChannelOption/MAX_MESSAGES_PER_READ Integer/MAX_VALUE)
      (.option ChannelOption/CONNECT_TIMEOUT_MILLIS (int connect-timeout))
      (.group group)
      (.channel channel-class)
      (.handler (pipeline-initializer pipeline-builder))
      bootstrap-transform)))


(defn ^Channel connect-client
  [^Bootstrap bootstrap, server-address, server-port]
  (let [address (InetSocketAddress. ^String server-address, (int server-port))]
    (.. bootstrap (connect address) syncUninterruptibly channel)))


(defn close-channel
  [^Channel channel]
  (.. channel close syncUninterruptibly)
  nil)


(defn shutdown-event-loop-group
  [^Bootstrap bootstrap]
  (.. bootstrap group shutdownGracefully syncUninterruptibly)
  nil)


(defn write-to-channel
  {:inline (fn [channel, data]
             `(let [^Channel channel# ~channel]
                (.. channel# (writeAndFlush ~data) syncUninterruptibly channel)))}
  [^Channel channel, data]
  (.. channel (writeAndFlush data) syncUninterruptibly channel))


(defn write-later-to-channel
  {:inline (fn [channel, data]
             `(let [^Channel channel# ~channel]
                (.. channel# (writeAndFlush ~data))
                channel#))}
  [^Channel channel, data]
  (.. channel (writeAndFlush data))
  channel)


; adapted from aleph.netty/start-server
(defn+opts create-server-bootstrap
  [pipeline-builder | {epoll? false, bootstrap-transform identity} :as options]
  (let [epoll? (and epoll? (epoll-available?)),
        ^Class
        channel-class (if epoll?
                        EpollServerSocketChannel
                        NioServerSocketChannel),
        ^EventLoopGroup
        group (event-loop-group "sputnik-tcp-server-event-pool", epoll?, options)]
    (doto (ServerBootstrap.)
      (.option ChannelOption/SO_BACKLOG (int 1024))
      (.option ChannelOption/SO_REUSEADDR true)
      (.option ChannelOption/MAX_MESSAGES_PER_READ Integer/MAX_VALUE)
      (.group group)
      (.channel channel-class)
      (.childHandler (pipeline-initializer pipeline-builder))
      (.childOption ChannelOption/SO_REUSEADDR true)
      (.childOption ChannelOption/MAX_MESSAGES_PER_READ Integer/MAX_VALUE)
      bootstrap-transform)))


(defn ^ServerSocketChannel start-server
  [^ServerBootstrap bootstrap, server-address, server-port]
  (let [address (InetSocketAddress. ^String server-address, (int server-port))]
    ; wait for bind to complete, rethrowing exceptions if any, and return channel
    (.. bootstrap (bind address) syncUninterruptibly channel)))


(defn close-server-channel
  [^ServerSocketChannel server-channel]
  (.. server-channel close syncUninterruptibly)
  nil)