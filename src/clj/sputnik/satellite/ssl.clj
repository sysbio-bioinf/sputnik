; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.ssl
  (:import
    (javax.net.ssl SSLContext SSLEngine KeyManagerFactory TrustManagerFactory)
    java.security.KeyStore
    java.io.FileNotFoundException)
  (:require
    [clojure.java.io :as io]
    [sputnik.tools.file-system :as fs])
  (:use
    [clojure.options :only [defn+opts]]))

; TODO: access to client certificate via SSLEngine.getSession().getPeerCertificateChain()
; comment from stackoverflow:  http://stackoverflow.com/questions/9777864/how-can-i-get-the-client-certificate-in-netty-handler-to-identify-user
; The zeroth entry is the peer's own certificate.
; ChannelHandlerContext ctx = ...
; SslHandler sslhandler = (SslHandler) ctx.channel().pipeline().get("ssl"); ; channel wird gewrapped in eine map (via def-derived-map) an handler übergeben
; sslhandler.engine().getSession().getPeerCertificateChain()[0].getSubjectDN())

(defn create-store
  [store, password]
  (if-let [f (fs/find-file store)]
    (with-open [in (io/input-stream f)]
      (doto (KeyStore/getInstance (KeyStore/getDefaultType))
        (.load in, password)))
    (throw (FileNotFoundException. (format "File \"%s\" not found!" store)))))


(defn+opts ^SSLContext create-ssl-context
  "Creates an SSL context from the property file at the given URL.
  <keystore>Path to the file containing a Java keystore with the private key and certificate of this process (a sputnik node).</>
  <keystore-password>Password for the keystore.</>
  <truststore>Path to the file containing a Java keystore with the certificate entries which this process (a sputnik node) trusts.</>
  <truststore-password>Password for the truststore.</>
  <protocol>Specifies the protocol to use.</>"
  [| {keystore "keystore.ks", keystore-password "", truststore "truststore.ks", truststore-password "", protocol "TLSv1.2"}]
  (let [; convert passwords for keystore and truststore to char array
        [keystore-pw truststore-pw] (mapv #(.toCharArray ^String %) [keystore-password truststore-password]),
        ; load keystore and truststore
        [keystore truststore] (mapv create-store [keystore truststore] [keystore-pw truststore-pw]),
        ; create KeyManagerFactory with loaded keystore
        kmf (doto (KeyManagerFactory/getInstance (KeyManagerFactory/getDefaultAlgorithm))
              (.init ^KeyStore keystore, keystore-pw)),
        ; create TrustManagerFactory with loaded truststore
        tmf (doto (TrustManagerFactory/getInstance (TrustManagerFactory/getDefaultAlgorithm))
              (.init ^KeyStore truststore))]
    ; create SSL context for the given protocol and initialize it
    (doto (SSLContext/getInstance protocol)
      (.init (.getKeyManagers kmf), (.getTrustManagers tmf), nil)
      (.. getDefaultSSLParameters (setNeedClientAuth true)))))



(defn+opts ^SSLEngine ssl-engine-from-context
  [^SSLContext ssl-context | {mode (choice :client, :server)}]
  (doto (.createSSLEngine ssl-context)
    (.setUseClientMode (= mode :client))))


(defn+opts ^SSLEngine create-ssl-engine
  [| :as options]
  (ssl-engine-from-context (create-ssl-context options), options))





(defn+opts setup-ssl
  "Set up the default SSLContext with the given options.
  Client authentication is mandatory."
  [| :as options]
  (doto (create-ssl-context options)
    (SSLContext/setDefault)))
