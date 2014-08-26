; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.satellite.ssl
  (:import
    (javax.net.ssl SSLContext KeyManagerFactory TrustManagerFactory)
    java.security.KeyStore
    java.io.FileNotFoundException)
  (:require
    [clojure.java.io :as io]
    [sputnik.tools.file-system :as fs])
  (:use
    [clojure.options :only [defn+opts]]))



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
  <protocol>Specifies the protocol to use. (Only \"SSL\" is tested.)</>"
  [| {keystore "keystore.ks", keystore-password "", truststore "truststore.ks", truststore-password "", protocol "SSL"}]
  (let [; convert passwords for keystore and truststore to char array
        [keystore-pw truststore-pw] (map #(.toCharArray ^String %) [keystore-password truststore-password]),
        ; load keystore and truststore
        [keystore truststore] (map create-store [keystore truststore] [keystore-pw truststore-pw]),
        ; create KeyManagerFactory with loaded keystore
        kmf (doto (KeyManagerFactory/getInstance (KeyManagerFactory/getDefaultAlgorithm))
              (.init ^KeyStore keystore, keystore-pw)),
        ; create TrustManagerFactory with loaded truststore
        tmf (doto (TrustManagerFactory/getInstance (TrustManagerFactory/getDefaultAlgorithm))
              (.init ^KeyStore truststore))]
    ; create SSL context for the given protocol and initialize it
    (doto (SSLContext/getInstance protocol)
      (.init (.getKeyManagers kmf), (.getTrustManagers tmf), nil))))


(defn+opts setup-ssl
  "Set up the default SSLContext with the given options.
  Client authentication is mandatory."
  [| :as options]
  (let [context (doto (create-ssl-context options)
                  (-> .getDefaultSSLParameters (.setNeedClientAuth true)))]
    (SSLContext/setDefault context)    
    nil))