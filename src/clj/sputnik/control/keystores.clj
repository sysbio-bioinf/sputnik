; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns sputnik.control.keystores
  "Inspired by http://cipherious.wordpress.com/2013/05/20/constructing-an-x-509-certificate-using-bouncy-castle/"
  (:import
    (java.util Random Date UUID)
    (java.security KeyStore KeyPairGenerator SecureRandom KeyPair Security)
    org.bouncycastle.cert.X509v3CertificateBuilder
    org.bouncycastle.asn1.ASN1Sequence
    org.bouncycastle.asn1.x500.X500Name
    org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
    org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
    org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
    org.bouncycastle.jce.provider.BouncyCastleProvider
    java.security.cert.X509Certificate)
  (:require
    [clojure.string :as str]
    [clojure.java.io :as io]))



(defn setup-provider
  []
  (when-not (Security/getProvider "BC")
    (Security/addProvider (BouncyCastleProvider.))))


(defn ^Random secure-prng
  []
  (SecureRandom/getInstance "SHA1PRNG"))


(defn ^KeyPair rsa-keypair
  ([keysize]
    (rsa-keypair keysize, (secure-prng)))
  ([keysize, ^SecureRandom prng]
    (let [gen (doto (KeyPairGenerator/getInstance "RSA")
                (.initialize (int keysize), prng))]
      (.generateKeyPair gen))))


(defn ^BigInteger serial-number
  [^Random prng]
  (-> prng .nextInt Math/abs BigInteger/valueOf))


(defn content-signer
  [private-key]
  (.. (JcaContentSignerBuilder. "SHA1WithRSAEncryption")
    (setProvider "BC")
    (build private-key)))


(defn create-certificate
  "
  validity-period in days"
  [subject-name, ^long keysize, ^long validity-period]
  (let [prng (secure-prng),
        keypair (rsa-keypair keysize, prng),        
        public-key (.getPublic keypair),
        private-key (.getPrivate keypair),
        serial-number (serial-number prng),
        subjectDN (X500Name. (format "C=DE,O=%1$s,OU=%1$s,CN=%1$s" subject-name)),
        issuerDN (X500Name. "C=DE,O=SputnikControl,OU=SputnikControl,CN=SputnikControl"),
        now (System/currentTimeMillis),
        not-before (Date. now),
        not-after (Date. (+ now (* validity-period 1000 60 60 24))),
        subjPubKeyInfo (SubjectPublicKeyInfo. (ASN1Sequence/getInstance (.getEncoded public-key))),
        cert-gen (X509v3CertificateBuilder. issuerDN, serial-number, not-before, not-after, subjectDN, subjPubKeyInfo)]
    {:public-key public-key, :private-key private-key,
     :certificate (.. (JcaX509CertificateConverter.)
                   (setProvider "BC")
                   (getCertificate (.build cert-gen (content-signer private-key))))}))



(defn ^KeyStore load-keystore
  [filename, ^String password]
  (doto (KeyStore/getInstance (KeyStore/getDefaultType))
    (.load (io/input-stream filename), (when password (.toCharArray password)))))


(defn new-keystore
  []
  (doto (KeyStore/getInstance (KeyStore/getDefaultType))
    (.load nil, nil)))


(defn save-keystore
  [^KeyStore ks, filename, ^String password]
  (.store ks, (io/output-stream filename), (when password (.toCharArray password))))


(defn add-keystore-entry
  [^KeyStore ks, alias, ^String password {:keys [private-key, certificate] :as certificate-map}]
  (.setKeyEntry ks, alias, private-key, (when password (.toCharArray password)), (into-array [certificate])))


(defn add-truststore-entry
  ([ts, certificate-map]
    (add-truststore-entry ts, (.toString (UUID/randomUUID)), certificate-map))
  ([^KeyStore ts, alias, {:keys [certificate] :as certificate-map}]
    (.setCertificateEntry ts, alias, certificate)))



(defn create-ssl-stores
  [keystore-filename, keystore-password, truststore-filename, truststore-password, key-alias]
  (when (str/blank? key-alias)
    (throw (IllegalArgumentException. "Non-blank key alias needed!")))
  (setup-provider)
  (let [cert (create-certificate "sputnik", 2048, 730)]
    ; build keystore
    (doto (new-keystore)
      (add-keystore-entry key-alias, keystore-password, cert)
      (save-keystore keystore-filename, keystore-password))
    ; build truststore
    (doto (new-keystore)
      (add-truststore-entry key-alias, cert)
      (save-keystore truststore-filename, truststore-password))
    nil))

(defn truststore-entries
  [filename, password]
  (let [ts (load-keystore filename, password)]
    (enumeration-seq (.aliases ts))))


(defn load-certificate
  [filename, password, alias]
  (let [ks (load-keystore filename, password)]
    (.getCertificate ks alias)))


(defn create-client-ssl-stores
  [client-keystore-filename, client-keystore-password, client-truststore-filename, client-truststore-password, client-key-alias,
   server-keystore-filename, server-keystore-password, server-truststore-filename, server-truststore-password, server-key-alias]
  (when (str/blank? client-key-alias)
    (throw (IllegalArgumentException. "Non-blank key alias for client key needed!")))
  (when (str/blank? server-key-alias)
    (throw (IllegalArgumentException. "Non-blank key alias for server key needed!")))
  (setup-provider)
  (let [client-cert (create-certificate "sputnik", 2048, 730),
        server-cert (load-certificate server-keystore-filename, server-keystore-password, server-key-alias)]
    (when (nil? server-cert)
      (throw (RuntimeException. (format "Certificate with alias \"%s\" not found in server keystore \"%s\"!" server-keystore-filename, server-key-alias))))
    ; build client keystore
    (doto (new-keystore)
      (add-keystore-entry client-key-alias, client-keystore-password, client-cert)
      (save-keystore client-keystore-filename, client-keystore-password))
    ; build client truststore
    (doto (new-keystore)
      (add-truststore-entry server-key-alias, {:certificate server-cert})
      (save-keystore client-truststore-filename, client-truststore-password))
    ; add client certificate to server truststore
    (doto (load-keystore server-truststore-filename, server-truststore-password)
      (add-truststore-entry client-key-alias, client-cert)
      (save-keystore server-truststore-filename, server-truststore-password))
    nil))