; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns feature-selection.bitset
  (:refer-clojure
    :exclude [and, or])
  (:require
    [clojure.string :as str]
    [feature-selection.tools :as t])
  (:import
    java.util.BitSet))



(definterface IReadBitSet
  (^boolean getBit [^long i])
  (^long nextSetBit [^long i])
  (^long nextClearBit [^long i])
  (^long previousSetBit [^long i])
  (^long previousClearBit [^long i])
  (^long cardinality [])
  (intersects [otherBitset])
  (^long size []))


(definterface IWriteBitSet
  (setBit [^long i])
  (setBit [^long i, ^boolean v])
  (setBits [^long from, ^long to])
  (setBits [^long from, ^long to, ^boolean v])
  (clearBit [^long i])
  (clearBits [^long from, ^long to])
  (flipBit [^long i])
  (flipBits [^long from, ^long to])
  (or [otherBitSet])
  (xor [otherBitSet])
  (and [otherBitSet])
  (andNot [otherBitSet]))


(definterface IBitSetData
  (getBitset [])
  (toByteArray [])
  (toLongArray [])
  (clone []))


(definterface ISwitch
  (makeReadonly [])
  (makeWritable []))


(declare create-writable-bitset, create-readonly-bitset)

(deftype ReadOnlyBitSet [^:volatile-mutable ^BitSet bitset]
  
  Object
  
  (hashCode [_]
    (.hashCode bitset))
  
  (equals [this, other]
    (cond
      (identical? this, other) true,
      (instance? IBitSetData other) (.equals bitset (.getBitset ^IBitSetData other)), 
      :else false))
  
  IReadBitSet
  
  (getBit [_, i]
    (.get bitset (unchecked-int i)))
  
  (nextSetBit [_, i]
    (.nextSetBit bitset (unchecked-int i)))
  
  (nextClearBit [_, i]
    (.nextClearBit bitset (unchecked-int i)))
  
  (previousSetBit [_, i]
    (.previousSetBit bitset (unchecked-int i)))
  
  (previousClearBit [_, i]
    (.previousClearBit bitset (unchecked-int i)))
  
  (cardinality [_]
    (.cardinality bitset))
  
  (intersects [_, otherBitSet]
    (.intersects bitset, ^BitSet (.getBitset ^IBitSetData otherBitSet)))
  
  (size [_]
    (.size bitset))
  
  IBitSetData
  
  (getBitset [_]
    bitset)
  
  (toByteArray [_]
    (.toByteArray bitset))
  
  (toLongArray [_]
    (.toLongArray bitset))
  
  (clone [_]
    (ReadOnlyBitSet. (.clone bitset)))
  
  ISwitch
  
  (makeReadonly [this]
    this)
  
  (makeWritable [_]
    (create-writable-bitset (.clone bitset))))



(deftype WritableBitSet [^:volatile-mutable ^BitSet bitset]
  
  Object
  
  (hashCode [_]
    (.hashCode bitset))
  
  (equals [this, other]
    (cond
      (identical? this, other) true,
      (instance? IBitSetData other) (.equals bitset (.getBitset ^IBitSetData other)), 
      :else false))
  
  IReadBitSet
  
  (getBit [_, i]
    (.get bitset (unchecked-int i)))
  
  (nextSetBit [_, i]
    (.nextSetBit bitset (unchecked-int i)))
  
  (nextClearBit [_, i]
    (.nextClearBit bitset (unchecked-int i)))
  
  (previousSetBit [_, i]
    (.previousSetBit bitset (unchecked-int i)))
  
  (previousClearBit [_, i]
    (.previousClearBit bitset (unchecked-int i)))
  
  (cardinality [_]
    (.cardinality bitset))
  
  (intersects [_, otherBitSet]
    (.intersects bitset, ^BitSet (.getBitset ^IBitSetData otherBitSet)))
  
  (size [_]
    (.size bitset))
  
  IWriteBitSet
  
  (setBit [this, i]
    (.set bitset, (unchecked-int i))
    this)
  
  (setBit [this, i, v]
    (.set bitset, (unchecked-int i), v)
    this)
  
  (setBits [this, from, to]    
    (.set bitset, (unchecked-int from), (unchecked-int to))
    this)
  
  (setBits [this, from, to, v]    
    (.set bitset, (unchecked-int from), (unchecked-int to), v)
    this)
  
  (clearBit [this, i]
    (.clear bitset, (unchecked-int i))
    this)
  
  (clearBits [this, from, to]    
    (.clear bitset, (unchecked-int from), (unchecked-int to))
    this)
 
  (flipBit [this, i]
    (.flip bitset, (unchecked-int i))
    this)
  
  (flipBits [this, ^long from, ^long to]    
    (.flip bitset, (unchecked-int from), (unchecked-int to))
    this)
  
  (or [this, otherBitSet]
    (.or bitset, (.getBitset ^IBitSetData otherBitSet))
    this)
  
  (xor [this, otherBitSet]
    (.xor bitset, (.getBitset ^IBitSetData otherBitSet))
    this)
  
  (and [this, otherBitSet]
    (.and bitset, (.getBitset ^IBitSetData otherBitSet))
    this)
  
  (andNot [this, otherBitSet]
    (.andNot bitset, (.getBitset ^IBitSetData otherBitSet))
    this)
  
  IBitSetData
  
  (getBitset [_]
    bitset)
  
  (toByteArray [_]
    (.toByteArray bitset))
  
  (toLongArray [_]
    (.toLongArray bitset))
  
  (clone [_]
    (WritableBitSet. (.clone bitset)))
  
  ISwitch
  
  (makeReadonly [_]
    (create-readonly-bitset bitset))
  
  (makeWritable [this]
    this))




(defn- create-writable-bitset
  [bitset]
  (WritableBitSet. bitset))


(defn- create-readonly-bitset
  [bitset]
  (ReadOnlyBitSet. bitset))


(defn writable-bitset
  [^long n]
  (WritableBitSet. (BitSet. (unchecked-int n))))


(defn readonly-bitset
  [^long n]
  (ReadOnlyBitSet. (BitSet. (unchecked-int n))))


(defn writable-bitset-from-byte-array
  [^bytes bytes]
  (WritableBitSet. (BitSet/valueOf bytes)))

(defn readonly-bitset-from-byte-array
  [^bytes bytes]
  (ReadOnlyBitSet. (BitSet/valueOf bytes)))


(defn writable-bitset-from-long-array
  [^longs longs]
  (WritableBitSet. (BitSet/valueOf longs)))

(defn readonly-bitset-from-long-array
  [^longs longs]
  (ReadOnlyBitSet. (BitSet/valueOf longs)))



(t/definline-method-fn get-bit        (getBit [^IReadBitSet rbs, ^long i]))
(t/definline-method-fn next-set-bit   (nextSetBit ^long [^IReadBitSet rbs, ^long i]))
(t/definline-method-fn next-clear-bit (nextClearBit ^long [^IReadBitSet rbs, ^long i]))
(t/definline-method-fn prev-set-bit   (previousSetBit ^long [^IReadBitSet rbs, ^long i]))
(t/definline-method-fn prev-clear-bit (previousClearBit ^long [^IReadBitSet rbs, ^long i]))
(t/definline-method-fn cardinality    (cardinality ^long [^IReadBitSet rbs]))
(t/definline-method-fn intersects?    (intersects [^IReadBitSet rbs1, ^IBitSetData bsd]))
(t/definline-method-fn size           (size ^long [^IReadBitSet rbs]))

(t/definline-method-fn set-bit     (setBit    [^IWriteBitSet wbs, ^long i]))
(t/definline-method-fn set-bit-to  (setBit    [^IWriteBitSet wbs, ^long i, v]))
(t/definline-method-fn set-bits    (setBits   [^IWriteBitSet wbs, ^long from, ^long to]))
(t/definline-method-fn set-bits-to (setBits   [^IWriteBitSet wbs, ^long from, ^long to, v]))
(t/definline-method-fn clear-bit   (clearBit  [^IWriteBitSet wbs, ^long i]))
(t/definline-method-fn clear-bits  (clearBits [^IWriteBitSet wbs, ^long from, ^long to]))
(t/definline-method-fn flip-bit    (flipBit   [^IWriteBitSet wbs, ^long i]))
(t/definline-method-fn flip-bits   (flipBits  [^IWriteBitSet wbs, ^long from, ^long to]))
(t/definline-method-fn or          (or        [^IWriteBitSet wbs, ^IBitSetData bsd]))
(t/definline-method-fn xor         (xor       [^IWriteBitSet wbs, ^IBitSetData bsd]))
(t/definline-method-fn and         (and       [^IWriteBitSet wbs, ^IBitSetData bsd]))
(t/definline-method-fn and-not     (andNot   [^IWriteBitSet wbs, ^IBitSetData bsd]))

(t/definline-method-fn to-byte-array (toByteArray [^IBitSetData bsd]))
(t/definline-method-fn to-long-array (toLongArray [^IBitSetData bsd]))
(t/definline-method-fn clone         (clone [^IBitSetData bsd]))

(t/definline-method-fn make-writable (makeWritable [^ISwitch s]))
(t/definline-method-fn make-readonly (makeReadonly [^ISwitch s]))



(defmacro reduce-over-set-bits
  "Reduce over all set bits and update `result` via executing `body` for each set bit.
  The bit values are primitive long values."
  [[bit bitset] [result initial-value], & body]
  `(let [bs# ~bitset]
     (loop [i# (next-set-bit bs# 0), ~result ~initial-value]
       (if (<= 0 i#)
         (recur (next-set-bit bs# (unchecked-inc i#)),
           (let [~bit i#]
             ~@body))
         ~result))))


(defmacro forall-set-bits
  "Creates a vector by iterating through all set bits and executing body for each set bit.
  The bit values are primitive long values."
  [[bit bitset], & body]
  `(let [bs# ~bitset]
     (loop [i# (next-set-bit bs# 0), result# (transient [])]
       (if (<= 0 i#)
         (recur (next-set-bit bs# (unchecked-inc i#)),
           (let [~bit i#]
             (conj! result# (do ~@body))))
         (persistent! result#)))))


(defn vector-of-set-bits
  [bs]
  (forall-set-bits [b bs] b))


(defn print-bitset
  [bs, ^java.io.Writer w]
  (.write w, (str/join ", " (vector-of-set-bits bs))))


(defmethod print-method WritableBitSet
  [bs, ^java.io.Writer w]
  (.write w "WritableBitSet: #{")
  (print-bitset bs, w)
  (.write w "}"))


(defmethod print-method ReadOnlyBitSet
  [bs, ^java.io.Writer w]
  (.write w "ReadOnlyBitSet: #{")
  (print-bitset bs, w)
  (.write w "}"))



(defn one-point-crossover
  [bs1, bs2, point]
  (let [n (size bs1),
        bs1 (make-writable bs1),
        bs2 (make-writable bs2),
        child1a (-> bs1 clone (clear-bits 0 point)),
        child1b (-> bs1 clone (clear-bits point n))
        child2a (-> bs2 clone (clear-bits 0 point)),
        child2b (-> bs2 clone (clear-bits point n))]
    [(make-readonly (or child1a child2b)) (make-readonly (or child2a child1b))]))


(defn nth-set-bit
  ^long [bs, ^long n]
  (loop [i (next-set-bit bs, 0), cnt 0]
    (cond
      (== i -1) -1
      (== cnt n) i
      :else (recur (next-set-bit bs, (unchecked-inc i)), (unchecked-inc cnt)))))


(defn nth-clear-bit
  ^long [bs, ^long n]
  (loop [i (next-clear-bit bs 0), cnt 0]
    (cond
      (== i -1) -1
      (== cnt n) i
      :else (recur (next-clear-bit bs (unchecked-inc i)), (unchecked-inc cnt)))))