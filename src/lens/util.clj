(ns lens.util
  (:require [clj-uuid :as uuid]
            [clojure.string :as str]
            [schema.core :as s :refer [Int]])
  (:import [java.io ByteArrayOutputStream]))

(defn parse-long [s]
  (Long/parseLong s))

(def Ms
  "Duration in milliseconds."
  s/Num)

(s/defn duration :- Ms
  "Returns the duaration in milliseconds from a System/nanoTime start point."
  [start :- Int]
  (/ (double (- (System/nanoTime) start)) 1000000.0))

;; ---- Schema ----------------------------------------------------------------

(def NonBlankStr
  (s/constrained s/Str (complement str/blank?) 'non-blank?))

(def PosInt
  (s/constrained s/Int pos? 'pos?))

(def NonNegInt
  (s/constrained s/Int (comp not neg?) 'non-neg?))

;; ---- UUID ------------------------------------------------------------------

(extend-protocol uuid/UUIDNameBytes

  clojure.lang.Keyword
  (as-byte-array ^bytes [this]
    (uuid/as-byte-array (str this)))

  clojure.lang.Seqable
  (as-byte-array ^bytes [this]
    (let [baos (ByteArrayOutputStream.)]
      (doseq [x (seq this)]
        (.write baos ^bytes (uuid/as-byte-array x)))
      (.toByteArray baos))))
