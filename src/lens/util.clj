(ns lens.util
  (:require [clojure.string :as str]
            [schema.core :as s :refer [Int]]))

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
