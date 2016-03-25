(ns lens.file-storage
  (:require [clojure.java.io :as io]
            [schema.core :as s :refer [Str Int Any]]
            [taoensso.carmine :as car :refer [wcar]]))

(def File
  {:host Str
   :port Int
   :key Str
   Any Any})

(s/defn read-file
  "Returns an inputstream of file."
  [file :- File]
  (-> (wcar {:pool {} :spec (select-keys file [:host :port])}
        (car/get (:key file)))
      (io/input-stream)))

