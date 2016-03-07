(ns lens.system
  (:use plumbing.core)
  (:require [com.stuartsierra.component :as comp]
            [lens.server :refer [new-server]]
            [lens.broker :refer [new-broker]]
            [lens.util :as u]
            [lens.import-clinical-data]))

(defnk new-system [lens-sds-batch-version port broker-host {broker-port "5672"}]
  (comp/system-map
    :version lens-sds-batch-version
    :port (u/parse-long port)
    :thread 1

    :broker
    (new-broker {:host broker-host :port (u/parse-long broker-port)
                 :num-batch-threads 1})

    :server
    (comp/using (new-server) [:port :thread :broker])))
