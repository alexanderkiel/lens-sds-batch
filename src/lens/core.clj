(ns lens.core
  (:use plumbing.core)
  (:require [com.stuartsierra.component :as comp]
            [environ.core :refer [env]]
            [lens.system :refer [new-system]]
            [lens.logging :refer [info]])
  (:import [java.util.concurrent Executors TimeUnit]))

(defn- max-memory []
  (quot (.maxMemory (Runtime/getRuntime)) (* 1024 1024)))

(defn- used-memory []
  (let [runtime (Runtime/getRuntime)]
    (quot (- (.totalMemory runtime) (.freeMemory runtime)) (* 1024 1024))))

(defn- log-memory []
  (info {:used-memory (used-memory) :max-memory (max-memory)}))

(defn- available-processors []
  (.availableProcessors (Runtime/getRuntime)))

(defn schedule-memory-logging [delay time-unit]
  (-> (Executors/newSingleThreadScheduledExecutor)
      (.scheduleWithFixedDelay log-memory delay delay time-unit)))

(defn -main [& _]
  (schedule-memory-logging 1 TimeUnit/MINUTES)
  (letk [[port thread version broker :as system] (new-system env)]
    (comp/start system)
    (info {:version version})
    (info {:max-memory (max-memory)})
    (info {:num-cpus (available-processors)})
    (info {:broker (:host broker)})
    (info {:listen (str "0.0.0.0:" port)})
    (info {:num-worker-threads thread})))
