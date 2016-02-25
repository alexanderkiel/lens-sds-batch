(ns user
  (:use plumbing.core)
  (:use criterium.core)
  (:require [clojure.pprint :refer [pprint pp]]
            [clojure.repl :refer :all]
            [clojure.tools.namespace.repl :refer [refresh]]
            [schema.core :as s]
            [com.stuartsierra.component :as comp]
            [lens.system :refer [new-system]]
            [environ.core :refer [env]]))

(s/set-fn-validation! true)

(def system nil)

(defn init []
  (when-not system (alter-var-root #'system (constantly (new-system env)))))

(defn start []
  (alter-var-root #'system comp/start))

(defn stop []
  (alter-var-root #'system comp/stop))

(defn startup []
  (init)
  (start))

(defn reset []
  (stop)
  (refresh :after 'user/startup))

;; Init Development
(comment
  (startup)
  )

;; Reset after making changes
(comment
  (reset)
  )

;; Init Remote Console
(comment
  (in-ns 'user)
  (init)
  )

(comment
  (pst)
  )
