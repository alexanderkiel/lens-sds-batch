(ns lens.app
  (:require [bidi.ring :as bidi-ring]
            [lens.handler :as h]))

(defn- route [opts]
  ["/health" (h/health-handler opts)])

(defn wrap-not-found [handler]
  (fn [req]
    (if-let [resp (handler req)]
      resp
      {:status 404})))

(defn app
  "Whole app Ring handler."
  [opts]
  (-> (bidi-ring/make-handler (route opts))
      (wrap-not-found)))
