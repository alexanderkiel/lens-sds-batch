(ns lens.handler
  "HTTP Handlers")

(defn health-handler [_]
  (fn [_]
    {:status 200
     :body "OK"}))
