(ns lens.amqp-async
  (:require [clojure.core.async :as async :refer [>!!]]
            [clojure.core.async.impl.protocols :as async-impl :refer [ReadPort Channel]]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.queue :as lqu]
            [langohr.consumers :as lco]
            [lens.logging :refer [debug]]))

(defprotocol AmqpChannel
  (amqp-channel [ch] "Returns the underlying AMQP channel.")
  (amqp-queue [ch] "Returns the underlying AMQP queue."))

(defn- delivery-fn [ch]
  (fn [amqp-ch meta payload]
    (debug {:action :receive-msg :payload-size (count payload)})
    (>!! ch payload)
    (lb/ack amqp-ch (:delivery-tag meta))))

(defn- declare-queue [amqp-ch {:keys [queue-name]}]
  (if queue-name
    (do (lqu/declare amqp-ch queue-name {:exclusive true}) queue-name)
    (lqu/declare-server-named amqp-ch)))

(defn chan
  "It's not cheap to create a channel!

  Opts accept :queue-name and :consumer-tag."
  ([conn]
   (chan conn 1))
  ([conn n]
   (chan conn n nil))
  ([conn n xform]
   (chan conn n xform {}))
  ([conn n xform opts]
   (let [amqp-ch (lch/open conn)
         _ (lb/qos amqp-ch n)
         ch (async/chan n xform)
         queue (declare-queue amqp-ch opts)]
     (debug {:action :subscribe :queue queue})
     (lco/subscribe amqp-ch queue (delivery-fn ch) (select-keys opts [:consumer-tag]))
     (reify
       ReadPort
       (take! [_ fn1-handler]
         (async-impl/take! ch fn1-handler))
       Channel
       (close! [_]
         (async-impl/close! ch))
       (closed? [_]
         (async-impl/closed? ch))
       AmqpChannel
       (amqp-channel [_]
         amqp-ch)
       (amqp-queue [_]
         queue)))))

(defn sub [exchange topic ch]
  (lqu/bind (amqp-channel ch) (amqp-queue ch) exchange {:routing-key topic}))
