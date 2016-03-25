(ns lens.broker
  (:use plumbing.core)
  (:require [clojure.core.async :as async :refer [go go-loop <! >!]]
            [clojure.java.io :as io]
            [cognitect.transit :as t]
            [com.stuartsierra.component :refer [Lifecycle]]
            [langohr.basic :as lb]
            [langohr.channel :as ch]
            [langohr.consumers :as co]
            [langohr.core :as rmq]
            [langohr.exchange :as ex]
            [langohr.queue :as qu]
            [lens.amqp-async :as aa]
            [lens.logging :as log :refer [error debug]]
            [lens.util :refer [NonBlankStr]]
            [schema.core :as s :refer [Keyword Any Uuid Int Str]]
            [shortid.core :as shortid])
  (:import [java.io ByteArrayOutputStream]
           [com.rabbitmq.client Channel]))

(set! *warn-on-reflection* true)

;; ---- Schemas ---------------------------------------------------------------

(def Params
  {Any Any})

(def Command
  "A command is something which a subject likes to do in a system.

  A command has an id which is an arbitrary UUID. The name of a command is a
  keyword like :create-subject in imperative form. The sub is the name of the
  subject which requested the command."
  {:id Uuid
   :name Keyword
   :sub NonBlankStr
   (s/optional-key :params) Params
   Any Any})

(def Event
  "An event is something which happend in a system.

  An event has an id which is an arbitrary UUID. The name of an event is a
  possibly namespaced keyword, which is used as topic in the lens-sds.events
  topic exchange. The name should be a verb in past tense like :subject/created."
  {:id Uuid
   :cid Uuid
   :name Keyword
   :sub NonBlankStr
   (s/optional-key :t) Int
   (s/optional-key :data) {Any Any}})

;; ---- Commands --------------------------------------------------------------

(defmulti handle-command (fn [_ {:keys [name]} _] name))

(defn- read-transit [payload]
  (try
    (t/read (t/reader (io/input-stream payload) :msgpack))
    (catch Exception e e)))

(defn requeue? [e]
  (if (:requeue (ex-data e)) true false))

(defn delivery-fn [env]
  (fn [^Channel ch {:keys [message-id delivery-tag]} payload]
    (let [cmd (read-transit payload)]
      (if (instance? Throwable cmd)
        (error cmd {:type :unreadable-command
                    :ch-num (.getChannelNumber ch)
                    :delivery-tag delivery-tag
                    :message-id message-id
                    :error (.getMessage ^Throwable cmd)})
        (if-let [e (s/check Command cmd)]
          (do (error {:type :invalid-command
                      :ch-num (.getChannelNumber ch)
                      :delivery-tag delivery-tag
                      :message-id message-id
                      :cmd cmd
                      :error e})
              (lb/reject ch delivery-tag))
          (do (debug {:ch-num (.getChannelNumber ch)
                      :delivery-tag delivery-tag
                      :cmd cmd})
              (try
                (handle-command env cmd (:params cmd))
                (lb/ack ch delivery-tag)
                (catch Exception e
                  (error e {:type :command-handling-error
                            :ch-num (.getChannelNumber ch)
                            :delivery-tag delivery-tag
                            :message-id message-id
                            :cmd cmd
                            :error (.getMessage e)})
                  (lb/reject ch delivery-tag (requeue? e))))))))))

(defn write [o]
  (let [out (ByteArrayOutputStream.)]
    (t/write (t/writer out :msgpack) o)
    (.toByteArray out)))

(s/defn send-command
  "Sends command to broker and returns a channel conveying the resulting event."
  {:arglists '([broker command])}
  [{:keys [ch queue event-pub]} {:keys [id] :as command} :- Command]
  (let [ch1 (async/chan)
        ch2 (async/chan)]
    (debug {:action :send-command :command command})
    (async/sub event-pub id ch1)
    (lb/publish ch "" queue (write command))
    (go
      (when-let [event (<! ch1)]
        (debug {:action :receive-event :event event})
        (async/unsub event-pub id ch1)
        (>! ch2 event))
      (async/close! ch2))
    ch2))

;; ---- Events ----------------------------------------------------------------

(defn event-routing-key [event-name]
  (if-let [ns (namespace event-name)]
    (str ns "." (name event-name))
    (name event-name)))

(s/defn send-event
  "Sends event to broker."
  {:arglists '([broker event])}
  [{:keys [ch exchange]} {:keys [name] :as event} :- Event]
  (debug {:event event})
  (lb/publish ch exchange (event-routing-key name) (write event)))

;; ---- Broker ----------------------------------------------------------------

(defn- info [msg]
  (log/info {:component "Broker" :msg msg}))

(defn- subscribe [ch queue f consumer-tag]
  (info (format "Subscribe to queue %s with consumer tag %s" queue consumer-tag))
  (co/subscribe ch queue f {:consumer-tag consumer-tag}))

(defn- consumer-tag [^Channel ch]
  (str "lens-sds-batch." (.getChannelNumber ch)))

(defrecord Broker [host port username password conn ch batch-chs queue
                   batch-queue exchange event-pub num-batch-threads]
  Lifecycle
  (start [broker]
    (info (str "Start broker on queue " batch-queue))
    (let [opts (cond-> {}
                 host (assoc :host host)
                 port (assoc :port port)
                 username (assoc :username username)
                 password (assoc :password password))
          conn (rmq/connect opts)
          ch (ch/open conn)
          batch-chs (for [_ (range num-batch-threads)] (ch/open conn))
          event-ch (aa/chan conn 16 (map read-transit)
                            {:queue-name (str "lens-sds-batch.events-" (shortid/generate 5))
                             :consumer-tag "lens-sds-batch"})
          event-pub (async/pub event-ch :cid)]
      (ex/declare ch exchange "topic" {:durable true})
      (qu/declare ch queue {:durable true :auto-delete false})
      (qu/declare ch batch-queue {:durable true :auto-delete false})
      (let [env {:broker (assoc broker :ch ch :exchange exchange
                                       :event-pub event-pub)}]
        (doseq [ch batch-chs]
          (lb/qos ch 1)
          (subscribe ch batch-queue (delivery-fn env) (consumer-tag ch))))
      (aa/sub exchange "#" event-ch)
      (assoc broker :conn conn :ch ch :event-pub event-pub)))

  (stop [broker]
    (info "Stop broker")
    (async/unsub-all event-pub)
    (rmq/close conn)
    (assoc broker :event-pub nil :ch nil :conn nil)))

(defn new-broker [opts]
  (cond-> opts
    true (assoc :queue "lens-sds.commands")
    true (assoc :batch-queue "lens-sds.batch-commands")
    true (assoc :exchange "lens-sds.events")
    (not (:num-batch-threads opts)) (assoc :num-batch-threads 4)
    true (map->Broker)))
