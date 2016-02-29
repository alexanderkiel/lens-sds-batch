(ns lens.import-clinical-data
  (:use plumbing.core)
  (:require [lens.broker :as b :refer [handle-command send-event]]
            [lens.logging :refer [debug error info]]
            [lens.util :refer [NonBlankStr]]
            [taoensso.carmine :as car :refer [wcar]]
            [clojure.core.async :as async :refer [<!! <! >! chan go go-loop]]
            [clojure.data.xml :as xml]
            [clojure.zip :as zip]
            [clojure.data.zip.xml :refer [xml-> xml1-> attr text]]
            [clojure.java.io :as io]
            [schema.core :as s :refer [Uuid Str Int Any Num]]
            [schema.coerce :as c]
            [schema.utils :as su]
            [clojure.string :as str]
            [clj-uuid :as uuid]
            [lens.util :as u]
            [clj-time.coerce :as tc]
            [clj-time.format :as tf])
  (:import [java.util Date]))

;; ---- Schemas ---------------------------------------------------------------

(def OID
  NonBlankStr)

(def SubjectKey
  NonBlankStr)

(def RepeatKey
  NonBlankStr)

(def TransactionType
  (s/enum "Insert" "Update" "Remove" "Upsert" "Context"))

(def TxType
  (s/enum :insert :update :remove :upsert :context))

(def DataType
  (s/enum :string :integer :float :datetime))

(def ItemData
  {:item-oid OID
   :tx-type TxType
   :data-type DataType
   :value Any})

(def ItemGroupData
  {:item-group-oid OID
   :tx-type TxType
   :items [ItemData]})

(def FormData
  {:form-oid OID
   :tx-type TxType
   :item-groups [ItemGroupData]})

(def StudyEventData
  {:study-event-oid OID
   :tx-type TxType
   :forms [FormData]})

(def SubjectData
  {:subject-key SubjectKey
   :tx-type TxType
   :study-events [StudyEventData]})

(def ClinicalData
  {:study-oid OID
   :subjects [SubjectData]})

;; ---- Parsing ---------------------------------------------------------------

(defn- validation-ex [{:keys [schema value]} loc]
  (ex-info (format "Invalid value %s should be a %s" value schema)
           {:type ::validation-error :schema schema :value value :loc loc}))

(defn- validate [checker loc value]
  (if-let [error (checker value)]
    (throw (validation-ex error loc))
    value))

(defn- coerce [coercer loc value]
  (let [value (coercer value)]
    (if-let [error (su/error-val value)]
      (throw (validation-ex error loc))
      value)))

(def oid-checker (s/checker OID))

(defn study-oid [loc]
  (->> (xml1-> loc (attr :StudyOID))
       (validate oid-checker loc)))

(def subject-key-checker (s/checker SubjectKey))

(defn subject-key [loc]
  (->> (xml1-> loc (attr :SubjectKey))
       (validate subject-key-checker loc)))

(defn study-event-oid [loc]
  (->> (xml1-> loc (attr :StudyEventOID))
       (validate oid-checker loc)))

(defn form-oid [loc]
  (->> (xml1-> loc (attr :FormOID))
       (validate oid-checker loc)))

(defn item-group-oid [loc]
  (->> (xml1-> loc (attr :ItemGroupOID))
       (validate oid-checker loc)))

(defn item-oid [loc]
  (->> (xml1-> loc (attr :ItemOID))
       (validate oid-checker loc)))

(defn string-data [loc]
  (xml1-> loc text))

(def integer-coercer (c/coercer Long {Long (c/safe #(Long/parseLong %))}))

(defn integer-data [loc]
  (->> (xml1-> loc text)
       (coerce integer-coercer loc)))

(def float-coercer (c/coercer Double {Double (c/safe #(Double/parseDouble %))}))

(defn float-data [loc]
  (->> (xml1-> loc text)
       (coerce float-coercer loc)))

(def datetime-coercer (c/coercer Date {Date (c/safe #(tc/to-date (tf/parse (tf/formatters :date-time) %)))}))

(defn datetime-data [loc]
  (->> (xml1-> loc text)
       (coerce datetime-coercer loc)))

(def transaction-type-checker (s/checker TransactionType))

(s/defn tx-type :- TxType
  "Tries to determine the transaction type of a loc. Does so recursive to
  parents. Throws a validation error on invalid or missing transaction type."
  [loc]
  (if-let [t (xml1-> loc (attr :TransactionType))]
    (-> (validate transaction-type-checker loc t)
        (str/lower-case)
        (keyword))
    (if-let [parent (zip/up loc)]
      (tx-type parent)
      (validate transaction-type-checker loc nil))))

(s/defn parse-string-item [item-data]
  {:item-oid (item-oid item-data)
   :tx-type (tx-type item-data)
   :data-type :string
   :value (string-data item-data)})

(s/defn parse-integer-item [item-data]
  {:item-oid (item-oid item-data)
   :tx-type (tx-type item-data)
   :data-type :integer
   :value (integer-data item-data)})

(s/defn parse-float-item [item-data]
  {:item-oid (item-oid item-data)
   :tx-type (tx-type item-data)
   :data-type :float
   :value (float-data item-data)})

(s/defn parse-datetime-item [item-data]
  {:item-oid (item-oid item-data)
   :tx-type (tx-type item-data)
   :data-type :datetime
   :value (datetime-data item-data)})

(s/defn parse-item-group [item-group-data]
  {:item-group-oid (item-group-oid item-group-data)
   :tx-type (tx-type item-group-data)
   :items
   (-> []
       (into (map parse-string-item)
             (xml-> item-group-data :ItemDataString))
       (into (map parse-integer-item)
             (xml-> item-group-data :ItemDataInteger))
       (into (map parse-float-item)
             (xml-> item-group-data :ItemDataFloat))
       (into (map parse-datetime-item)
             (xml-> item-group-data :ItemDataDatetime)))})

(s/defn parse-form [form-data]
  {:form-oid (form-oid form-data)
   :tx-type (tx-type form-data)
   :item-groups (mapv parse-item-group (xml-> form-data :ItemGroupData))})

(s/defn parse-study-event [study-event-data]
  {:study-event-oid (study-event-oid study-event-data)
   :tx-type (tx-type study-event-data)
   :forms (mapv parse-form (xml-> study-event-data :FormData))})

(s/defn parse-subject [subject-data]
  {:subject-key (subject-key subject-data)
   :tx-type (tx-type subject-data)
   :study-events (mapv parse-study-event (xml-> subject-data :StudyEventData))})

(s/defn parse-clinical-data :- ClinicalData [clinical-data]
  {:study-oid (study-oid clinical-data)
   :subjects (mapv parse-subject (xml-> clinical-data :SubjectData))})

;; ---- Commands --------------------------------------------------------------

(s/defn insert-subject [study-id :- Uuid subject-key :- SubjectKey]
  [:odm-import/insert-subject study-id {:subject-key subject-key}])

(s/defn upsert-subject [study-id :- Uuid subject-key :- SubjectKey]
  [:odm-import/upsert-subject study-id {:subject-key subject-key}])

(s/defn create-study-event [subject-id :- Uuid study-event-oid :- OID]
  [:odm-import/insert-study-event subject-id {:study-event-oid study-event-oid}])

(s/defn upsert-study-event [subject-id :- Uuid study-event-oid :- OID]
  [:odm-import/upsert-study-event subject-id {:study-event-oid study-event-oid}])

(s/defn create-form [study-event-id :- Uuid form-oid :- OID]
  [:odm-import/insert-form study-event-id {:form-oid form-oid}])

(s/defn create-item-group [form-id :- Uuid item-group-oid :- OID]
  [:odm-import/insert-item-group form-id {:item-group-oid item-group-oid}])

(s/defn create-item [item-group-id :- Uuid item-oid :- OID data-type :- DataType
                     value]
  [:odm-import/insert-item item-group-id {:item-oid item-oid :data-type data-type
                               :value value}])

;; ---- Events ----------------------------------------------------------------

(defn validation-failed [e]
  {:name :clinical-data-import/validation-failed
   :data (-> (select-keys (ex-data e) [:schema :value])
             (assoc :tag (:tag (zip/node (:loc (ex-data e))))))})

;; ---- Other -----------------------------------------------------------------

(defn- read-file [file]
  (-> (wcar {:pool {} :spec (select-keys file [:host :port])}
        (car/get (:key file)))
      (io/input-stream)))

(defn- pipeline [event-ch af coll]
  (let [ch (chan)]
    (async/pipeline-async 32 event-ch af ch)
    (async/onto-chan ch coll)))

(defn- dispatch [_ _ {:keys [tx-type]} _] tx-type)

(defmulti handle-item-data dispatch)

(defmethod handle-item-data :insert
  [send-command item-group-id {:keys [item-oid data-type value]} event-ch]
  (let [ch (send-command (create-item item-group-id item-oid data-type value))]
    (go
      (if-let [event (<! ch)]
        (do
          (>! event-ch event)
          (async/close! event-ch))
        (async/close! event-ch)))))

(defmulti handle-item-group-data dispatch)

(defmethod handle-item-group-data :insert
  [send-command form-id {:keys [item-group-oid items]} event-ch]
  (let [ch (send-command (create-item-group form-id item-group-oid))
        af (partial handle-item-data send-command
                    (uuid/v5 form-id item-group-oid))]
    (go
      (if-let [event (<! ch)]
        (do
          (>! event-ch event)
          (if (= :item-group/created (:name event))
            (pipeline event-ch af items)
            (async/close! event-ch)))
        (async/close! event-ch)))))

(defmulti handle-form-data dispatch)

(defmethod handle-form-data :insert
  [send-command study-event-id {:keys [form-oid item-groups]} event-ch]
  (let [ch (send-command (create-form study-event-id form-oid))
        af (partial handle-item-group-data send-command
                    (uuid/v5 study-event-id form-oid))]
    (go
      (if-let [event (<! ch)]
        (do
          (>! event-ch event)
          (if (= :form/created (:name event))
            (pipeline event-ch af item-groups)
            (async/close! event-ch)))
        (async/close! event-ch)))))

(defmulti handle-study-event-data dispatch)

(defmethod handle-study-event-data :insert
  [send-command subject-id {:keys [study-event-oid forms]} event-ch]
  (let [ch (send-command (create-study-event subject-id study-event-oid))
        af (partial handle-form-data send-command
                    (uuid/v5 subject-id study-event-oid))]
    (go
      (if-let [event (<! ch)]
        (do
          (>! event-ch event)
          (if (= :study-event/created (:name event))
            (pipeline event-ch af forms)
            (async/close! event-ch)))
        (async/close! event-ch)))))

(defmethod handle-study-event-data :upsert
  [send-command subject-id {:keys [study-event-oid forms]} event-ch]
  (let [ch (send-command (upsert-study-event subject-id study-event-oid))
        af (partial handle-form-data send-command
                    (uuid/v5 subject-id study-event-oid))]
    (go
      (if-let [event (<! ch)]
        (do
          (>! event-ch event)
          (if (#{:study-event/created :study-event/updated} (:name event))
            (pipeline event-ch af forms)
            (async/close! event-ch)))
        (async/close! event-ch)))))

(defmulti handle-subject-data dispatch)

(defmethod handle-subject-data :insert
  [send-command study-id {:keys [subject-key study-events]} event-ch]
  (let [ch (send-command (insert-subject study-id subject-key))
        af (partial handle-study-event-data send-command
                    (uuid/v5 study-id subject-key))]
    (go
      (if-let [event (<! ch)]
        (do
          (>! event-ch event)
          (if (= :subject/created (:name event))
            (pipeline event-ch af study-events)
            (async/close! event-ch)))
        (async/close! event-ch)))))

(defmethod handle-subject-data :upsert
  [send-command study-id {:keys [subject-key study-events]} event-ch]
  (let [ch (send-command (upsert-subject study-id subject-key))
        af (partial handle-study-event-data send-command
                    (uuid/v5 study-id subject-key))]
    (go
      (if-let [event (<! ch)]
        (do
          (>! event-ch event)
          (if (#{:subject/created :subject/updated} (:name event))
            (pipeline event-ch af study-events)
            (async/close! event-ch)))
        (async/close! event-ch)))))

(defmethod handle-subject-data :update
  [send-command study-id {:keys [subject-key study-events]} event-ch]
  (let [af (partial handle-study-event-data send-command
                    (uuid/v5 study-id subject-key))]
    (pipeline event-ch af study-events)))

(s/defn handle-clinical-data
  [send-command {:keys [study-oid subjects]} :- ClinicalData]
  (let [event-ch (chan)]
    (pipeline event-ch (partial handle-subject-data send-command
                                (uuid/v5 uuid/+null+ study-oid)) subjects)
    (<!! (go-loop []
           (when-let [event (<! event-ch)]
             (if (= "transaction-failed" (name (:name event)))
               (error {:event event})
               (debug {:event event}))
             (recur))))))

(defn command [name sub params-or-aid params]
  (cond-> {:id (uuid/squuid)
           :name name
           :sub sub}
    (not (map? params-or-aid)) (assoc :aid params-or-aid)
    (map? params-or-aid) (assoc :params params-or-aid)
    params (assoc :params params)))

(defn send-command-fn [broker sub]
  (fn [[name params-or-aid params]]
    (b/send-command broker (command name sub params-or-aid params))))

(defn read-and-parse-file [file]
  (let [start (System/nanoTime)
        data (-> (read-file file)
                 (xml/parse)
                 (zip/xml-zip)
                 (parse-clinical-data))]
    (info (format "Finished reading and parsing the file in %.1f s."
                  (/ (u/duration start) 1000)))
    data))

(defmethod handle-command :import-clinical-data
  [{:keys [broker]} {:keys [sub]} {:keys [file]}]
  (info "Start import of clinical data...")
  (let [start (System/nanoTime)]
    (try
      (->> (read-and-parse-file file)
           (handle-clinical-data (send-command-fn broker sub)))
      (catch Exception e
        (case (:type (ex-data e))
          ::validation-error (send-event broker (validation-failed e))
          (throw e))))
    (info (format "Finished import of clinical data in %.1f s."
                  (/ (u/duration start) 1000)))))
