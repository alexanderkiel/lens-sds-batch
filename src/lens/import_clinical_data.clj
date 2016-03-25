(ns lens.import-clinical-data
  (:use plumbing.core)
  (:require [clj-uuid :as uuid]
            [clojure.core.async :as async :refer [<!! <! >! chan go go-loop]]
            [clojure.data.xml :as xml]
            [lens.broker :as b :refer [handle-command send-event]]
            [lens.file-storage :as fs :refer [File]]
            [lens.logging :refer [debug error info]]
            [lens.util :as u :refer [NonBlankStr]]
            [lens-odm-parser.core :as p :refer [OID DataType SubjectKey
                                                ClinicalData
                                                ODMFile]]
            [schema.core :as s :refer [Uuid Str Int Any Num]])
  (:import [clojure.lang Keyword]))

;; ---- Commands --------------------------------------------------------------

(def Cmd
  [(s/one Keyword "name")
   (s/one {Keyword Any} "prams")])

(s/defn insert-subject [study-id :- Uuid subject-key :- SubjectKey]
  [:odm-import/insert-subject {:study-id study-id :subject-key subject-key}])

(s/defn upsert-subject [study-id :- Uuid subject-key :- SubjectKey]
  [:odm-import/upsert-subject {:study-id study-id :subject-key subject-key}])

(s/defn remove-subject [study-id :- Uuid subject-key :- SubjectKey]
  [:odm-import/remove-subject {:study-id study-id :subject-key subject-key}])

(s/defn insert-study-event [subject-id :- Uuid study-event-oid :- OID]
  [:odm-import/insert-study-event {:subject-id subject-id :study-event-oid study-event-oid}])

(s/defn upsert-study-event [subject-id :- Uuid study-event-oid :- OID]
  [:odm-import/upsert-study-event {:subject-id subject-id :study-event-oid study-event-oid}])

(s/defn remove-study-event [subject-id :- Uuid study-event-oid :- OID]
  [:odm-import/remove-study-event {:subject-id subject-id :study-event-oid study-event-oid}])

(s/defn insert-form [study-event-id :- Uuid form-oid :- OID]
  [:odm-import/insert-form {:study-event-id study-event-id :form-oid form-oid}])

(s/defn remove-form [study-event-id :- Uuid form-oid :- OID]
  [:odm-import/remove-form {:study-event-id study-event-id :form-oid form-oid}])

(s/defn insert-item-group [form-id :- Uuid item-group-oid :- OID]
  [:odm-import/insert-item-group {:form-id form-id :item-group-oid item-group-oid}])

(s/defn remove-item-group [form-id :- Uuid item-group-oid :- OID]
  [:odm-import/remove-item-group {:form-id form-id :item-group-oid item-group-oid}])

(s/defn insert-item [item-group-id :- Uuid item-oid :- OID data-type :- DataType value]
  [:odm-import/insert-item {:item-group-id item-group-id :item-oid item-oid :data-type data-type :value value}])

(s/defn update-item [item-id :- Uuid data-type :- DataType value]
  [:odm-import/update-item {:item-id item-id :data-type data-type :value value}])

(s/defn remove-item [item-group-id :- Uuid item-oid :- OID]
  [:odm-import/remove-item {:item-group-id item-group-id :item-oid item-oid}])

;; ---- Events ----------------------------------------------------------------

(defn validation-failed [{:keys [id sub]} e]
  (let [name :clinical-data-import/validation-failed]
    {:id (uuid/v5 id name)
     :cid id
     :name name
     :sub sub
     :data {:tag (-> (ex-data e) :element :tag)
            :error (pr-str (:error (ex-data e)))
            :value (:value (ex-data e))}}))

;; ---- Other -----------------------------------------------------------------

(defn- pipeline
  "Runs pipeline with async function af with elements from coll."
  [to af coll]
  (let [ch (chan)]
    (async/pipeline-async 32 to af ch)
    (async/onto-chan ch coll)))

(defn- tx-type-dispatch
  "Transaction type dispatch.

  Tries the current element first than the parent element and defaults to
  :insert for snapshot files."
  ([_ _ [_ {:keys [tx-type]}] _]
   (or tx-type :insert))
  ([_ _ parent-tx-type [_ {:keys [tx-type]}] _]
   (or tx-type parent-tx-type)))

(defmulti handle-item-data
  {:arglists '([send-command item-group-id item-group-tx-type [item-oid item] event-ch])}
  tx-type-dispatch)

(defmethod handle-item-data :insert
  [send-command item-group-id _ [item-oid {:keys [data-type value]}] event-ch]
  (let [ch (send-command (insert-item item-group-id item-oid data-type value))]
    (async/pipe ch event-ch)))

(defmethod handle-item-data :update
  [send-command item-group-id _ [item-oid {:keys [data-type value]}] event-ch]
  (let [ch (send-command (update-item (uuid/v5 item-group-id item-oid) data-type value))]
    (async/pipe ch event-ch)))

(defmethod handle-item-data :remove
  [send-command item-group-id _ [item-oid] event-ch]
  (let [ch (send-command (remove-item item-group-id item-oid))]
    (async/pipe ch event-ch)))

(defmulti handle-item-group-data
  {:arglists '([send-command form-id form-tx-type [item-group-oid item-group] event-ch])}
  tx-type-dispatch)

(defmethod handle-item-group-data :insert
  [send-command form-id _ [item-group-oid {:keys [items]}] event-ch]
  (let [ch (send-command (insert-item-group form-id item-group-oid))
        item-group-id (uuid/v5 form-id item-group-oid)
        af (partial handle-item-data send-command item-group-id :insert)]
    (go
      (if-let [event (<! ch)]
        (do
          (>! event-ch event)
          (if (= :item-group/created (:name event))
            (pipeline event-ch af items)
            (async/close! event-ch)))
        (async/close! event-ch)))))

(defmethod handle-item-group-data :update
  [send-command form-id _ [item-group-oid {:keys [items]}] event-ch]
  (let [item-group-id (uuid/v5 form-id item-group-oid)
        af (partial handle-item-data send-command item-group-id :update)]
    (pipeline event-ch af items)))

(defmethod handle-item-group-data :remove
  [send-command form-id _ [item-group-oid] event-ch]
  (let [ch (send-command (remove-item-group form-id item-group-oid))]
    (async/pipe ch event-ch)))

(defmulti handle-form-data
  {:arglists '([send-command study-event-id study-event-tx-type [form-oid form] event-ch])}
  tx-type-dispatch)

(defmethod handle-form-data :insert
  [send-command study-event-id _ [form-oid {:keys [item-groups]}] event-ch]
  (let [ch (send-command (insert-form study-event-id form-oid))
        form-id (uuid/v5 study-event-id form-oid)
        af (partial handle-item-group-data send-command form-id :insert)]
    (go
      (if-let [event (<! ch)]
        (do
          (>! event-ch event)
          (if (= :form/created (:name event))
            (pipeline event-ch af item-groups)
            (async/close! event-ch)))
        (async/close! event-ch)))))

(defmethod handle-form-data :update
  [send-command study-event-id _ [form-oid {:keys [item-groups]}] event-ch]
  (let [form-id (uuid/v5 study-event-id form-oid)
        af (partial handle-item-group-data send-command form-id :update)]
    (pipeline event-ch af item-groups)))

(defmethod handle-form-data :remove
  [send-command study-event-id _ [form-oid] event-ch]
  (let [ch (send-command (remove-form study-event-id form-oid))]
    (async/pipe ch event-ch)))

(defmulti handle-study-event-data
  {:arglists '([send-command subject-id subject-tx-type [subject-key subject] event-ch])}
  tx-type-dispatch)

(defmethod handle-study-event-data :insert
  [send-command subject-id _ [study-event-oid {:keys [forms]}] event-ch]
  (let [ch (send-command (insert-study-event subject-id study-event-oid))
        form-id (uuid/v5 subject-id study-event-oid)
        af (partial handle-form-data send-command form-id :insert)]
    (go
      (if-let [event (<! ch)]
        (do
          (>! event-ch event)
          (if (= :study-event/created (:name event))
            (pipeline event-ch af forms)
            (async/close! event-ch)))
        (async/close! event-ch)))))

(defmethod handle-study-event-data :upsert
  [send-command subject-id _ [study-event-oid {:keys [forms]}] event-ch]
  (let [ch (send-command (upsert-study-event subject-id study-event-oid))
        form-id (uuid/v5 subject-id study-event-oid)
        af (partial handle-form-data send-command form-id :upsert)]
    (go
      (if-let [event (<! ch)]
        (do
          (>! event-ch event)
          (if (#{:study-event/created :study-event/updated} (:name event))
            (pipeline event-ch af forms)
            (async/close! event-ch)))
        (async/close! event-ch)))))

(defmethod handle-study-event-data :update
  [send-command subject-id _ [study-event-oid {:keys [forms]}] event-ch]
  (let [form-id (uuid/v5 subject-id study-event-oid)
        af (partial handle-form-data send-command form-id :update)]
    (pipeline event-ch af forms)))

(defmethod handle-study-event-data :remove
  [send-command subject-id _ [study-event-oid] event-ch]
  (let [ch (send-command (remove-study-event subject-id study-event-oid))]
    (async/pipe ch event-ch)))

(defmulti handle-subject-data
  {:arglists '([send-command study-id [subject-key subject] event-ch])}
  tx-type-dispatch)

(defmethod handle-subject-data :insert
  [send-command study-id [subject-key {:keys [study-events]}] event-ch]
  (let [ch (send-command (insert-subject study-id subject-key))
        subject-id (uuid/v5 study-id subject-key)
        af (partial handle-study-event-data send-command subject-id :insert)]
    (go
      (if-let [event (<! ch)]
        (do
          (>! event-ch event)
          (if (= :subject/created (:name event))
            (pipeline event-ch af study-events)
            (async/close! event-ch)))
        (async/close! event-ch)))))

(defmethod handle-subject-data :upsert
  [send-command study-id [subject-key {:keys [study-events]}] event-ch]
  (let [ch (send-command (upsert-subject study-id subject-key))
        subject-id (uuid/v5 study-id subject-key)
        af (partial handle-study-event-data send-command subject-id :upsert)]
    (go
      (if-let [event (<! ch)]
        (do
          (>! event-ch event)
          (if (#{:subject/created :subject/updated} (:name event))
            (pipeline event-ch af study-events)
            (async/close! event-ch)))
        (async/close! event-ch)))))

(defmethod handle-subject-data :update
  [send-command study-id [subject-key {:keys [study-events]}] event-ch]
  (let [subject-id (uuid/v5 study-id subject-key)
        af (partial handle-study-event-data send-command subject-id :update)]
    (pipeline event-ch af study-events)))

(defmethod handle-subject-data :remove
  [send-command study-id [subject-key] event-ch]
  (let [ch (send-command (remove-subject study-id subject-key))]
    (async/pipe ch event-ch)))

(s/defn handle-clinical-data
  [send-command [study-oid {:keys [subjects]}] event-ch]
  (let [study-id (uuid/v5 uuid/+null+ study-oid)
        af (partial handle-subject-data send-command study-id)]
    (pipeline event-ch af subjects)))

(defn- assoc-file-oid [send-command file-oid]
  (fn [[name params]]
    (send-command [name (assoc params :file-oid file-oid)])))

(s/defn handle-odm-file
  [send-command {:keys [file-oid] :as odm-file} :- ODMFile]
  (let [event-ch (chan)
        send-command (assoc-file-oid send-command file-oid)
        af (partial handle-clinical-data send-command)]
    (pipeline event-ch af (:clinical-data odm-file))
    (<!! (go-loop []
           (when-let [event (<! event-ch)]
             (if (= :error (:name event))
               (error {:event event})
               (debug {:event event}))
             (recur))))))

(defn- command [name sub params]
  {:id (uuid/squuid)
   :name name
   :sub sub
   :params params})

(defn send-command-fn [broker sub]
  (s/fn [[name params] :- Cmd]
    (b/send-command broker (command name sub params))))

(s/defn read-and-parse-file :- ODMFile [file :- File]
  (let [start (System/nanoTime)
        data (-> (fs/read-file file)
                 (xml/parse)
                 (p/parse-odm-file))]
    (info (format "Finished reading and parsing the file in %.1f s."
                  (/ (u/duration start) 1000)))
    data))

(defmethod handle-command :import-clinical-data
  [{:keys [broker]} {:keys [sub] :as cmd} {:keys [file]}]
  (s/validate File file)
  (info "Start import of clinical data...")
  (let [start (System/nanoTime)]
    (try
      (->> (read-and-parse-file file)
           (handle-odm-file (send-command-fn broker sub)))
      (catch Exception e
        (case (:type (ex-data e))
          ::p/validation-error
          (do (error (.getMessage e))
              (send-event broker (validation-failed cmd e)))
          (throw e))))
    (info (format "Finished import of clinical data in %.1f s."
                  (/ (u/duration start) 1000)))))
