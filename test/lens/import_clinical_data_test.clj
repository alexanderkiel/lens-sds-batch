(ns lens.import-clinical-data-test
  (:require [clj-uuid :as uuid]
            [clojure.core.async :refer [<!! chan go]]
            [clojure.test :refer :all]
            [lens.import-clinical-data :refer :all]))

(deftest handle-subject-data-test

  (testing "insert"
    (let [event-ch (chan)
          study-id (uuid/v1)
          subject {:tx-type :insert}
          send-command
          (fn [cmd]
            (is (= cmd [:odm-import/insert-subject {:study-id study-id :subject-key "SK01"}]))
            (go ::event))]
      (handle-subject-data send-command study-id ["SK01" subject] event-ch)
      (is (= (<!! event-ch) ::event))
      (is (nil? (<!! event-ch)))))

  (testing "insert cascades to study events"
    (let [event-ch (chan)
          subject
          {:tx-type :insert
           :study-events
           {"SE01" {}}}
          send-command
          (fn [[name]]
            (if (= name :odm-import/insert-subject)
              (go {:name :subject/created})
              (go ::event)))]
      (handle-subject-data send-command (uuid/v1) ["SK01" subject] event-ch)
      (is (= (<!! event-ch) {:name :subject/created}))
      (is (= (<!! event-ch) ::event))
      (is (nil? (<!! event-ch)))))

  (testing "failed insert skips study events"
    (let [event-ch (chan)
          subject
          {:tx-type :insert
           :study-events
           {"SE01" {}}}
          send-command
          (fn [[name]]
            (if (= name :odm-import/insert-subject)
              (go {:name :error})
              (Exception. "Unexpected command.")))]
      (handle-subject-data send-command (uuid/v1) ["SK01" subject] event-ch)
      (is (= (<!! event-ch) {:name :error}))
      (is (nil? (<!! event-ch)))))

  (testing "update just cascades to study events without issuing a command"
    (let [event-ch (chan)
          subject
          {:tx-type :update
           :study-events
           {"SE01" {:tx-type :insert}}}
          send-command
          (fn [[name]]
            (if (= name :odm-import/insert-study-event)
              (go {:name :study-event/created})
              (Exception. "Unexpected command.")))]
      (handle-subject-data send-command (uuid/v1) ["SK01" subject] event-ch)
      (is (= (<!! event-ch) {:name :study-event/created}))
      (is (nil? (<!! event-ch))))))

(deftest handle-study-event-data-test

  (testing "insert"
    (let [event-ch (chan)
          subject-id (uuid/v1)
          study-event {:tx-type :insert}
          send-command
          (fn [cmd]
            (is (= cmd [:odm-import/insert-study-event {:subject-id subject-id :study-event-oid "SE01"}]))
            (go ::event))]
      (handle-study-event-data send-command subject-id nil ["SE01" study-event] event-ch)
      (is (= (<!! event-ch) ::event))
      (is (nil? (<!! event-ch)))))

  (testing "insert cascades to forms"
    (let [event-ch (chan)
          subject-id (uuid/v1)
          study-event {:tx-type :insert :forms {"F1" {}}}
          send-command
          (fn [[name]]
            (if (= name :odm-import/insert-study-event)
              (go {:name :study-event/created})
              (go ::event)))]
      (handle-study-event-data send-command subject-id nil ["SE01" study-event] event-ch)
      (is (= (<!! event-ch) {:name :study-event/created}))
      (is (= (<!! event-ch) ::event))
      (is (nil? (<!! event-ch)))))

  (testing "update just cascades to forms without issuing a command"
    (let [event-ch (chan)
          subject-id (uuid/v1)
          study-event {:tx-type :update :forms {"F1" {:tx-type :insert}}}
          send-command
          (fn [[name]]
            (if (= name :odm-import/insert-form)
              (go {:name :form/created})
              (Exception. "Unexpected command.")))]
      (handle-study-event-data send-command subject-id nil ["SE01" study-event] event-ch)
      (is (= (<!! event-ch) {:name :form/created}))
      (is (nil? (<!! event-ch))))))

(deftest handle-form-data-test

  (testing "insert"
    (let [event-ch (chan)
          study-event-id (uuid/v1)
          form {:tx-type :insert}
          send-command
          (fn [cmd]
            (is (= cmd [:odm-import/insert-form {:study-event-id study-event-id :form-oid "F1"}]))
            (go ::event))]
      (handle-form-data send-command study-event-id nil ["F1" form] event-ch)
      (is (= (<!! event-ch) ::event))
      (is (nil? (<!! event-ch)))))

  (testing "insert cascades to item-groups"
    (let [event-ch (chan)
          study-event-id (uuid/v1)
          form {:tx-type :insert :item-groups {"IG1" {}}}
          send-command
          (fn [[name]]
            (if (= name :odm-import/insert-form)
              (go {:name :form/created})
              (go ::event)))]
      (handle-form-data send-command study-event-id nil ["F1" form] event-ch)
      (is (= (<!! event-ch) {:name :form/created}))
      (is (= (<!! event-ch) ::event))
      (is (nil? (<!! event-ch)))))

  (testing "update just cascades to item-groups without issuing a command"
    (let [event-ch (chan)
          study-event-id (uuid/v1)
          form {:tx-type :update :item-groups {"IG1" {:tx-type :insert}}}
          send-command
          (fn [[name]]
            (if (= name :odm-import/insert-item-group)
              (go {:name :item-group/created})
              (Exception. "Unexpected command.")))]
      (handle-form-data send-command study-event-id nil ["F1" form] event-ch)
      (is (= (<!! event-ch) {:name :item-group/created}))
      (is (nil? (<!! event-ch))))))

(deftest handle-item-group-data-test

  (testing "insert"
    (let [event-ch (chan)
          form-id (uuid/v1)
          item-group {:tx-type :insert}
          send-command
          (fn [cmd]
            (is (= cmd [:odm-import/insert-item-group {:form-id form-id :item-group-oid "IG1"}]))
            (go ::event))]
      (handle-item-group-data send-command form-id nil ["IG1" item-group] event-ch)
      (is (= (<!! event-ch) ::event))
      (is (nil? (<!! event-ch)))))

  (testing "insert cascades to items"
    (let [event-ch (chan)
          form-id (uuid/v1)
          item-group
          {:tx-type :insert :items {"I1" {:data-type :string :value "x"}}}
          send-command
          (fn [[name]]
            (if (= name :odm-import/insert-item-group)
              (go {:name :item-group/created})
              (go ::event)))]
      (handle-item-group-data send-command form-id nil ["IG1" item-group] event-ch)
      (is (= (<!! event-ch) {:name :item-group/created}))
      (is (= (<!! event-ch) ::event))
      (is (nil? (<!! event-ch)))))

  (testing "update just cascades to items without issuing a command"
    (let [event-ch (chan)
          form-id (uuid/v1)
          item-group
          {:tx-type :update
           :items {"I1" {:tx-type :insert :data-type :string :value "x"}}}
          send-command
          (fn [[name]]
            (if (= name :odm-import/insert-item)
              (go {:name :item/created})
              (Exception. "Unexpected command.")))]
      (handle-item-group-data send-command form-id nil ["IG1" item-group] event-ch)
      (is (= (<!! event-ch) {:name :item/created}))
      (is (nil? (<!! event-ch)))))

  (testing "remove"
    (let [event-ch (chan)
          form-id (uuid/v1)
          item-group {:tx-type :remove}
          send-command
          (fn [cmd]
            (is (= cmd [:odm-import/remove-item-group {:form-id form-id :item-group-oid "IG1"}]))
            (go ::event))]
      (handle-item-group-data send-command form-id nil ["IG1" item-group] event-ch)
      (is (= (<!! event-ch) ::event))
      (is (nil? (<!! event-ch)))))

  (testing "remove doesn't cascade"
    (let [event-ch (chan)
          form-id (uuid/v1)
          item-group
          {:tx-type :remove :items {"I1" {:data-type :string :value "x"}}}
          send-command
          (fn [[name]]
            (if (= name :odm-import/remove-item-group)
              (go ::event)
              (Exception. "Unexpected command.")))]
      (handle-item-group-data send-command form-id nil ["IG1" item-group] event-ch)
      (is (= (<!! event-ch) ::event))
      (is (nil? (<!! event-ch))))))

(deftest handle-item-data-test

  (testing "insert"
    (let [event-ch (chan)
          item-group-id (uuid/v1)
          item {:tx-type :insert :data-type :string :value "x"}
          send-command
          (fn [cmd]
            (is (= cmd [:odm-import/insert-item {:item-group-id item-group-id :item-oid "I1" :data-type :string :value "x"}]))
            (go ::event))]
      (handle-item-data send-command item-group-id nil ["I1" item] event-ch)
      (is (= (<!! event-ch) ::event))
      (is (nil? (<!! event-ch)))))

  (testing "update"
    (let [event-ch (chan)
          item-group-id (uuid/v1)
          item-id (uuid/v5 item-group-id "I1")
          item {:tx-type :update :data-type :string :value "x"}
          send-command
          (fn [cmd]
            (is (= cmd [:odm-import/update-item {:item-id item-id :data-type :string :value "x"}]))
            (go ::event))]
      (handle-item-data send-command item-group-id nil ["I1" item] event-ch)
      (is (= (<!! event-ch) ::event))
      (is (nil? (<!! event-ch)))))

  (testing "remove"
    (let [event-ch (chan)
          item-group-id (uuid/v1)
          item {:tx-type :remove}
          send-command
          (fn [cmd]
            (is (= cmd [:odm-import/remove-item {:item-group-id item-group-id :item-oid "I1"}]))
            (go ::event))]
      (handle-item-data send-command item-group-id nil ["I1" item] event-ch)
      (is (= (<!! event-ch) ::event))
      (is (nil? (<!! event-ch))))))
