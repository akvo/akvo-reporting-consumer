(ns akvo-reporting-consumer.consumer
  (:require [akvo-reporting-consumer.pg :as pg]
            [clojure.string :as string]
            [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async]
            [cheshire.core :as json]
            [taoensso.timbre :as log])
  (:import [org.postgresql.util PSQLException]
           [com.fasterxml.jackson.core JsonParseException]
           [java.sql.Date]
           [com.zaxxer.hikari HikariConfig HikariDataSource]))

(defprotocol IConsumer
  (-start [consumer])
  (-stop [consumer])
  (-reset [consumer])
  (-offset [consumer]))

(defn start
  "Start the consumer"
  [consumer]
  (-start consumer))


(defn stop
  "Stop the consumer"
  [consumer]
  (-stop consumer))

(defn offset [consumer]
  (-offset consumer))

(defn restart [consumer]
  (stop consumer)
  (-reset consumer)
  (start consumer))

(def event-type
  {"surveyGroupCreated" :survey
   "surveyGroupUpdated" :survey
   "surveyGroupDeleted" :delete
   "formCreated" :form
   "formUpdated" :form
   "formDeleted" :delete
   "questionGroupCreated" :question-group
   "questionGroupUpdated" :question-group
   "questionGroupDeleted" :delete
   "questionCreated" :question
   "questionUpdated" :question
   "questionDeleted" :delete
   "dataPointCreated" :data-point
   "dataPointUpdated" :data-point
   "dataPointDeleted" :delete
   "formInstanceCreated" :form-instance
   "formInstanceUpdated" :form-instance
   "formInstanceDeleted" :delete
   "answerCreated" :answer
   "answerUpdated" :answer
   "answerDeleted" :delete
   "deviceFileCreated" :skip
   "deviceFileUpdated" :skip
   "deviceFileDeleted" :skip})

(def queries
  {:survey
   {:insert "insert into survey
               (display_text, description, id) values
               (?, ?, ?)"
    :update "update survey set
               display_text=?,
               description=?,
               updated_at=now()
             where id=?"}
   :form
   {:insert "insert into form
               (survey_id, display_text, description, id) values
               (?, ?, ?, ?)"
    :update "update form set
               survey_id=?,
               display_text=?,
               description=?,
               updated_at=now()
             where id=?"}
   :question-group
   {:insert "insert into question_group
               (form_id, display_order, repeatable, display_text, id) values
               (?, ?, ?, ?, ?)"
    :update "update question_group set
               form_id=?,
               display_order=?,
               repeatable=?,
               display_text=?,
               updated_at=now()
             where id=?"}
   :question
   {:insert "insert into question
               (question_group_id, type, display_order, display_text, identifier, id) values
               (?, ?, ?, ?, ?, ?)"
    :update "update question set
               question_group_id=?,
               type=?,
               display_order=?,
               display_text=?,
               identifier=?,
               updated_at=now()
             where id=?"}
   :data-point
   {:insert "insert into data_point
               (survey_id, identifier, longitude, latitude, id) values
               (?, ?, ?, ?, ?)"
    :update "update data_point set
               survey_id=?,
               identifier=?,
               longitude=?,
               latitude=?,
               updated_at=now()
             where id=?"}
   :form-instance
   {:insert "insert into form_instance
               (form_id, data_point_id, submitter, submitted_at, id) values
               (?, ?, ?, ?, ?)"
    :update "update form_instance set
               form_id=?,
               data_point_id=?,
               submitter=?,
               submitted_at=?,
               updated_at=now()
             where id=?"}
   :answer
   {:insert "insert into response
               (form_instance_id, question_id, \"value\", iteration, id) values
               (?, ?, ?::jsonb, ?, ?)"
    :update "update response set
               form_instance_id=?,
               question_id=?,
               \"value\"=?::jsonb,
               iteration=?,
               updated_at=now()
             where id=?"}})

;; Note: This will not work in a transaction. There are also other
;; approaches to do upsert pre 9.5
(defn upsert [conn type params]
  (let [{:keys [insert update]}  (get queries type)]
    (try
      (jdbc/execute! conn (into [insert] params))
      (catch Exception e
        (jdbc/execute! conn (into [update] params))))))

(defmulti handle-event*
  (fn [conn event]
    (event-type (get event "eventType"))))

(defmethod handle-event* :default
  [conn event]
  (println "No event handler for")
  (clojure.pprint/pprint event))

(defmethod handle-event* :skip
  [conn event])

(defmethod handle-event* :delete
  [conn event]
  (when-let [table (condp = (get event "eventType")
                     "surveyGroupDeleted" "survey"
                     "formDeleted" "form"
                     "questionGroupDeleted" "question_group"
                     "questionDeleted" "question"
                     "dataPointDeleted" "data_point"
                     "formInstanceDeleted" "form_instance"
                     "answerDeleted" "response")]
    (jdbc/execute! conn [(format "delete from %s where id=?" table)
                         (get-in event ["entity" "id"])])))

(defmethod handle-event* :survey
  [conn event]
  (let [{:strs [id name description surveyGroupType]} (get event "entity")]
    (when (= surveyGroupType "SURVEY")
      (upsert conn
              :survey
              [name
               description
               id]))))

(defmethod handle-event* :form
  [conn event]
  (let [{:strs [id surveyId displayText description]} (get event "entity")]
    (if surveyId
      (upsert conn
              :form
              [surveyId
               displayText
               description
               id])
      (log/debugf "Missing surveyId for form #%s" id))))

(defmethod handle-event* :question-group
  [conn event]
  (let [{:strs [id name formId order repeatable]} (get event "entity")]
    (if formId
      (upsert conn
              :question-group
              [formId
               order
               repeatable
               name
               id])
      (log/debugf "Missing formId for question group #%s" id))))

(defmethod handle-event* :question
  [conn event]
  (let [{:strs [id questionGroupId questionType order displayText identifier]} (get event "entity")]
    (if questionGroupId
      (upsert conn
              :question
              [questionGroupId
               questionType
               order ;; Note: no order in unilog?
               displayText
               identifier
               id])
      (log/debugf "Missing questionGroupId for question #%s" id))))

;; What about data point name?
(defmethod handle-event* :data-point
  [conn event]
  (let [{:strs [id surveyId identifier lat lon]} (get event "entity")]
    (if surveyId
      (upsert conn
              :data-point
              [surveyId
               identifier
               lat
               lon
               id])
      (log/debugf "Missing surveyId for data point #%s" id))))

;; Submitter not available?
(defmethod handle-event* :form-instance
  [conn event]
  (let [{:strs [id formId dataPointId submitter collectionDate]} (get event "entity")]
    (if (and formId dataPointId)
      (upsert conn
              :form-instance
              [formId
               dataPointId
               submitter
               (when collectionDate (java.sql.Date. collectionDate))
               id])
      (do
        (when-not formId
          (log/debugf "Missing formId for form instance #%s" id))
        (when-not dataPointId
          (log/debugf "Missing dataPointId for form instance #%s" id))))))

(defn parse-cascade-value [value]
  {:type "CASCADE"
   :value (if (string/starts-with? value "[")
            (json/parse-string value)
            (map (fn [name] {:name name})
                 (string/split value #"\|")))})

(defn parse-option-value [value]
  {:type "OPTION"
   :value (if (string/starts-with? value "[")
            (json/parse-string value)
            (map (fn [text] {:text text})
                 (string/split value #"\|")))})

(defn parse-date-value [value]
  {:type "DATE"
   :value (Long/parseLong value)})

(defn parse-geo-value [value]
  {:type "GEO" ;; TODO: unifiy with geoshape?
   :value (let [;; TODO lat/lon or lon/lat? GEOJson is lon/lat/elev
                [lat lon elev code] (string/split value #"\|")]
            {:type "Feature"
             :geometry {:type "Point"
                        :coordinates [(Double/parseDouble lon)
                                      (Double/parseDouble lat)
                                      (Double/parseDouble elev)]}
             :properties {:code code}})})

(defn parse-signature-value [value]
  {:type "SIGNATURE"
   :value (json/parse-string value)})

(defn parse-answer-with-question-type [question-type value]
  (condp = question-type
    "NUMBER" {:type "NUMBER"
              :value (try
                       (Long/parseLong value)
                       (catch NumberFormatException e
                         (Double/parseDouble value)))}
    "OPTION" (parse-option-value value)
    "VIDEO" {:type "VIDEO"
             :value value}
    "PHOTO" {:type "PHOTO"
             :value value}
    "CASCADE" (parse-cascade-value value)
    "DATE" (parse-date-value value)
    "FREE_TEXT" {:type "FREE_TEXT"
                 :value value}
    "GEOSHAPE" {:type "GEOSHAPE"
                :value (json/parse-string value)}
    "SCAN" {:type "SCAN"
            :value value}
    "GEO" (parse-geo-value value)
    "SIGNATURE" (parse-signature-value value)))

;; We try to unify with the question types.
(defn parse-answer-with-answer-type [answer-type value]
  (condp = answer-type
    "CASCADE" (parse-cascade-value value)
    "DATE" (parse-date-value value)
    "IMAGE" {:type "PHOTO" ;; IMAGE is called PHOTO
             :image value}
    "VIDEO" {:type "VIDEO"
             :value value}
    "GEO" (parse-geo-value value)
    "OPTION" (parse-option-value value)
    "SIGNATURE" (parse-signature-value value)

    ;; OTHER & VALUE could be almost anything
    ;; leave empty :type for now.
    "VALUE" {:value value}
    "OTHER" {:value value}))

;; We could cache this value for perf gains
(defn question-type [conn question-id]
  (-> (jdbc/query conn ["select \"type\" from question where id=?" question-id])
      first
      :type))

(defmethod handle-event* :answer
  [conn event]
  (let [{:strs [id formInstanceId questionId answerType value iteration]} (get event "entity")
        value (if value (string/trim value) "")
        question-type (question-type conn questionId)]
    (if (and formInstanceId questionId)
      (upsert conn
              :answer
              [formInstanceId
               questionId
               (json/generate-string
                (try
                  (if question-type
                    (parse-answer-with-question-type question-type value)
                    (parse-answer-with-answer-type answerType value))
                  (catch Exception e
                    {:value value})))
               iteration
               id])
      (do
        (when-not formInstanceId
          (log/debugf "Missing formInstanceId from answer #%s" id))
        (when-not questionId
          (log/debugf "Missing questionId from answer #%s" id))))))

(defn handle-event [db-spec offset event]
  (jdbc/with-db-connection [conn db-spec]
    (handle-event* conn event)
    (jdbc/execute! conn ["UPDATE consumer_offset SET \"offset\"=?" offset])))

(defn ^HikariDataSource datasource
  [{:keys [org-id
           reporting-jdbc-uri
           reporting-user
           reporting-password]}]
  (let [config (doto (HikariConfig.)
                 (.setJdbcUrl (format reporting-jdbc-uri org-id))
                 (.setUsername reporting-user)
                 (.setPassword reporting-password)
                 (.setMaximumPoolSize 1))]
    (HikariDataSource. config)))

(defn start-reporting-consumer! [ds chan]
  (async/thread
    (loop []
      (when-let [{:keys [payload offset] :as evt} (async/<!! chan)]
        (try
          (handle-event {:datasource ds} offset payload)
          (catch Exception e
            (log/errorf e "Failed to handle event %s" (pr-str evt))))
        (recur)))))

(defn get-offset [ds]
  (-> (jdbc/query {:datasource ds} ["SELECT \"offset\" from consumer_offset"])
      first
      :offset))

(defn reset-data [ds]
  (doseq [table ["survey" "form" "question_group" "question" "data_point" "form_instance" "response"]]
    (jdbc/execute! {:datasource ds} [(format "DELETE FROM %s" table)]))
  (jdbc/execute! {:datasource ds} ["UPDATE consumer_offset SET \"offset\"=0"]))

(defn consumer [config]
  (let [state (atom {:chan nil
                     :close-fn nil
                     :datasource nil
                     :running? false})]
    (reify IConsumer
      (-start [this]
        (when (:running? @state)
          (throw (ex-info "Consumer already running" config)))
        (log/infof "Starting consumer for %s" (:org-id config))
        (let [ds (datasource config)
              offset (get-offset ds)
              chan (async/chan)]
          (start-reporting-consumer! ds chan)
          (reset! state {:chan chan
                         :datasource ds
                         :running? true
                         :close-fn (pg/event-chan config chan offset)})))
      (-stop [this]
        (log/infof "Stopping consumer for %s" (:org-id config))
        (let [{:keys [close-fn chan datasource running?]} @state]
          (when-not running?
            (throw (ex-info "Consumer not running" config)))
          (close-fn)
          (.close datasource)
          (async/close! chan)
          (reset! state {:chan nil
                         :close-fn nil
                         :datasource nil
                         :running? false})))

      (-offset [this]
        (with-open [ds (datasource config)]
          (get-offset ds)))

      (-reset [this]
        (with-open [ds (datasource config)]
          (reset-data ds))))))
