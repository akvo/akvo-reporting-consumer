(ns akvo-reporting-consumer.pg
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async]
            [cheshire.core :as json]
            [taoensso.timbre :as log])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]))

(set! *warn-on-reflection* true)

(defn get-from [offset]
  {:pre [(integer? offset)]}
  (format "SELECT id, payload::text FROM event_log WHERE id > %s ORDER BY id ASC"
          offset))

(defn ^HikariDataSource datasource
  [{:keys [org-id
           event-log-jdbc-uri
           event-log-user
           event-log-password]}]
  (let [config (doto (HikariConfig.)
                 (.setJdbcUrl (format event-log-jdbc-uri org-id))
                 (.setUsername event-log-user)
                 (.setPassword event-log-password)
                 (.setAutoCommit false)
                 (.setReadOnly true)
                 (.setMaximumPoolSize 1))]
    (HikariDataSource. config)))

(defn event-chan
  [config chan offset]
  {:pre [(integer? offset)]}
  (log/infof "Start event listening for %s" (:org-id config))
  (let [close-chan (async/chan)
        ds (datasource config)
        last-offset (atom offset)]
    (async/thread
      (loop []
        (try
          (with-open [conn (jdbc/get-connection {:datasource ds})
                      stmt (.createStatement conn)]
            (.setFetchSize stmt 1000)
            (with-open [result-set (.executeQuery stmt (get-from @last-offset))]
              (while (.next result-set)
                (let [offset (.getLong result-set 1)
                      payload (json/parse-string (.getString result-set 2))]
                  (when (async/>!! chan {:offset offset :payload payload})
                    (reset! last-offset offset))))))
          (catch Exception e
            (log/errorf e "Error while fething events for %s" (:org-id config))))
        (async/alt!!
          (async/timeout 1000) (recur)
          close-chan nil)))
    (fn []
      (log/infof "Shutting down event listening for %s" (:org-id config))
      (async/close! close-chan)
      (.close ds))))
