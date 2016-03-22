(ns akvo-reporting-consumer.core
  (:gen-class)
  (:require [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.pprint :as pp]
            [clojure.core.async :as async]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.shell :as shell]
            [akvo.commons.git :as git]
            [akvo-reporting-consumer.pg :as pg]
            [akvo-reporting-consumer.consumer :as consumer]
            [compojure.core :refer (defroutes GET POST routes)]
            [ring.adapter.jetty :as jetty]
            [taoensso.timbre :as log]
            [environ.core :refer (env)])
  (:import [org.postgresql.util PSQLException]))

(defonce dev? (some? (:akvo-reporting-dev-mode env)))

(defn get-config [akvo-config-path config-filename]
  (let [config (-> (str akvo-config-path "/services/reporting/" config-filename)
                   slurp
                   edn/read-string
                   (assoc :akvo-config-path akvo-config-path
                          :config-filename config-filename))
        instances (reduce (fn [result org-id]
                            (update-in result
                                       [org-id]
                                       merge
                                       (select-keys config [:event-log-jdbc-uri
                                                            :event-log-user
                                                            :event-log-password
                                                            :reporting-jdbc-uri
                                                            :reporting-user
                                                            :reporting-password])
                                       (get-in config [:instances org-id])
                                       {:org-id org-id}))
                          {}
                          (keys (:instances config)))]
    (assoc config :instances instances)))

(defn start-consumer [consumer-config]
  (let [consumer (consumer/consumer consumer-config)]
    (consumer/start consumer)
    consumer))

(defn reload-config [current-config running-consumers]
  (let [previous-config @current-config
        repos-dir (:akvo-config-path previous-config)
        next-config (do (when-not dev? (git/pull repos-dir "akvo-config"))
                        (get-config repos-dir (:config-filename previous-config)))
        previous-instances (-> previous-config :instances keys set)
        next-instances (-> next-config :instances keys set)
        new-instances (set/difference next-instances previous-instances)
        removed-instances (set/difference previous-instances next-instances)]
    (doseq [org-id new-instances]
      (swap! running-consumers
             assoc
             org-id
             (start-consumer (get-in next-config [:instances org-id]))))
    (doseq [org-id removed-instances]
      (let [consumer (get @running-consumers org-id)]
        (consumer/stop consumer)
        (swap! running-consumers dissoc org-id)))
    (reset! current-config next-config)))

(defn run-consumers [config]
  (reduce (fn [result org-id]
            (assoc result org-id (start-consumer (get-in config [:instances org-id]))))
          {}
          (keys (:instances config))))

;; Note: both config and running consumers are atoms. They can be updated at runtime
(defn app [config running-consumers]
  (routes
   (GET "/" [] "ok")

   (GET "/config" []
     (format "<pre>%s</pre>"
             (str/escape (with-out-str (pp/pprint @config)) {\< "&lt;" \> "&gt;"})))

   (GET "/offset/:org-id" [org-id]
     (if-let [consumer (get @running-consumers org-id)]
       (format "Offset for consumer %s is currently %s" org-id (consumer/offset consumer))
       (format "Reporting consumer %s is not currently running" org-id)))

   (POST "/restart/:org-id" [org-id]
     (let [current-consumers @running-consumers]
       (if-let [consumer (get current-consumers org-id)]
         (do (consumer/restart consumer)
             (format "Reporting consumer %s restarted" org-id))
         (format "Reporting consumer %s is not currently running" org-id))))

   (POST "/reload-config" []
     (reload-config config running-consumers)
     "ok")))

(defn -main []
  (let [clone-url (format "https://%s@github.com/akvo/akvo-config"
                          (:github-oauth-token env))
        repos-dir (:akvo-reporting-repos-dir env "/tmp/repos")
        config-filename (:akvo-reporting-config-filename env "test.edn")]
    (git/ensure-directory repos-dir)
    (when-not dev? (git/clone-or-pull repos-dir clone-url))
    (let [config (get-config (str repos-dir "/akvo-config")
                             config-filename)
          running-consumers (run-consumers config)]
      (log/merge-config! {:level (:log-level config :info)
                          :output-fn (partial log/default-output-fn {:stacktrace-fonts {}})})
      (let [port (Integer. (:port config 3030))]
        (jetty/run-jetty (app (atom config) (atom running-consumers))
                         {:port port
                          :join? false})))))
