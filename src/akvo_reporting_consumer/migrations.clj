(ns akvo-reporting-consumer.migrations
  (:require [clojure.java.jdbc :as jdbc]
            [ragtime.jdbc]
            [ragtime.repl]
            [taoensso.timbre :as log]))

(defn database-exists? [conn db-name]
  (not (empty? (jdbc/query conn ["SELECT 1 from pg_database WHERE datname=?" db-name]))))

(defn ensure-instance-db-exists [conn org-id]
  (when-not (database-exists? conn org-id)
    (log/infof "Creating instance database %s" org-id)
    (jdbc/execute! conn
                   [(format "CREATE DATABASE \"%s\" WITH TEMPLATE template0 ENCODING 'UTF8'" org-id)]
                   :transaction? false)))

(defn connection-string [uri-format db user password]
  (cond-> (format uri-format db)
    (not (empty? user)) (str "?user=" user)
    (not (empty? password)) (str "&password=" password)))

(defn migrate-instance-db [{:keys [org-id reporting-jdbc-uri reporting-user reporting-password] :as config}]
  (let [master-conn (connection-string reporting-jdbc-uri
                                       "akvo-reporting"
                                       reporting-user
                                       reporting-password)]
    (ensure-instance-db-exists master-conn org-id))

  (let [instance-conn (connection-string reporting-jdbc-uri
                                         org-id
                                         reporting-user
                                         reporting-password)]
    (ragtime.repl/migrate {:datastore (ragtime.jdbc/sql-database instance-conn)
                           :migrations (ragtime.jdbc/load-resources "migrations")})))
