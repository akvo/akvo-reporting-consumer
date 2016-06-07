(defproject akvo-reporting-consumer "0.1.0-SNAPSHOT"
  :description "Akvo FLOW reporting consumer"
  :url "https://github.com/akvo/akvo-reporting-consumer"
  :license {:name "GNU Affero General Public License"
            :url "http://www.gnu.org/licenses/agpl-3.0.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jdbc "0.4.2"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/tools.nrepl "0.2.12"]
                 [org.akvo/commons "0.4.2"]
                 [org.postgresql/postgresql "9.4.1207.jre7"]
                 [ragtime "0.5.3"]
                 [ring/ring-core "1.4.0" :exclusions [org.clojure/tools.reader]]
                 [ring/ring-json "0.4.0"]
                 [ring/ring-jetty-adapter "1.4.0"]
                 [com.taoensso/timbre "4.2.0" :exclusions [org.clojure/tools.reader]]
                 [compojure "1.4.0"]
                 [cheshire "5.5.0"]
                 [environ "1.0.2"]
                 [com.zaxxer/HikariCP "2.4.3"]]
  :aot [akvo-reporting-consumer.core]
  :main akvo-reporting-consumer.core
  :uberjar-name "reporting-consumer.jar")
