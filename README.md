# akvo-reporting-consumer

## Running locally


* Clone `https://github.com/akvo/akvo-config` to `/tmp/repos` or to a location
specified by by the `AKVO_REPORTING_REPOS_DIR` enviroment variable).
* Edit the file `akvo-config/services/reporting/test.edn` to make it match

```clojure
{
 :event-log-jdbc-uri "jdbc:postgresql://localhost:5433/%s"
 :event-log-user "unilog"
 :event-log-password "<unilog-db-password>"

 :reporting-jdbc-uri "jdbc:postgresql://localhost:5432/%s"
 :reporting-user "postgres"
 :reporting-password ""

 :port 3131

 ;; Map of instances to instance specific configs
 :instances {"akvoflow-uat1" nil
             ;; and possibly other instances
             }}
```

* Open an ssh tunnel to the unilog test instance:
``` sh
ssh -nNT -L 5433:unilog.test.akvo.org:5432 <you>@unilog.akvo.org
```
* You need a local postgres instance running on port 5432
* Create the reporting db:
``` sh
createdb akvoflow-uat1
```
* Generate the tables:
``` sh
psql -d akvoflow-uat1 -f resources/tables.sql
```
* Run the consumer with
``` sh
AKVO_REPORTING_DEV_MODE=true lein run
```


Copyright © 2015 Akvo Foundation
