cd src
mvn clean package
# java -cp "$(find . | grep .jar | awk '{print}' ORS=':')" com.yahoo.ycsb.CommandLine -db com.yahoo.ycsb.db.SQLiteClient -p sqlite.dbname=ycsb.db
./bin/ycsb run sqlite -s -P workloads/gdpr_controller -p sqlite.dbname=ycsb.db
