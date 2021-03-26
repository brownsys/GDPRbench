#!/bin/bash
mvn package

rm -rf gdprbench.db

#mvn clean package
./bin/ycsb load sqlite -s -P workloads/gdpr_all -p sqlite.dbname=:memory:
./bin/ycsb run sqlite -s -P workloads/gdpr_all -p sqlite.dbname=:memory:

# Old generic JDBC adapter
#java -cp "$(find . | grep .jar | awk '{print}' ORS=':')" com.yahoo.ycsb.db.JdbcDBCreateTable -p db.driver=org.sqlite.JDBC -p db.url=jdbc:sqlite:gdprbench.db -n usertable -p db.user=
#./bin/ycsb -cp "$(find . | grep .jar | awk '{print}' ORS=':')" load jdbc -s -P workloads/gdpr_controller -p db.driver=org.sqlite.JDBC -p db.url=jdbc:sqlite:gdprbench.db
#./bin/ycsb -cp "$(find . | grep .jar | awk '{print}' ORS=':')" run jdbc -s -P workloads/gdpr_controller -p db.driver=org.sqlite.JDBC -p db.url=jdbc:sqlite:gdprbench.db
