#!/bin/bash
mvn package

./bin/ycsb load tracefile -s -P workloads/gdpr_all -p sharded.path=traces/pelton.sql -p unsharded.path=traces/mysql.sql -p file.append=no
./bin/ycsb run tracefile -s -P workloads/gdpr_all -p sharded.path=traces/pelton.sql -p unsharded.path=traces/mysql.sql -p file.append=yes
