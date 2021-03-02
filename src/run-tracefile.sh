#!/bin/bash
mvn package

if [ $1 = "sqlite" ]
then
  ./bin/ycsb load tracefile -s -P workloads/gdpr_all -p file.path=traces/sqlite.sql -p file.append=no -p file.shard=no
  ./bin/ycsb run tracefile -s -P workloads/gdpr_all -p file.path=traces/sqlite.sql -p file.append=yes -p file.shard=no
else
  ./bin/ycsb load tracefile -s -P workloads/gdpr_all -p file.path=traces/pelton.sql -p file.append=no -p file.shard=yes
  ./bin/ycsb run tracefile -s -P workloads/gdpr_all -p file.path=traces/pelton.sql -p file.append=yes -p file.shard=yes
fi
