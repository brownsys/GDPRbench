#!/bin/bash
# Usage: ./run-tracefile <suffix>
#        such that workloads/pelton_<suffix> is a load config file.
# Outputs: two trace files for pelton and vanilla MySQL respectively at:
#          traces/pelton_<suffix>.sql and traces/mysql_<suffix>.sql

echo "Don't forget to run 'mvn package' after changes!"

if [[ "$1" == "" ]]; then
  ./run-tracefile.sh all
  ./run-tracefile.sh select_pk
  ./run-tracefile.sh select_pur
  ./run-tracefile.sh select_usr
  ./run-tracefile.sh scan
  ./run-tracefile.sh insert
  ./run-tracefile.sh update_pk
  ./run-tracefile.sh update_pur
  ./run-tracefile.sh update_usr
  ./run-tracefile.sh delete_pk
  ./run-tracefile.sh delete_pur
  ./run-tracefile.sh delete_usr
else
  LOAD_CONF="workloads/pelton_$1"
  SFILE_NAME="traces/pelton_$1.sql"
  UFILE_NAME="traces/mysql_$1.sql"


  ./bin/ycsb load tracefile -s -P $LOAD_CONF -p sharded.path=$SFILE_NAME \
                            -p unsharded.path=$UFILE_NAME -p file.append=no

  ./bin/ycsb run tracefile -s -P $LOAD_CONF -p sharded.path=$SFILE_NAME \
                           -p unsharded.path=$UFILE_NAME -p file.append=yes
fi
