#!/bin/bash
mvn package

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo ${DIR}
rm -rf gdprbench.db

#mvn clean package
./bin/ycsb -jvm-args="-Djava.library.path=${DIR}/../../../bazel-bin/jni" load pelton -s -P workloads/gdpr_controller -p sqlite.dbname=gdprbench.db
./bin/ycsb -jvm-args="-Djava.library.path=${DIR}/../../../bazel-bin/jni" run pelton -s -P workloads/gdpr_controller -p sqlite.dbname=gdprbench.db
