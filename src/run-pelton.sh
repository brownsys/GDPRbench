#!/bin/bash
mvn package

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo ${DIR}
rm -rf peltondb
mkdir peltondb

#mvn clean package
./bin/ycsb -jvm-args="-Djava.library.path=${DIR}/../../../bazel-bin/jni" load pelton -s -P workloads/gdpr_controller -p pelton.dbdir=peltondb
./bin/ycsb -jvm-args="-Djava.library.path=${DIR}/../../../bazel-bin/jni" run pelton -s -P workloads/gdpr_controller -p pelton.dbdir=peltondb
