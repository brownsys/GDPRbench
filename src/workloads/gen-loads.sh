#!/bin/bash
INPUT=$1
OUTPUT=$2
USRCOUNT=$3
RECORDCOUNT=$4
OPCOUNT=$5

cp $INPUT $OUTPUT
sed -i "s+usrcount=[0-9]*+usrcount=$USRCOUNT+g" $OUTPUT
sed -i "s+operationcount=[0-9]*+operationcount=$OPCOUNT+g" $OUTPUT
sed -i "s+recordcount=[0-9]*+recordcount=$RECORDCOUNT+g" $OUTPUT
