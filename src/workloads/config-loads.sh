#!/bin/bash
USRCOUNT=1000
INSERTOPCOUNT=1000
INSERTDBSIZE=1000
SELECTOPCOUNT=10000
SELECTDBSIZE=1000
UPDATEOPCOUNT=1000
UPDATEDBSIZE=1000
DELETEOPCOUNT=1000
DELETEDBSIZE=1000

sed -i "s+usrcount=[0-9]*+usrcount=$USRCOUNT+g" pelton_*

sed -i "s+operationcount=[0-9]*+operationcount=$INSERTOPCOUNT+g" pelton_insert*
sed -i "s+recordcount=[0-9]*+recordcount=$INSERTDBSIZE+g" pelton_insert*

sed -i "s+operationcount=[0-9]*+operationcount=$SELECTOPCOUNT+g" pelton_select*
sed -i "s+recordcount=[0-9]*+recordcount=$SELECTDBSIZE+g" pelton_select*

sed -i "s+operationcount=[0-9]*+operationcount=$UPDATEOPCOUNT+g" pelton_update*
sed -i "s+recordcount=[0-9]*+recordcount=$UPDATEDBSIZE+g" pelton_update*

sed -i "s+operationcount=[0-9]*+operationcount=$DELETEOPCOUNT+g" pelton_delete*
sed -i "s+recordcount=[0-9]*+recordcount=$DELETEDBSIZE+g" pelton_delete*
