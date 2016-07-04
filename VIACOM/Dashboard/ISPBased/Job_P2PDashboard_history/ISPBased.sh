#!/bin/bash

#Date: 2016-04-05
#author: duli

source /etc/profile
bash /root/data-integration/kitchen.sh -file=/Job/VIACOM/Dashboard/ISPBased/Job_P2PDashboard_history/Job_Meta_Isp.kjb

echo 'kettle finished, python start!'
python /Job/VIACOM/Dashboard/ISPBased/Job_P2PDashboard_history/ISPBased.py

yesterday=`date -d "1 days ago" +%Y-%m-%d`
title="ISPBased_${yesterday}"
LinesNum=(`mysql -h54.67.114.123 -ukettle -pkettle VIACOM_DASHBOARD -e "select count(1) as '' from ISPBased where dateID = '$yesterday' group by infringingFlag order by infringingFlag"`)
content="ALL RowNum: ${LinesNum[0]}; Infringing RowNum: ${LinesNum[1]};"
echo ${content}
from="ISPBased Monitor"
to="team_reporting@vobile.cn"
echo "${content}"|mail -s "${title}" -t "${to}" -a From:"${from}"
