#!/bin/bash

# date: 2015-08-26
# author: cwj

# server 242


source /etc/profile

bash /root/data-integration/kitchen.sh -file=/Job/VIACOM/SiteDetail_Yearly/SDY_Link_Files/SiteDetail_Yearly_alter_linking.kjb

mysql_conf_123="-h54.67.114.123 -ukettle -pkettle DM_VIACOM"
mysql_conf_114="-h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM"

jobName=SiteDetail_Yearly_alter_linking
endDate=`date -d 'now' +%Y-%m-%d`

cd /Job/VIACOM/log
mysql $mysql_conf_114 -e "select * from KLog where JOBNAME = '$jobName' and date_format(ENDDATE, '%Y-%m-%d') = '$endDate' order by ENDDATE desc limit 1\G" > "$jobName".log

if [ -n "$(grep -i error $jobName'.log'| grep -v 'ERRORS: 0')" ]
  then
        cat "$jobName".log | mail -s "viacom ETL ERROR "$jobName$endDate chen_weijie@vobile.cn
  else
        echo "ETL Job "$jobName" is OK. Well Done."| mail -s "viacom ETL OK "$jobName$endDate chen_weijie@vobile.cn
  fi

