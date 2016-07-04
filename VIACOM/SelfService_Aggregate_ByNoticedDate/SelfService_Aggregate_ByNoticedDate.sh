#!/bin/bash

source /etc/profile



/root/data-integration/kitchen.sh -file=/Job/VIACOM/SelfService_Aggregate_ByNoticedDate/ND_FILES/SelfService_Aggregate_ByNoticedDate.kjb

bash /Job/VIACOM/SelfService_Aggregate_ByNoticedDate/backup.sh

mysql_conf_114="-h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM"

jobName=SelfService_Aggregate_ByNoticedDate
endDate=`date -d 'now' +%Y-%m-%d`

cd /Job/VIACOM/log
mysql $mysql_conf_114 -e "select * from KLog where JOBNAME = '$jobName' and date_format(ENDDATE, '%Y-%m-%d') = '$endDate' order by ENDDATE desc limit 1\G" > "$jobName".log

if [ -n "$(grep -i error $jobName'.log'| grep -v 'ERRORS: 0')" ]
  then
        cat "$jobName".log | mail -s "viacom ETL ERROR "$jobName$endDate chen_weijie@vobile.cn
  else
        echo "ETL Job "$jobName" is OK. Well Done."| mail -s "viacom ETL OK "$jobName$endDate chen_weijie@vobile.cn
  fi

