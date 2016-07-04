#!/bin/bash

source /etc/profile

bash /root/data-integration/kitchen.sh -file=/Job/VIACOM/P2PISPSUM_Yearly/PY_Files/P2PISPSUM_Yearly.kjb

mysql_conf_114="-h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM"

jobName=P2PISPSUM_Yearly
endDate=`date -d 'now' +%Y-%m-%d`

cd /Job/VIACOM/log
mysql $mysql_conf_114 -e "select * from KLog where JOBNAME = '$jobName' and date_format(ENDDATE, '%Y-%m-%d') = '$endDate' order by ENDDATE desc limit 1\ \G" > "$jobName".log

if [ -n "$(grep -i error $jobName'.log'| grep -v 'ERRORS: 0')" ]
  then
        cat "$jobName".log | mail -s "viacom ETL ERROR "$jobName$endDate chen_weijie@vobile.cn
  else
        echo "ETL Job "$jobName" is OK. Well Done."| mail -s "viacom ETL OK "$jobName$endDate chen_weijie@vobile.cn
  fi
