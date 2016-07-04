#!/bin/bash

#Date: 2015-09-10
#author: c

source /etc/profile
bash /root/data-integration/kitchen.sh -file=/Job/HBO/SiteTitleDetailMonthly/SDM_Files/SiteTitleDetailMonthly.kjb

mysql_conf_123="-h54.67.123.123 -ukettle -pkettle DM_HBO"

jobName=SiteTitleDetailMonthly
endDate=`date -d 'now' +%Y-%m-%d`

cd /Job/HBO/log
mysql $mysql_conf_123 -e "select * from KLog where JOBNAME = '$jobName' and date_format(ENDDATE, '%Y-%m-%d') = '$endDate' order by ENDDATE desc limit 1\G" > ${jobName}.log

if [ -n "$(grep -i error ${jobName}.log| grep -v 'ERRORS: 0')" ]; then
    cat ${jobName}.log | mail -s "HBO ETL ERROR "${jobName}${endDate} chen_weijie@vobile.cn
else
    echo "ETL Job ${jobName} is OK. Well Done."| mail -s "HBO ETL OK "${jobName}${endDate} chen_weijie@vobile.cn
fi

