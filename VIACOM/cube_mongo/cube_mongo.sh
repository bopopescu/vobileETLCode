#!/bin/bash

#Date: 2015-12-18
#author: cwj


source /etc/profile
####################################################################################################
# mysql to mongo
bash /Job/VIACOM/cube_mongo/mongoToMysql.sh
#####################################################################################################
# not including min, including max, interval default value is 1
interval=1
min_vtweb_date=`date -d "now ${interval} days ago" "+%Y-%m-%d 08:00:00"` 
max_vtweb_date=`date -d "now 0 days ago" "+%Y-%m-%d 08:00:00"`  

# not including min, including max, insight_min_interval default 2, insight_max_interval default is 1
insight_min_interval=2
insight_max_interval=1
min_insight_date=`date -d "now ${insight_min_interval} days ago" +%Y-%m-%d`  
max_insight_date=`date -d "now ${insight_max_interval} days ago" +%Y-%m-%d` 

# including min, including max, cms_interval_month default is 2
cms_interval_month=2
min_CMS_YM=`date -d "now ${cms_interval_month} month ago" +%Y-%m` 
max_CMS_YM=`date -d "now" +%Y-%m`   


bash /root/data-integration/kitchen.sh -file=/Job/VIACOM/cube_mongo/zipFile/SelfService_Aggregate_ByNoticedDate_mongo_20151215.kjb  -param:min_vtweb_date="${min_vtweb_date}" -param:max_vtweb_date="${max_vtweb_date}" -param:min_insight_date="${min_insight_date}" -param:max_insight_date="${max_insight_date}" -param:min_CMS_YM="${min_CMS_YM}" -param:max_CMS_YM="${max_CMS_YM}" 

mysql_conf_114="-h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM"

jobName=SelfService_Aggregate_ByNoticedDate_mongo_20151215
endDate=`date -d 'now' +%Y-%m-%d`

cd /Job/VIACOM/log
mysql $mysql_conf_114 -e "select * from KLog where JOBNAME = '$jobName' and date_format(ENDDATE, '%Y-%m-%d') = '$endDate' order by ENDDATE desc limit 1\G" > ${jobName}.log

if [ -n "$(grep -i error ${jobName}.log| grep -v 'ERRORS: 0')" ]
  then
        cat ${jobName}.log | mail -s "viacom ETL ERROR "${jobName}${endDate} chen_weijie@vobile.cn
  else
        echo "ETL Job "$jobName" is OK. Well Done."| mail -s "viacom ETL OK "${jobName}${endDate} chen_weijie@vobile.cn
  fi

function dataFromCubeTargetToOldTable(){
  local from_table=SelfService_Aggregate_ByNoticedDate_mongo20151215_target
  local to_table=SelfService_Aggregate_ByNoticedDate
  local to_table=SelfService_Aggregate_ByNoticedDate_test20160122

  # delete from 114 old table
  mysql $mysql_conf_114 -e "delete from ${to_table};"

  # from 114 SelfService_Aggregate_ByNoticedDate_mongo20151215_target to  114 SelfService_Aggregate_ByNoticedDate
  mysql $mysql_conf_114 -e "insert into ${to_table}  select * from ${from_table};"
  
  # from 114 SelfService_Aggregate_ByNoticedDate to 123 SelfService_Aggregate_ByNoticedDate
  #bash /root/Job/SelfService_Aggregate_ByNoticedDate_Test/backup.sh  
}

dataFromCubeTargetToOldTable
