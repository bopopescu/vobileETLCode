#!/bin/bash
#Date: 2016-02-26
#author: sc

MYSQL_CON_staging="-h192.168.110.114 -ukettle -pk3UTLe"
MYSQL_CON_online="-h54.67.114.123 -ukettle -pkettle"
yesterday=`date -d"1 day ago" +%Y.%m.%d`
TARGET=/Job/executiveDashboard/Tmp_file/$yesterday.csv

cd /Job/executiveDashboard/src 

echo "------ start RT data ------"
curl -u gs:pav}2Two ftp://206.99.94.101/$yesterday.csv > $TARGET
python /Job/executiveDashboard/src/stat_rt.py

echo "------ start vt, vtx, reclaim matches, mediawise through-put ------"
rm /Job/executiveDashboard/Tmp_file/md5
python /Job/executiveDashboard/src/system_throughput_daily.py

echo "------ start cpu info ------"
bash  /Job/executiveDashboard/src/download_ec2file.sh
python /Job/executiveDashboard/src/stat_cpu.py

echo "------ sync data from staging to online ------"
#mysqldump $MYSQL_CON_staging DM_EDASHBOARD match_metric mediawise_task_metric system_efficiency_metric > sync.sql
#mysql $MYSQL_CON_online DM_EDASHBOARD -e"source /Job/executiveDashboard/sync.sql"
#rm /Job/executiveDashboard/sync.sql

echo "------ finished all Job, send verify mail ------"
python /Job/executiveDashboard/src/verifyMail.py
