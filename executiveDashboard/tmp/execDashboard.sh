#!/bin/bash
#Date: 2016-02-26
#author: sc

MYSQL_CON_staging="-h192.168.110.114 -ukettle -pk3UTLe"
MYSQL_CON_online="-h54.67.114.123 -ukettle -pkettle"
yesterday=`date -d"1 day ago" +%Y.%m.%d`
TARGET=/Job/executiveDashboard/Tmp_file/$yesterday.csv

cd /Job/executiveDashboard/tmp 

echo "------ start cpu info ------"
bash  /Job/executiveDashboard/tmp/download_ec2file.sh
python /Job/executiveDashboard/tmp/stat_cpu.py

