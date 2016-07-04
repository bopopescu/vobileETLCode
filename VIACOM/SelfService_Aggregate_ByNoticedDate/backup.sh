#!/bin/bash

#2015-08-14


cd /root/script/backup/

cd /root/script/backup/




Date=`date -d "-1 day $curDate" +%Y%m%d`
MYSQL_CON_114="-h192.168.110.114 -ukettle -pk3UTLe"
MYSQL_CON_123="-h54.67.114.123 -ukettle -pkettle"
MYSQL_CON_217="-h54.184.177.217 -ukettle -pkettle"

tables="SelfService_Aggregate_ByNoticedDate"

for t in ${tables}
do
    cd /root/script/backup/
    t_d=$t$Date
    mysql $MYSQL_CON_114 DM_VIACOM_ARCH -e"drop table if exists $t_d; create table $t_d like DM_VIACOM.$t; insert into $t_d select * from DM_VIACOM.$t"
    mysqldump $MYSQL_CON_114 DM_VIACOM $t --where="Date_ID >= '2014-07-01'" --skip-lock-tables > $t_d.sql
    mysql $MYSQL_CON_123 DM_VIACOM -e"source /root/script/backup/$t_d.sql"
    mysql $MYSQL_CON_217 DM_VIACOM -e"source /root/script/backup/$t_d.sql"
done


