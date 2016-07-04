#!/bin/bash
#
#Date: 2015-9-2 20:43:08
# Author: cwj
#

tableName=(SiteDetail_Monthly SiteDetail_Yearly TitleDetail_Monthly TitleDetail_Yearly Estimated_Summary_Monthly TopTitle_Monthly P2PISPSUM_Yearly P2PISPSUM_Monthly)
mysql_conf_123="-h54.67.114.123 -ukettle -pkettle DM_VIACOM"
mysql_conf_123_arch="-h54.67.114.123 -ukettle -pkettle DM_VIACOM_ARCH"

mysql_conf_114="-h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM"
mysql_conf_114_arch="-h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM_ARCH"

cd /Job/VIACOM/backupsTable/monthData/
rm /Job/VIACOM/backupsTable/monthData/*

date_today=`date -d 'now'  +%Y%m%d`
last_month=`date -d 'last month' +%Y-%m`

for t in ${tableName[*]}
  do
	#backup table to DM_VIACOM_ARCH in the 123 production environment
        arch_t=bak_${t}_${date_today}
        #echo "arh table name is "${arch_t}
        mysql $mysql_conf_123_arch -e "drop table if exists ${arch_t}; create table ${arch_t} like DM_VIACOM.${t}; insert into $arch_t select * from DM_VIACOM.${t}"
	
	#dump data from 114 staging environment in database DM_VIACOM 
	cd /Job/VIACOM/backupsTable/monthData	
        mysqldump $mysql_conf_114  $t -w "YM='$last_month'" > $t
	cd /Job/VIACOM/backupsTable/monthData
        #rm /Job/VIACOM/backupsTable/monthData/${t}.sql
        echo "set names utf8;" > /Job/VIACOM/backupsTable/monthData/${t}.sql
	grep -i "INSERT INTO" ${t} >>  /Job/VIACOM/backupsTable/monthData/${t}.sql
	rm /Job/VIACOM/backupsTable/monthData/${t}

	# delete data where YM is last month if last month data exists	
        mysql $mysql_conf_123 -e "delete from   $t where YM = '$last_month'"

	# Migration data into  123 production environment 
	mysql $mysql_conf_123  -e "source /Job/VIACOM/backupsTable/monthData/${t}.sql"
  done 
