#!/bin/bash

#Date: 2015-9-2 20:43:08
# Author: cwj

tableName=(SiteDetail_Monthly SiteDetail_Yearly TitleDetail_Monthly TitleDetail_Yearly Estimated_Summary_Monthly TopTitle_Monthly P2PISPSUM_Yearly P2PISPSUM_Monthly)

mysql_conf_114="-h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM"
mysql_conf_114_arch="-h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM_ARCH"

mysql_conf_217=" -h54.184.177.217 -ukettle -pkettle DM_VIACOM"
mysql_conf_217_arch="-h54.184.177.217 -ukettle -pkettle DM_VIACOM_ARCH"

cd /Job/VIACOM/backupsTable/monthData/
rm /Job/VIACOM/backupsTable/monthData/*

date_today=`date -d 'now'  +%Y%m%d`
last_month=`date -d 'last month' +%Y-%m`

for t in ${tableName[*]}
  do
	#backup table to DM_VIACOM_ARCH in the 217 staging environment
        arch_t="bak_"$t"_"$date_today
        echo "arh table name is "$arch_t
        mysql $mysql_conf_217_arch -e "create table $arch_t like DM_VIACOM.`echo $t`; insert into $arch_t  select * from DM_VIACOM.`echo $t`"
        echo "table name is "$t
	
	#dump data from 114 staging environment in database DM_VIACOM 
	cd /Job/VIACOM/backupsTable/monthData	
        mysqldump $mysql_conf_114  $t -w "YM='$last_month'" > $t
	cd /Job/VIACOM/backupsTable/monthData
	grep -i "INSERT INTO" $t >  $t".sql"
	rm /Job/VIACOM/backupsTable/monthData/$t

	# delete data from 217 staging environment where YM is last month if last month data exists	
        mysql $mysql_conf_217 -e "delete from   $t where YM = '$last_month'"

	# Migration data into  217 staging environment 
	mysql $mysql_conf_217  -e "source /Job/VIACOM/backupsTable/monthData/`echo $t`.sql"
  done 
