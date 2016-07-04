#!/bin/bash

#Date: 2015-9-2 20:43:08
# Author: cwj

# backups the following table in host 54.67.114.123 

tableName=(SiteDetail_Yearly SiteDetail_Yearly TitleDetail_Yearly TitleDetail_Yearly Estimated_Summary_Yearly TopTitle_Yearly P2PISPSUM_Yearly P2PISPSUM_Yearly)

date_today=`date -d 'now'  +%Y%m%d`

mysql_conf_123="-h54.67.114.123 -ukettle -pkettle DM_VIACOM"
mysql_conf_114="mysql -h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM"
mysql_conf_217="mysql -h54.184.177.217 -ukettle -pkettle DM_VIACOM"

tables=`mysql $mysql_conf_123 -e "show tables like '%2015%';"`
for t in $tables
  do
	echo $t
  done

#for t in ${tableName[*]}
#  do
#	tn="DM_VIACOM_ARCH.bak_"$t"_"$date_today
#	echo $tn
#  	mysql $myql_conf_123 -e "create table $tn  as select * from $t"
#        echo $t
#  done



