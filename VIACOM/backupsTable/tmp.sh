#!/bin/bash

#Date: 2015-9-2 20:43:08
# Author: cwj
#Desc dump data from 114 to ${table}.sql file

tableName=(SiteDetail_Monthly SiteDetail_Yearly TitleDetail_Monthly TitleDetail_Yearly Estimated_Summary_Monthly TopTitle_Monthly P2PISPSUM_Yearly P2PISPSUM_Monthly)

mysql_conf_114="-h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM"
mysql_conf_114_arch="-h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM_ARCH"
date_now=`date -d "now" +%Y%m%d%H%M%S`

for table in ${tableName[*]}
  do
	#dump data from 114 staging environment in database DM_VIACOM 
        mysqldump $mysql_conf_114  $table >/Job/VIACOM/backupsTable/tmp1/${table}${date_now}.sql
  done 
