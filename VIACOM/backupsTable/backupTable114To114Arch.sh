#!/bin/bash
#Date:2015-10-11 11:46:57
#Author: cwj
#Desc: backups table from 114 DM_VIACOM to 114 DM_VIACOM_ARCH

mysql_conf_114_arch="-h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM_ARCH"

date_now=`date -d 'now'  +%Y%m%d%H%M%S`
tableName=(SiteDetail_Monthly SiteDetail_Yearly TitleDetail_Monthly TitleDetail_Yearly Estimated_Summary_Monthly TopTitle_Monthly P2PISPSUM_Yearly P2PISPSUM_Monthly)

for table in ${tableName[*]}
  do
        arch_table=bak_${table}_${date_now}
        echo "arh table name is "$arch_table
        mysql $mysql_conf_114_arch -e "create table ${arch_table} like DM_VIACOM.${table}; insert into $arch_table select * from DM_VIACOM.${table}"
        echo "table name is "$table
	
  done 

