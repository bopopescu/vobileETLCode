#!/bin/bash

#Date: 2015-10-9 10:34:05
# Author: cwj

tableName=(FilteringDataMonthly SiteDetailMonthly TitleDetailMonthly)


mysql_conf_123="-h54.67.114.123 -ukettle -pkettle DM_HBO"
mysql_conf_123_arch="-h54.67.114.123 -ukettle -pkettle DM_HBO_ARCH"

mysql_conf_114="-h192.168.110.114 -ukettle -pk3UTLe DM_HBO"
mysql_conf_114_arch="-h192.168.110.114 -ukettle -pk3UTLe DM_HBO_ARCH"

cd /Job/HBO/SiteTitleDetailMonthly/backup
rm /Job/HBO/SiteTitleDetailMonthly/backup/*

date_today=`date -d 'now'  +%Y%m%d`
last_month=`date -d 'last month' +%Y-%m`

for t in ${tableName[*]}
  do
	#backup table to DM_HBO_ARCH in the 123 production environment
        arch_t="bak_"$t"_"$date_today
        echo "arh table name is "$arch_t
        mysql $mysql_conf_123_arch -e "drop table if exists $arch_t;create table $arch_t like DM_HBO.`echo $t`; insert into $arch_t select * from DM_HBO.`echo $t`"
        echo "table name is "$t
	
	#dump data from 114 staging environment in database DM_HBO 
	cd /Job/HBO/SiteTitleDetailMonthly/backup
        mysqldump $mysql_conf_114  $t -w "YM='$last_month'" > $t
	cd /Job/HBO/SiteTitleDetailMonthly/backup
	grep -i "INSERT INTO" $t >  $t".sql"
	rm /Job/HBO/SiteTitleDetailMonthly/backup/$t

	# delete data where YM is last month if last month data exists	
        mysql $mysql_conf_123 -e "delete from   $t where YM = '$last_month'"

	# Migration data into  123 production environment 
	mysql $mysql_conf_123  -e "source /Job/HBO/SiteTitleDetailMonthly/backup/`echo $t`.sql"
  done 
