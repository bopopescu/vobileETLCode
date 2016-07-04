#!/bin/bash
#Date: 2015-10-9 19:31:27
#Author: cwj
#Desc: cp p2p cube data(P2PTitle_ByNoticedDate_Daily) from 123 server to 114 every day

tableName=(P2PTitle_ByNoticedDate_Daily)
mysql_conf_123="-h54.67.114.123 -ukettle -pkettle DM_VIACOM"
mysql_conf_114="-h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM"
yesterday=`date -d 'now 1 days ago'  +%Y-%m-%d`
today=`date -d 'now'  +%Y%m%d`

for t in ${tableName[*]}
  do
        echo "table name is "$t

	#dump data from 123 production environment in database DM_VIACOM 
        mysqldump $mysql_conf_123  $t -w "Date_ID='$yesterday'" > /Job/VIACOM/backupsTable/p2pCubeData/$t$today
	grep -i "INSERT INTO" /Job/VIACOM/backupsTable/p2pCubeData/$t$today >  /Job/VIACOM/backupsTable/p2pCubeData/$t$today".sql"
        rm /Job/VIACOM/backupsTable/p2pCubeData/$t$today

	# delete data where Date_ID is yesterday if yesterday data exists	
        mysql $mysql_conf_114 -e "delete from $t where Date_ID = '$yesterday'"

	# Migration data into 114 staging environment 
	mysql $mysql_conf_114  -e "source /Job/VIACOM/backupsTable/p2pCubeData/`echo $t$today`.sql"
  done 

#monitor data
