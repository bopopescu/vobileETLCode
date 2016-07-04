#!/bin/bash

# 2015-9-24
mysql_conf_123=" -h54.67.114.123 -ukettle -pkettle DM_MANWIN2 "
mysql_conf_114=" -h192.168.110.114 -ukettle -pk3UTLe DM_MANWIN2"
table=(InfringingVideosFound InfringingVideosRemoveOrNot KLog NoticeSendDaily siteList websiteDomainList)

cd /Job/MANWIN2/ManWinDaily/backup
today=`date -d "now" +%Y%m%d`
for t in ${table[*]}
do
    mysqldump $mysql_conf_123  $t>$t$today.sql
    mysql $mysql_conf_114 -e "source /Job/MANWIN2/ManWinDaily/backup/$t$today.sql"
done

