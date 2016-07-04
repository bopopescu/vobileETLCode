#!/bin/bash
# Date: 2015-09-30
# Author: cwj
#
#
today=`date -d "now" +%Y%m%d`
crontab -l  > /Job/backupCrontab/backupFiles/crontab${today}

cat /Job/backupCrontab/backupFiles/crontab"$today"| mail -s "Backups crontab 192.168.111.235 -- $today" chen_weijie@vobile.cn

if [ $? -ne 0 ];then
    echo "Oh, bad luck today. Backups crontab 192.168.111.235 -- ERROR"| mail -s "Backups crontab failed" chen_weijie@vobile.cn
fi
    
    

