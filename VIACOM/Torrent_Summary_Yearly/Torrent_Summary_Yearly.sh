#!/bin/bash
# 2015-08-24
# author : cwj

source /etc/profile

bash /root/data-integration/kitchen.sh /file://Job/VIACOM/Torrent_Summary_Yearly/STEP1/Torrent_Summary_Yearly_step1.kjb

jobName=Torrent_Summary_Yearly_step1
mysql_conf_114="-h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM"

cd /Job/VIACOM/log
mysql $mysql_conf_114 -e "select * from KLog where JOBNAME = '$jobName' and date_format(ENDDATE, '%Y-%m-%d') = '$endDate'\G" > "$jobName".log

if [ -n "$(grep -i error $jobName'.log'| grep -v 'ERRORS: 0')" ]
  then
        cat "$jobName".log | mail -s "viacom ETL ERROR "$jobName$endDate chen_weijie@vobile.cn
  else
        echo "ETL Job "$jobName" is OK. Well Done."| mail -s "viacom ETL OK "$jobName$endDate chen_weijie@vobile.cn
  fi


# ========================================================================================================================================
mysql -h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM -e "delete from Website_Alexa_Country_Yearly where YM = '$last_month'"

python /Job/VIACOM/Torrent_Summary_Yearly/spiderCountryYearly.py

bash /root/data-integration/kitchen.sh /file://Job/VIACOM/Torrent_Summary_Yearly/STEP2/Torrent_Summary_Yearly_step2.kjb


jobName=Torrent_Summary_Yearly_step2
mysql_conf_114="-h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM"

cd /Job/VIACOM/log
mysql $mysql_conf_114 -e "select * from KLog where JOBNAME = '$jobName' and date_format(ENDDATE, '%Y-%m-%d') = '$endDate'\G" > "$jobName".log

if [ -n "$(grep -i error $jobName'.log'| grep -v 'ERRORS: 0')" ]
  then
        cat "$jobName".log | mail -s "viacom ETL ERROR "$jobName$endDate chen_weijie@vobile.cn
  else
        echo "ETL Job "$jobName" is OK. Well Done."| mail -s "viacom ETL OK "$jobName$endDate chen_weijie@vobile.cn
  fi

