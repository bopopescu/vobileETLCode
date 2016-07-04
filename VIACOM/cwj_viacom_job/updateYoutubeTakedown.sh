#!/bin/bash
#Desc: update youtube and ugc total take down rate in SiteDetail_Monthly and SiteDetail_Yearly table of viacom monthly report 


mysql_conf_114="mysql -h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM "
#mysql_conf_123="mysql -h54.67.114.123 -ukettle -pkettle DM_VIACOM "
last_month=`date -d 'now 1 month ago' +%Y-%m`
start_month=`date -d 'now 12 month ago' +%Y-%m`

${mysql_conf_114} -e "insert into bak_YouTubeTakeDownUpdate select * from YouTubeTakeDownUpdate where YM = '${last_month}'; delete from YouTubeTakeDownUpdate where YM = '${last_month}'; insert into YouTubeTakeDownUpdate select YM, '1' as periodFlag, '1' as isTotal ,InfringingNum as infringingNum, TakeDownNum as takeDownNum, (select SUM(CMSInfringingNum) from SelfService_Aggregate_ByNoticedDate where YM = '${last_month}') as CMS, current_timestamp AS ETL_DTE from SiteDetail_Monthly where YM = '${last_month}' and websiteType = 'UGC' and websiteName = 'Total' UNION ALL select YM, '1' as periodFlag, '0' as isTotal ,InfringingNum as infringingNum , TakeDownNum as takeDownNum, (select SUM(CMSInfringingNum) from SelfService_Aggregate_ByNoticedDate where YM = '${last_month}') as CMS, current_timestamp AS ETL_DTE from SiteDetail_Monthly where YM = '${last_month}' and websiteType = 'UGC' and websiteName = 'YouTube' UNION ALL select YM, '0' as periodFlag, '1' as isTotal ,InfringingNum as infringingNum, TakeDownNum as takeDownNum, (select SUM(CMSInfringingNum) from SelfService_Aggregate_ByNoticedDate where YM >= '${start_month}' and YM <= '${last_month}' and YM >= '2014-12') AS CMS, current_timestamp AS ETL_DTE from SiteDetail_Yearly where YM = '${last_month}' and websiteType = 'UGC' and websiteName = 'Total' UNION ALL select YM, '0' as periodFlag, '0' as isTotal ,InfringingNum as infringingNum , TakeDownNum as takeDownNum, (select SUM(CMSInfringingNum) from SelfService_Aggregate_ByNoticedDate where YM >= '${start_month}' and YM <= '${last_month}' and YM >= '2014-12') AS CMS, current_timestamp AS ETL_DTE from SiteDetail_Yearly where YM = '${last_month}' and websiteType = 'UGC' and websiteName = 'YouTube';"

youtube_takedown_monthly=`${mysql_conf_114} -e "select if(takeDownNum+CMS > infringingNum, round(infringingNum*0.97), takeDownNum+CMS) AS '' from YouTubeTakeDownUpdate where  periodFlag = '1' and isTotal = '0' and YM = '${last_month}'"`
total_takedown_monthly=`${mysql_conf_114} -e "select if(takeDownNum+CMS > infringingNum, round(infringingNum*0.97), takeDownNum+CMS) AS '' from YouTubeTakeDownUpdate where  periodFlag = '1' and isTotal = '1' and YM = '${last_month}'"`

youtube_takedown_yearly=`${mysql_conf_114} -e "select if(takeDownNum+CMS > infringingNum,round(infringingNum*0.97), takeDownNum+CMS) AS '' from YouTubeTakeDownUpdate where  periodFlag = '0' and isTotal = '0' and YM = '${last_month}'"`
total_takedown_yearly=`${mysql_conf_114} -e "select if(takeDownNum+CMS > infringingNum, round(infringingNum*0.97), takeDownNum+CMS) AS '' from YouTubeTakeDownUpdate where  periodFlag = '0' and isTotal = '1' and YM = '${last_month}'"`

${mysql_conf_114} -e "update SiteDetail_Monthly set TakedownNum = ${youtube_takedown_monthly} where YM = '${last_month}' and WebsiteType = 'UGC' and WebsiteName = 'YouTube'; update SiteDetail_Monthly set TakedownNum = ${total_takedown_monthly} where YM = '${last_month}' and WebsiteType = 'UGC' and WebsiteName = 'Total';update SiteDetail_Yearly set TakedownNum = ${youtube_takedown_yearly} where YM = '${last_month}' and WebsiteType = 'UGC' and WebsiteName = 'YouTube'; update SiteDetail_Yearly set TakedownNum = ${total_takedown_yearly} where YM = '${last_month}' and WebsiteType = 'UGC' and WebsiteName = 'Total';"

youtube_takedown_monthly_abnormal_KPI=`${mysql_conf_114} -e "select if(takeDownNum+CMS > infringingNum, 0, 1) AS '' from YouTubeTakeDownUpdate where  periodFlag = '1' and isTotal = '0' and YM = '${last_month}'"`

total_takedown_monthly_abnormal_KPI=`${mysql_conf_114} -e "select if(takeDownNum+CMS > infringingNum, 0, 1) AS '' from YouTubeTakeDownUpdate where  periodFlag = '1' and isTotal = '1' and YM = '${last_month}';"`

youtube_takedown_yearly_abnormal_KPI=`${mysql_conf_114} -e "select if(takeDownNum+CMS > infringingNum, 0, 1) AS '' from YouTubeTakeDownUpdate where  periodFlag = '0' and isTotal = '0' and YM = '${last_month}'"`

total_takedown_yearly_abnormal_KPI=`${mysql_conf_114} -e "select if(takeDownNum+CMS > infringingNum, 0, 1) AS '' from YouTubeTakeDownUpdate where  periodFlag = '0' and isTotal = '1' and YM = '${last_month}'"`

para_list=(${youtube_takedown_monthly_abnormal_KPI} ${total_takedown_monthly_abnormal_KPI} ${youtube_takedown_yearly_abnormal_KPI} ${total_takedown_yearly_abnormal_KPI})
para_name_list=(youtube_takedown_monthly_abnormal_KPI total_takedown_monthly_abnormal_KPI youtube_takedown_yearly_abnormal_KPI total_takedown_yearly_abnormal_KPI)

idx=0
for p in ${para_list[*]}
do
    if [ ${p} -eq 0 ];then
        echo "${para_name_list[$idx]}" | mail -s "Youtube Takedown Update Error" chen_weijie@vobile.cn
    fi
    idx=$((${idx}+1))
done

