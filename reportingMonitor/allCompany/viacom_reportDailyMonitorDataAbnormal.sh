#!/bin/bash
#Date:2015-10-22 20:18:25
#Desc: Pending daily report where data is abnormal ro not.

mysql_conf_123="mysql -h54.67.114.123 -ukettle -pkettle "
yesterday=`date -d "now 1 day ago" +%Y-%m-%d`
comparisonInterval=20

###VIACOM
#viacomTableList=(DurationDistribution_Daily P2PTitle_ByNoticedDate_Daily SelfService_Aggregate_ByNoticedDate TrackingTitle_Daily YouTube_Match_Daily)
##DurationDistribution_Daily
viacom_DurationDistribution_Daily_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(SUM(Clip_Duration_Less30S + Matched_Duration_Less30S + Clip_Duration_30STo1M + Matched_Duration_30STo1M + Clip_Duration_1MTo2M + Matched_Duration_1MTo2M + Clip_Duration_2MTo3M + Matched_Duration_2MTo3M + Clip_Duration_3MTo4M +  Matched_Duration_3MTo4M + Clip_Duration_4MTo5M + Matched_Duration_4MTo5M + Clip_Duration_LGT5M + Matched_Duration_LGT5M)*100*${comparisonInterval}/(SELECT SUM(Clip_Duration_Less30S + Matched_Duration_Less30S + Clip_Duration_30STo1M + Matched_Duration_30STo1M + Clip_Duration_1MTo2M + Matched_Duration_1MTo2M + Clip_Duration_2MTo3M + Matched_Duration_2MTo3M + Clip_Duration_3MTo4M +  Matched_Duration_3MTo4M + Clip_Duration_4MTo5M + Matched_Duration_4MTo5M + Clip_Duration_LGT5M + Matched_Duration_LGT5M)  FROM (SELECT * FROM DurationDistribution_Daily WHERE Date_ID != '$yesterday' ORDER BY Date_ID DESC LIMIT ${comparisonInterval}) A)) AS '' FROM DurationDistribution_Daily WHERE Date_ID = '$yesterday';"`


##P2PTitle_ByNoticedDate_Daily 
#CAS=0
viacom_P2PTitle_ByNoticedDate_Daily_CAS0_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(SUM(InfringingIPs_Noticed)*${comparisonInterval}*100/(SELECT SUM(InfringingIPs_Noticed) FROM P2PTitle_ByNoticedDate_Daily WHERE Date_ID < '$yesterday' AND Date_ID >= DATE_ADD('$yesterday', INTERVAL -${comparisonInterval} DAY)  AND CASFLAG = 0)) AS '' FROM P2PTitle_ByNoticedDate_Daily WHERE Date_ID = '$yesterday' AND CASFLAG = 0;"`

#CAS=1
viacom_P2PTitle_ByNoticedDate_Daily_CAS1_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(SUM(InfringingIPs_Noticed)*${comparisonInterval}*100/(SELECT SUM(InfringingIPs_Noticed) FROM P2PTitle_ByNoticedDate_Daily WHERE Date_ID < '$yesterday' AND Date_ID >= DATE_ADD('$yesterday', INTERVAL -${comparisonInterval} DAY)  AND CASFLAG = 1)) AS '' FROM P2PTitle_ByNoticedDate_Daily WHERE Date_ID = '$yesterday' AND CASFLAG = 1;"`


##SelfService_Aggregate_ByNoticedDate
#UGC
viacom_SelfService_Aggregate_ByNoticedDate_UGC_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(SUM(InfringingNum)*100*${comparisonInterval}/(SELECT SUM(InfringingNum) FROM SelfService_Aggregate_ByNoticedDate WHERE Date_ID < '$yesterday' AND Date_ID >= DATE_ADD('$yesterday', INTERVAL -${comparisonInterval} DAY)  AND WebsiteType = 'UGC')) AS '' FROM  SelfService_Aggregate_ByNoticedDate WHERE Date_ID = '$yesterday' AND WebsiteType = 'UGC';"`

#hybrid
viacom_SelfService_Aggregate_ByNoticedDate_hybrid_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(SUM(InfringingNum)*100*${comparisonInterval}/(SELECT SUM(InfringingNum) FROM SelfService_Aggregate_ByNoticedDate WHERE Date_ID < '$yesterday' AND Date_ID >= DATE_ADD('$yesterday', INTERVAL -${comparisonInterval} DAY)  AND WebsiteType = 'hybrid')) AS '' FROM  SelfService_Aggregate_ByNoticedDate WHERE Date_ID = '$yesterday' AND WebsiteType = 'hybrid';"`

#cyberlocker
viacom_SelfService_Aggregate_ByNoticedDate_cyberlocker_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(SUM(InfringingNum)*100*${comparisonInterval}/(SELECT SUM(InfringingNum) FROM SelfService_Aggregate_ByNoticedDate WHERE Date_ID < '$yesterday' AND Date_ID >= DATE_ADD('$yesterday', INTERVAL -${comparisonInterval} DAY)  AND WebsiteType = 'cyberlocker')) AS '' FROM  SelfService_Aggregate_ByNoticedDate WHERE Date_ID = '$yesterday' AND WebsiteType = 'cyberlocker';"`

#linkingsite
viacom_SelfService_Aggregate_ByNoticedDate_linkingsite_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(SUM(InfringingNum)*100*${comparisonInterval}/(SELECT SUM(InfringingNum) FROM SelfService_Aggregate_ByNoticedDate WHERE Date_ID < '$yesterday' AND Date_ID >= DATE_ADD('$yesterday', INTERVAL -${comparisonInterval} DAY)  AND WebsiteType = 'linkingsite')) AS '' FROM  SelfService_Aggregate_ByNoticedDate WHERE Date_ID = '$yesterday' AND WebsiteType = 'linkingsite';"`

#TrackingTitle_Daily  # JG.Du tell me something
viacom_TrackingTitle_Daily_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(SUM(Titles_NUM)*100*${comparisonInterval}/(SELECT SUM(Titles_NUM) FROM  TrackingTitle_Daily WHERE Date_ID < '$yesterday' AND Date_ID >= DATE_ADD('$yesterday', INTERVAL -${comparisonInterval} DAY))) AS '' FROM TrackingTitle_Daily WHERE Date_ID = '$yesterday';"`

#YouTube_Match_Daily # JG.Du tell me something
viacom_YouTube_Match_Daily_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(SUM(TakeDownNO)*100*${comparisonInterval}/(SELECT SUM(TakeDownNO) FROM  YouTube_Match_Daily WHERE DateID < '$yesterday' AND DateID >= DATE_ADD('$yesterday', INTERVAL -${comparisonInterval} DAY))) AS '' FROM YouTube_Match_Daily WHERE DateID = '$yesterday';"`


#############################################################################
###MANWIN2
##manwin2TableList=(InfringingVideosFound InfringingVideosRemoveOrNot NoticeSendDaily)
##InfringingVideosFound
manwin2_InfringingVideosFound_abnormalKPI=`$mysql_conf_123 DM_MANWIN2 -e "SELECT ROUND(SUM(totalNum)*100*${comparisonInterval}/(SELECT SUM(totalNum) FROM  InfringingVideosFound WHERE DateID < '$yesterday' AND DateID >= DATE_ADD('$yesterday', INTERVAL -${comparisonInterval} DAY))) AS '' FROM InfringingVideosFound WHERE DateID = '$yesterday';"`

##InfringingVideosRemoveOrNot
manwin2_InfringingVideosFound_abnormalKPI=`$mysql_conf_123 DM_MANWIN2 -e "SELECT ROUND(SUM(infringRemoveNum+infringNotRemoveNum)*100*${comparisonInterval}/(SELECT SUM(infringRemoveNum+infringNotRemoveNum) FROM  InfringingVideosRemoveOrNot WHERE DateID < '$yesterday' AND DateID >= DATE_ADD('$yesterday', INTERVAL -${comparisonInterval} DAY))) AS '' FROM InfringingVideosRemoveOrNot WHERE DateID = '$yesterday';"`

##NoticeSendDaily
manwin2_NoticeSendDaily_abnormalKPI=`$mysql_conf_123 DM_MANWIN2 -e "SELECT ROUND(SUM(totalSendNum)*100*${comparisonInterval}/(SELECT SUM(totalSendNum) FROM  NoticeSendDaily WHERE DateID < '$yesterday' AND DateID >= DATE_ADD('$yesterday', INTERVAL -${comparisonInterval} DAY))) AS '' FROM NoticeSendDaily WHERE DateID = '$yesterday';"`

#############################################################################
#disneyTableList=(DisneySelfService)

#############################################################################
paraNameList=(viacom_DurationDistribution_Daily_abnormalKPI viacom_P2PTitle_ByNoticedDate_Daily_CAS0_abnormalKPI viacom_P2PTitle_ByNoticedDate_Daily_CAS1_abnormalKPI  viacom_SelfService_Aggregate_ByNoticedDate_UGC_abnormalKPI viacom_SelfService_Aggregate_ByNoticedDate_hybrid_abnormalKPI viacom_SelfService_Aggregate_ByNoticedDate_cyberlocker_abnormalKPI viacom_SelfService_Aggregate_ByNoticedDate_linkingsite_abnormalKPI viacom_TrackingTitle_Daily_abnormalKPI viacom_YouTube_Match_Daily_abnormalKPI manwin2_InfringingVideosFound_abnormalKPI manwin2_InfringingVideosFound_abnormalKPI manwin2_NoticeSendDaily_abnormalKPI)

paraList=($viacom_DurationDistribution_Daily_abnormalKPI $viacom_P2PTitle_ByNoticedDate_Daily_CAS0_abnormalKPI $viacom_P2PTitle_ByNoticedDate_Daily_CAS1_abnormalKPI  $viacom_SelfService_Aggregate_ByNoticedDate_UGC_abnormalKPI $viacom_SelfService_Aggregate_ByNoticedDate_hybrid_abnormalKPI $viacom_SelfService_Aggregate_ByNoticedDate_cyberlocker_abnormalKPI $viacom_SelfService_Aggregate_ByNoticedDate_linkingsite_abnormalKPI $viacom_TrackingTitle_Daily_abnormalKPI $viacom_YouTube_Match_Daily_abnormalKPI $manwin2_InfringingVideosFound_abnormalKPI $manwin2_InfringingVideosFound_abnormalKPI $manwin2_NoticeSendDaily_abnormalKPI)

echo ${#paraNameList[@]}
echo ${#paraList[@]}

idx=0
today=`date -d now +%Y%m%d%H%M%S`
abnormalKPI=1
for para in ${paraList[*]}
do
    echo "${paraNameList[$idx]} : ${paraList[$idx]}" >> /Job/reportingMonitor/allCompany/log/reportDailyMonitorDataAbnormal${today}.log
    echo ----------------------------------------------------------------------------- >> /Job/reportingMonitor/allCompany/log/reportDailyMonitorDataAbnormal${today}.log
    echo  >> /Job/reportingMonitor/allCompany/log/reportDailyMonitorDataAbnormal${today}.log
    if [ ${para} = NULL ];then
        para=0
    fi

    if [ $para -lt 40 ];then
        para=0
    else
        para=1
    fi
    
    idx=$(($idx+1))
    abnormalKPI=$((${para}*${abnormalKPI}))
done

if [ ${abnormalKPI} -ne 0 ];then
    mail -s "Report Daily Monitor Data abnormal is OK" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn  < /Job/reportingMonitor/allCompany/log/reportDailyMonitorDataAbnormal${today}.log
    python /Job/reportingMonitor/heartbeart.py  dbpc.ops.vobile.org 5800 reporting_portal data_abnormal.daily
else
    mail -s "Report Daily Monitor Data abnormal WARNING" chen_weijie@vobile.cn   du_li@vobile.cn sun_cong@vobile.cn < /Job/reportingMonitor/allCompany/log/reportDailyMonitorDataAbnormal${today}.log
fi

