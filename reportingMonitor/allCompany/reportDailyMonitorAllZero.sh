#!/bin/bash
#Date: 2015-10-12 15:01:42
#Desc: Pending daily report wheather data is all zero or not
#
#
#
mysql_conf_114="mysql -hdna-613 -ukettle -pk3UTLe "

mysql_conf_123="mysql -h54.67.114.123 -ukettle -pkettle "
viacomTableList=(DurationDistribution_Daily P2PTitle_ByNoticedDate_Daily SelfService_Aggregate_ByNoticedDate TrackingTitle_Daily YouTube_Match_Daily)
manwin2TableList=(InfringingVideosFound InfringingVideosRemoveOrNot NoticeSendDaily)
disneyTableList=(DisneySelfService)
yesterday=`date -d "1 days ago" +%Y-%m-%d`

viacom_DurationDistribution_DailyKPISum=`$mysql_conf_123 DM_VIACOM -e "select sum(Clip_Duration_Less30S+Matched_Duration_Less30S+Clip_Duration_30STo1M+Matched_Duration_30STo1M+Clip_Duration_1MTo2M+Matched_Duration_1MTo2M+Clip_Duration_2MTo3M+Matched_Duration_2MTo3M+Clip_Duration_3MTo4M+Matched_Duration_3MTo4M+Clip_Duration_4MTo5M+Matched_Duration_4MTo5M+Clip_Duration_LGT5M+Matched_Duration_LGT5M) as '' from DurationDistribution_Daily where Date_ID = '${yesterday}'"`

viacom_P2PTitle_ByNoticedDate_DailyKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(InfringingIPs_Noticed)) as '' from P2PTitle_ByNoticedDate_Daily  where Date_ID = '${yesterday}'"`

viacom_SelfService_Aggregate_ByNoticedDateKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(InfringingNum)) as '' from SelfService_Aggregate_ByNoticedDate  where Date_ID = '${yesterday}'"`

viacom_TrackingTitle_Daily_KPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(Titles_NUM+Tracking_Duration)) as '' from TrackingTitle_Daily where Date_ID = '${yesterday}'"`

viacom_YouTube_Match_DailyKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(VPercent+APercent+TakeDownNO)) as '' from YouTube_Match_Daily where DateID='${yesterday}'"`

manwin2_InfringingVideosRemoveOrNotKPISum=`$mysql_conf_114 DM_MANWIN2  -e "select round(sum(infringRemoveNum+infringNotRemoveNum)) as '' from InfringingVideosRemoveOrNot  where DateID='${yesterday}'"`

manwin2_InfringingVideosFoundKPISum=`$mysql_conf_114 DM_MANWIN2 -e "select round(sum(totalNum)) as '' from InfringingVideosFound  where DateID='${yesterday}'"`

manwin2_NoticeSendDailyKPISum=`$mysql_conf_114 DM_MANWIN2 -e "select round(sum(totalSendNum+firstNum+secondNum+thirdNum)) as '' from NoticeSendDaily  where DateID='${yesterday}'"`

#disney_DisneySelfServiceKPISum=`$mysql_conf_123 DM_DISNEY -e "select sum() as '' from DisneySelfService where DateID='${yesterday}'"`

paraNameList=(viacom_DurationDistribution_DailyKPISum viacom_P2PTitle_ByNoticedDate_DailyKPISum viacom_SelfService_Aggregate_ByNoticedDateKPISum viacom_TrackingTitle_Daily_KPISum viacom_YouTube_Match_DailyKPISum manwin2_InfringingVideosRemoveOrNotKPISum manwin2_InfringingVideosFoundKPISum manwin2_NoticeSendDailyKPISum)

paraList=($viacom_DurationDistribution_DailyKPISum $viacom_P2PTitle_ByNoticedDate_DailyKPISum $viacom_SelfService_Aggregate_ByNoticedDateKPISum $viacom_TrackingTitle_Daily_KPISum $viacom_YouTube_Match_DailyKPISum $manwin2_InfringingVideosRemoveOrNotKPISum $manwin2_InfringingVideosFoundKPISum $manwin2_NoticeSendDailyKPISum)

#echo ${#paraNameList[@]}
#echo ${#paraList[@]}

today=`date -d now +%Y%m%d%H%M%S`
idx=0
allZeroKPI=1
for para in ${paraList[*]}
do 
    echo "${paraNameList[$idx]} monitor sum(KPI): ${paraList[$idx]}" >> /Job/reportingMonitor/allCompany/log/reportDailyAllZero${today}.log
    echo --------------------------------------------------------------- >> /Job/reportingMonitor/allCompany/log/reportDailyAllZero${today}.log
    echo  >> /Job/reportingMonitor/allCompany/log/reportDailyAllZero${today}.log

    if [ ${para} = NULL ];then
        para=0
    fi
    if [ ${para} -ne 0 ];then
        para=1
    fi
    idx=$(($idx+1))
    allZeroKPI=$((${allZeroKPI}*${para}))
done

#send  mail to me 
if [ ${allZeroKPI} -ne 0 ];then
    mail -s "Daily Report Monitor All Zero IS OK" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn  < /Job/reportingMonitor/allCompany/log/reportDailyAllZero${today}.log
    python /Job/reportingMonitor/heartbeart.py  dbpc.ops.vobile.org 5800 reporting_portal data_all_zero.daily 
else
    mail -s "Daily Report Monitor All Zero WARNING" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn  < /Job/reportingMonitor/allCompany/log/reportDailyAllZero${today}.log
fi
##############################################



