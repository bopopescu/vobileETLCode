#!/bin/bash
#Date:2015-10-16 14:34:29
#Author: cwj
#Desc: Monthly data is abnormal or not

#mysql_conf_123="mysql -h54.67.114.123 -ukettle -pkettle "
mysql_conf_123="mysql -h192.168.110.114 -ukettle -pk3UTLe "

viacomTableList=(Estimated_Summary_Monthly P2PISPSUM_Monthly P2PISPSUM_Yearly PartialTitle_Monthly SiteDetail_Monthly SiteDetail_Yearly SiteInfringements_Monthly TitleDetail_Monthly TitleDetail_Yearly TopTitle_Monthly)
lastMonth=`date -d "1 month ago" +%Y-%m`
lastMonth_1=`date -d "2 month ago" +%Y-%m`
lastMonth_3=`date -d "4 month ago" +%Y-%m`
Year=`date -d now +%Y`
YM01=${Year}-01
YM02=${Year}-02
YM03=${Year}-03

##Estimated_Summary_Monthly
viacom_Estimated_Summary_Monthly_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "select ROUND(SUM(EstimatedNum)/(select SUM(EstimatedNum)/3 from Estimated_Summary_Monthly where YM >= '$lastMonth_3' and YM <= '$lastMonth_1')*100)  AS '' from Estimated_Summary_Monthly where YM = '$lastMonth';"`
##P2PISPSUM_Monthly
viacom_P2PISPSUM_Monthly_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(SUM(InfringingIPs)/(SELECT SUM(InfringingIPs)/3 FROM P2PISPSUM_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1')*100) AS '' FROM P2PISPSUM_Monthly WHERE YM = '$lastMonth';"`

##PartialTitle_Monthly
viacom_PartialTitle_Monthly_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(SUM(InfringingFiles)*300/(SELECT SUM(InfringingFiles) FROM PartialTitle_Monthly WHERE DATE_FORMAT(Date_ID, '%Y-%m') >= '$lastMonth_3' AND DATE_FORMAT(Date_ID, '%Y-%m') <= '$lastMonth_1')) AS '' FROM PartialTitle_Monthly WHERE DATE_FORMAT(Date_ID, '%Y-%m') = '$lastMonth';"`

##SiteDetail_Monthly
#UGC
viacom_SiteDetail_Monthly_UGC_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(InfringingNum*300/(SELECT SUM(InfringingNum)  FROM SiteDetail_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND WebsiteType = 'UGC' AND WebsiteName = 'Total'))  AS '' FROM SiteDetail_Monthly WHERE YM = '$lastMonth' AND WebsiteType = 'UGC' AND WebsiteName = 'Total';"`

#hybrid
viacom_SiteDetail_Monthly_Hybrid_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(InfringingNum*300/(SELECT SUM(InfringingNum)  FROM SiteDetail_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND WebsiteType = 'Hybrid' AND WebsiteName = 'Total'))  AS '' FROM SiteDetail_Monthly WHERE YM = '$lastMonth' AND WebsiteType = 'Hybrid' AND WebsiteName = 'Total';"`

#Cyberlocker
viacom_SiteDetail_Monthly_Cyberlocker_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(InfringingNum*300/(SELECT SUM(InfringingNum)  FROM SiteDetail_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND WebsiteType = 'Cyberlocker' AND WebsiteName = 'Total'))  AS '' FROM SiteDetail_Monthly WHERE YM = '$lastMonth' AND WebsiteType = 'Cyberlocker' AND WebsiteName = 'Total';"`

#linkingsite
viacom_SiteDetail_Monthly_linkingsite_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(InfringingNum*300/(SELECT SUM(InfringingNum)  FROM SiteDetail_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND WebsiteType = 'linkingsite' AND WebsiteName = 'Total'))  AS '' FROM SiteDetail_Monthly WHERE YM = '$lastMonth' AND WebsiteType = 'linkingsite' AND WebsiteName = 'Total';"`

#Torrent
viacom_SiteDetail_Monthly_Torrent_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(InfringingNum*300/(SELECT SUM(InfringingNum)  FROM SiteDetail_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND WebsiteType = 'Torrent' AND WebsiteName = 'Total'))  AS '' FROM SiteDetail_Monthly WHERE YM = '$lastMonth' AND WebsiteType = 'Torrent' AND WebsiteName = 'Total';"`

#searchEngine
viacom_SiteDetail_Monthly_searchEngine_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(InfringingNum*300/(SELECT SUM(InfringingNum)  FROM SiteDetail_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND WebsiteType = 'searchEngine' AND WebsiteName = 'Total'))  AS '' FROM SiteDetail_Monthly WHERE YM = '$lastMonth' AND WebsiteType = 'searchEngine' AND WebsiteName = 'Total';"`

##TitleDetail_Monthly
#UGC
viacom_TitleDetail_Monthly_UGC_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(InfringingNum*300/(SELECT SUM(InfringingNum)  FROM TitleDetail_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND WebsiteType = 'UGC' AND Title = 'Total'))  AS '' FROM TitleDetail_Monthly WHERE YM = '$lastMonth' AND WebsiteType = 'UGC' AND Title = 'Total';"`

#hybrid
viacom_TitleDetail_Monthly_hybrid_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(InfringingNum*300/(SELECT SUM(InfringingNum)  FROM TitleDetail_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND WebsiteType = 'hybrid' AND Title = 'Total'))  AS '' FROM TitleDetail_Monthly WHERE YM = '$lastMonth' AND WebsiteType = 'hybrid' AND Title = 'Total';"`

#Cyberlocker
viacom_TitleDetail_Monthly_Cyberlocker_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(InfringingNum*300/(SELECT SUM(InfringingNum)  FROM TitleDetail_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND WebsiteType = 'Cyberlocker' AND Title = 'Total'))  AS '' FROM TitleDetail_Monthly WHERE YM = '$lastMonth' AND WebsiteType = 'Cyberlocker' AND Title = 'Total';"`

#P2P
viacom_TitleDetail_Monthly_p2p_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(SUM(InfringingNum)*300/(SELECT SUM(InfringingNum)  FROM TitleDetail_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND WebsiteType = 'p2p' AND Title = 'Total'))  AS '' FROM TitleDetail_Monthly WHERE YM = '$lastMonth' AND WebsiteType = 'p2p' AND Title = 'Total';"`

##TopTitle_Monthly 
#Estimated_Consumption
viacom_TopTitle_Monthly_Consumption_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(Estimated_Consumption*300/(SELECT SUM(Estimated_Consumption) FROM TopTitle_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND PeriodFlag = 1 AND TITLE = 'Total')) AS '' FROM TopTitle_Monthly WHERE YM = '$lastMonth' AND PeriodFlag = 1 AND TITLE = 'Total';"`

#Estimated_Streams
viacom_TopTitle_Monthly_Streams_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(Estimated_Streams*300/(SELECT SUM(Estimated_Streams) FROM TopTitle_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND PeriodFlag = 1 AND TITLE = 'Total')) AS '' FROM TopTitle_Monthly WHERE YM = '$lastMonth' AND PeriodFlag = 1 AND TITLE = 'Total';"`

#Estimated_Downloads
viacom_TopTitle_Monthly_Downloads_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(Estimated_Downloads*300/(SELECT SUM(Estimated_Downloads) FROM TopTitle_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND PeriodFlag = 1 AND TITLE = 'Total')) AS '' FROM TopTitle_Monthly WHERE YM = '$lastMonth' AND PeriodFlag = 1 AND TITLE = 'Total';"`
##########################################################################################################
paraNameList=(viacom_Estimated_Summary_Monthly_abnormalKPI viacom_P2PISPSUM_Monthly_abnormalKPI viacom_SiteDetail_Monthly_UGC_abnormalKPI viacom_SiteDetail_Monthly_Hybrid_abnormalKPI viacom_SiteDetail_Monthly_Cyberlocker_abnormalKPI viacom_SiteDetail_Monthly_linkingsite_abnormalKPI viacom_SiteDetail_Monthly_Torrent_abnormalKPI viacom_SiteDetail_Monthly_searchEngine_abnormalKPI viacom_TitleDetail_Monthly_UGC_abnormalKPI viacom_TitleDetail_Monthly_hybrid_abnormalKPI viacom_TitleDetail_Monthly_Cyberlocker_abnormalKPI viacom_TitleDetail_Monthly_p2p_abnormalKPI viacom_TopTitle_Monthly_Consumption_abnormalKPI viacom_TopTitle_Monthly_Streams_abnormalKPI viacom_TopTitle_Monthly_Downloads_abnormalKPI)
paraList=($viacom_Estimated_Summary_Monthly_abnormalKPI $viacom_P2PISPSUM_Monthly_abnormalKPI $viacom_SiteDetail_Monthly_UGC_abnormalKPI $viacom_SiteDetail_Monthly_Hybrid_abnormalKPI $viacom_SiteDetail_Monthly_Cyberlocker_abnormalKPI $viacom_SiteDetail_Monthly_linkingsite_abnormalKPI $viacom_SiteDetail_Monthly_Torrent_abnormalKPI $viacom_SiteDetail_Monthly_searchEngine_abnormalKPI $viacom_TitleDetail_Monthly_UGC_abnormalKPI $viacom_TitleDetail_Monthly_hybrid_abnormalKPI $viacom_TitleDetail_Monthly_Cyberlocker_abnormalKPI $viacom_TitleDetail_Monthly_p2p_abnormalKPI $viacom_TopTitle_Monthly_Consumption_abnormalKPI $viacom_TopTitle_Monthly_Streams_abnormalKPI $viacom_TopTitle_Monthly_Downloads_abnormalKPI)

#echo ${#paraNameList[@]}
#echo ${#paraList[@]}

today=`date -d now +%Y%m%d%H%M%S`
abnormalKPI=1
idx=0
for para in ${paraList[*]}
do
    echo "${paraNameList[$idx]} : ${paraList[$idx]}" >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorDataAbnormal${today}.log
    echo ---------------------------------------------------------------------- >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorDataAbnormal${today}.log
    echo >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorDataAbnormal${today}.log
    if [ $para = NULL ];then
        para=0
    fi
    if [ $para -lt 40 ];then
        para=0
    else
        para=1
    fi

    idx=$(($idx+1))
    abnormalKPI=$((${abnormalKPI}*${para}))
done

if [ ${abnormalKPI} -ne 0 ];then
    mail -s "Report Monthly Monitor Data abnormal is OK" chen_weijie@vobile.cn  < /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorDataAbnormal${today}.log
else
    mail -s "Report Monthly Monitor Data abnormal WARNING" chen_weijie@vobile.cn  < /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorDataAbnormal${today}.log
fi

