#!/bin/bash
#Date: 2015-10-22 21:53:53
#Author: cwj
#Desc: Pending wheather CBS weekly report data is abnormal
#
#
#
mysql_conf_123="mysql -h54.67.114.123 -ukettle -pkettle "
todayETL=`date -d "now" +%Y-%m-%d`
#CBSTableList=(ActiveTVSeries ActivityReport Infringements NoListAssigned SentNotRemove)

##ActiveTVSeries
#Change Status
cbs_ActiveTVSeries_ChangeStatus_abnormalKPI=`$mysql_conf_123 DM_CBS -e "SELECT ROUND(COUNT(*)*100*2/(SELECT COUNT(*) FROM ActiveTVSeries WHERE  DATEID IN (DATE_ADD('${todayETL}', INTERVAL -7 DAY), DATE_ADD('${todayETL}', INTERVAL -14 DAY)) AND Note = 'Change Status')) AS '' FROM ActiveTVSeries WHERE DATEID='${todayETL}' AND Note = 'Change Status';"`

#Change Tier
cbs_ActiveTVSeries_ChangeTier_abnormalKPI=`$mysql_conf_123 DM_CBS -e "SELECT ROUND(COUNT(*)*100*2/(SELECT COUNT(*) FROM ActiveTVSeries WHERE DATEID IN (DATE_ADD('${todayETL}', INTERVAL -7 DAY), DATE_ADD('${todayETL}', INTERVAL -14 DAY))  AND Note = 'Change Tier')) AS '' FROM ActiveTVSeries WHERE DATEID='${todayETL}' AND Note = 'Change Tier';"`

#New titles added
cbs_ActiveTVSeries_NewTitlesAdded_abnormalKPI=`$mysql_conf_123 DM_CBS -e "SELECT ROUND(COUNT(*)*100*2/(SELECT COUNT(*) FROM ActiveTVSeries WHERE DATEID IN (DATE_ADD('${todayETL}', INTERVAL -7 DAY), DATE_ADD('${todayETL}', INTERVAL -14 DAY))  AND Note = 'New titles added')) AS '' FROM ActiveTVSeries WHERE DATEID='${todayETL}' AND Note = 'New titles added';"`

##ActivityReport
#ActiveTier1NO+ActiveTier2NO
cbs_ActivityReport_ActiveTier12_abnormalKPI=`$mysql_conf_123 DM_CBS -e "SELECT ROUND(SUM(ActiveTier1NO+ActiveTier2NO)*100*2/(SELECT SUM(ActiveTier1NO+ActiveTier2NO) FROM ActivityReport WHERE DateID IN (DATE_ADD('${todayETL}', INTERVAL -7 DAY), DATE_ADD('${todayETL}', INTERVAL -14 DAY)))) AS '' FROM ActivityReport WHERE DateID = '${todayETL}';"`

#InactiveTier1NO+InactiveTier2NO
cbs_ActivityReport_InactiveTier12_abnormalKPI=`$mysql_conf_123 DM_CBS -e "SELECT ROUND(SUM(InactiveTier1NO+InactiveTier2NO)*100*2/(SELECT SUM(InactiveTier1NO+InactiveTier2NO) FROM ActivityReport WHERE DateID IN (DATE_ADD('${todayETL}', INTERVAL -7 DAY), DATE_ADD('${todayETL}', INTERVAL -14 DAY)))) AS '' FROM ActivityReport WHERE DateID = '${todayETL}';"`

##Infringements
#Infringing30Days
cbs_Infringements_Infringing30Days_abnormalKPI=`$mysql_conf_123 DM_CBS -e "SELECT ROUND(SUM(Infringing30Days)*100*2/(SELECT SUM(Infringing30Days) FROM Infringements WHERE DateID IN (DATE_ADD('${todayETL}', INTERVAL -7 DAY), DATE_ADD('${todayETL}', INTERVAL -14 DAY)))) AS '' FROM Infringements WHERE DateID = '${todayETL}';"`

##NoListAssigned -- NO MONITOR

##SentNotRemove
#TakedownSentNO
cbs_SentNotRemove_TakedownSentNO_abnormalKPI=`$mysql_conf_123 DM_CBS -e "SELECT ROUND(SUM(TakedownSentNO)*100*2/(SELECT SUM(TakedownSentNO) FROM SentNotRemove WHERE DateID IN (DATE_ADD('${todayETL}', INTERVAL -7 DAY), DATE_ADD('${todayETL}', INTERVAL -14 DAY)))) AS '' FROM SentNotRemove WHERE DateID = '${todayETL}';"`

paraList=($cbs_ActiveTVSeries_ChangeStatus_abnormalKPI $cbs_ActiveTVSeries_ChangeTier_abnormalKPI $cbs_ActiveTVSeries_NewTitlesAdded_abnormalKPI $cbs_ActivityReport_ActiveTier12_abnormalKPI $cbs_ActivityReport_InactiveTier12_abnormalKPI $cbs_Infringements_Infringing30Days_abnormalKPI $cbs_SentNotRemove_TakedownSentNO_abnormalKPI)
paraNameList=(cbs_ActiveTVSeries_ChangeStatus_abnormalKPI cbs_ActiveTVSeries_ChangeTier_abnormalKPI cbs_ActiveTVSeries_NewTitlesAdded_abnormalKPI cbs_ActivityReport_ActiveTier12_abnormalKPI cbs_ActivityReport_InactiveTier12_abnormalKPI cbs_Infringements_Infringing30Days_abnormalKPI cbs_SentNotRemove_TakedownSentNO_abnormalKPI)

echo ${#paraNameList[@]}
echo ${#paraList[@]}

today=`date -d now +%Y%m%d%H%M%S`
abnormalKPI=1
idx=0
for para in ${paraList[*]}
do
    echo "${paraNameList[$idx]} : ${paraList[$idx]}" >> /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorDataAbnormal${today}.log
    echo ---------------------------------------------------------------------- >> /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorDataAbnormal${today}.log
    echo >> /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorDataAbnormal${today}.log
    if [ $para = NULL ];then
        para=0
    fi
    if [ $para -lt 5 ];then
        para=0
    else
        para=1
    fi

    idx=$(($idx+1))
    abnormalKPI=$((${abnormalKPI}*${para}))
done

if [ ${abnormalKPI} -ne 0 ];then
    mail -s "CBS Report Weekly Monitor Data abnormal is OK" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn < /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorDataAbnormal${today}.log
    python /Job/reportingMonitor/heartbeart.py  dbpc.ops.vobile.org 5800 reporting_portal data_abnormal.weekly
else
    mail -s "CBS Report Weekly Monitor Data abnormal WARNING" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn < /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorDataAbnormal${today}.log
fi



