#!/bin/bash
#Date: 2015-10-14
#Author: cwj
#Desc: 
#
#
#
mysql_conf_123="mysql -h54.67.114.123 -ukettle -pkettle "
todayETL=`date -d "now" +%Y-%m-%d`
CBSTableList=(ActiveTVSeries ActivityReport Infringements NoListAssigned SentNotRemove)

ActiveTVSeries_KPISum=`$mysql_conf_123 DM_CBS -e "select round(sum(ifnull(SeasonNO, 0)+ifnull(EpisodeNO, 0))) AS '' from ActiveTVSeries where DateID = '$todayETL'"`

ActivityReport_KPISum=`$mysql_conf_123 DM_CBS -e "select round(sum(ActiveTier1NO+ActiveTier2NO+ActiveTier1Last7+InactiveTier1NO+InactiveTier2NO+ActiveTier2last7)) '' from ActivityReport where DateID = '$todayETL'"`

Infringements_KPISum=`$mysql_conf_123 DM_CBS -e "select round(sum(Infringing14Days+Infringing30Days+Infringing365Days+Active14Days+Active365Days+InfringingPerWeek)) AS '' from Infringements where DateID = '$todayETL'"`

#NoListAssigned

SentNotRemove_KPISum=`$mysql_conf_123 DM_CBS -e "select round(sum(Views+ClipDuration)) AS '' from SentNotRemove where DateID = '$todayETL'"`

paraList=($ActiveTVSeries_KPISum $ActivityReport_KPISum $Infringements_KPISum $SentNotRemove_KPISum)
paraNameList=(ActiveTVSeries_KPISum ActivityReport_KPISum Infringements_KPISum SentNotRemove_KPISum)

echo ${#paraList[@]}
echo ${#paraNameList[@]}

idx=0
today=`date -d now +%Y%m%d%H%M%S`
allZeroKPI=1
for para in ${paraList[*]}
do
    echo "CBS Weekly: ${paraNameList[$idx]}; sum(KPI):"${para} >> /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorAllZero${today}.log
    echo ------------------------------------------------------------------------ >> /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorAllZero${today}.log
    echo  >> /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorAllZero${today}.log
    
    idx=$(($idx+1))
    if [ ${para} = NULL ];then
        para=0
    fi
    
    if [ $para -ne 0 ];then
        para=1
    fi

    allZeroKPI=$((${para}*${allZeroKPI}))
done

if [ ${allZeroKPI} -ne 0 ];then
    mail -s "CBS Weekly Report Monitor All Zero IS OK" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn < /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorAllZero${today}.log
    /Job/reportingMonitor/heartbeat.py dbpc.ops.vobile.org 5800 reporting_portal data_all_zero.weekly
else
    mail -s "CBS Weekly Report Monitor All Zero WARNING" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn  < /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorAllZero${today}.log
fi

