#!/bin/bash
#Date: 2015-10-14
#Author: cwj
#Desc: 
#
#
#
mysql_conf_123="mysql -h54.67.114.123 -ukettle -pkettle "
todayETL=`date -d "now" +%Y-%m-%d`
today=`date -d "now" +%Y%m%d%H%M%S`
#CBSTableList=(ActiveTVSeries ActivityReport Infringements NoListAssigned SentNotRemove)
CBSTableList=(ActiveTVSeries ActivityReport Infringements SentNotRemove)
CBSCount=1
for table in ${CBSTableList[*]}
do
    linesNum=`$mysql_conf_123 DM_CBS -e "select count(*) as '' from ${table} where DateID = '${todayETL}'"`
    echo "CBS Table: ${table}; Row Number:"${linesNum} >> /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorHasNoData${today}.log
    echo ------------------------------------------------------------------------ >> /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorHasNoData${today}.log
    echo  >> /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorHasNoData${today}.log

    if [ $linesNum -ne 0 ];then
        linesNum=1
    fi

    CBSCount=$(($linesNum*$CBSCount))
done


if [ ${CBSCount} -ne 0 ];then
    mail -s "CBS Weekly Report Monitor Have Or Not Have Data OK" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn < /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorHasNoData${today}.log
    python /Job/reportingMonitor/heartbeart.py  dbpc.ops.vobile.org 5800 reporting_portal data_no_data.weekly
else
    mail -s "CBS Weekly Report Monitor Have Or Not Have Data WARNING" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn < /Job/reportingMonitor/allCompany/log/CBSReportWeeklyMonitorHasNoData${today}.log
fi

