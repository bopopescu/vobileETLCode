#!/bin/bash
#Date:2015-10-16 14:34:29
#Author: cwj
#


mysql_conf_123="mysql -h54.67.114.123 -ukettle -pkettle "
#viacomTableList=(Estimated_Summary_Monthly P2PISPSUM_Monthly P2PISPSUM_Yearly PartialTitle_Monthly SiteDetail_Monthly SiteDetail_Yearly SiteInfringements_Monthly TitleDetail_Monthly TitleDetail_Yearly TopTitle_Monthly)

viacomTableList=(Estimated_Summary_Monthly P2PISPSUM_Monthly P2PISPSUM_Yearly PartialTitle_Monthly SiteDetail_Monthly SiteDetail_Yearly SiteInfringements_Monthly TitleDetail_Monthly TitleDetail_Yearly TopTitle_Monthly)
manwin2TableList=(Manwin2_Monthly)
#CBSTableList=()
foxTableList=(Fox_Complaince_Duration_Monthly Fox_Infringement Fox_Infringment_level_dropping Fox_Low_Compliant_SiteTitle Fox_Low_Compliant_Title Fox_Site_Monthly Fox_Title_Monthly)
HBOTableList=(FilteringDataMonthly SiteDetailMonthly TitleDetailMonthly)
summitTableList=()
lastMonth=`date -d "1 month ago" +%Y-%m`
today=`date -d now +%Y%m%d%H%M%S`


viacomCount=1
for table in ${viacomTableList[*]}
do
    if [ "${table}" = "PartialTitle_Monthly" ];then
        linesNum=`$mysql_conf_123 DM_VIACOM -e "select count(*) as '' from $table where date_format(Date_ID, '%Y-%m') = '$lastMonth'"`
    else
        linesNum=`$mysql_conf_123 DM_VIACOM -e "select count(*) as '' from $table where YM = '$lastMonth'"`
    fi

    echo "VIACOM Table: ${table}; Row Number:"${linesNum} >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorHasNoData${today}.log
    echo ------------------------------------------------------------------------ >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorHasNoData${today}.log
    echo  >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorHasNoData${today}.log

    if [ $linesNum -ne 0 ];then
        linesNum=1
    fi  
    
    viacomCount=$((${linesNum}*${viacomCount}))
done

manwin2Count=1
for table in ${manwin2TableList[*]}
do
    linesNum=`$mysql_conf_123 DM_MANWIN2 -e "select count(*) as '' from $table where YM = '$lastMonth'"`
    echo "MANWIN2 Table: ${table}; Row Number:"${linesNum} >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorHasNoData${today}.log
    echo ------------------------------------------------------------------------ >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorHasNoData${today}.log
    echo  >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorHasNoData${today}.log
    if [ $linesNum -ne 0 ];then
        linesNum=1
    fi  
    
    manwin2Count=$((${linesNum}*${manwin2Count}))
done

foxCount=1
for table in ${foxTableList[*]}
do
    linesNum=`$mysql_conf_123 DM_FOX -e "select count(*) as '' from $table where YM = '$lastMonth'"`
    echo "FOX Table: ${table}; Row Number:"${linesNum} >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorHasNoData${today}.log
    echo ------------------------------------------------------------------------ >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorHasNoData${today}.log
    echo  >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorHasNoData${today}.log
    if [ $linesNum -ne 0 ];then
        linesNum=1
    fi  
    foxCount=$((${linesNum}*${foxCount}))
done

HBOCount=1
for table in ${HBOTableList[*]}
do
    linesNum=`$mysql_conf_123 DM_HBO -e "select count(*) as '' from $table where YM = '$lastMonth'"`
    echo "HBO Table: ${table}; Row Number:"${linesNum} >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorHasNoData${today}.log
    echo ------------------------------------------------------------------------ >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorHasNoData${today}.log
    echo  >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorHasNoData${today}.log
    if [ $linesNum -ne 0 ];then
        linesNum=1
    fi  
    HBOCount=$((${linesNum}*${HBOCount}))
done

rows=$((${viacomCount}*${manwin2Count}*${foxCount}*${HBOCount}))
if [ ${rows} -ne 0 ];then
    mail -s "Monthly Report Monitor Have Or Not Have Data OK" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn  < /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorHasNoData${today}.log
    python /Job/reportingMonitor/heartbeart.py  dbpc.ops.vobile.org 5800 reporting_portal data_no_data.monthly
else
    mail -s "Monthly Report Monitor Have Or Not Have Data WARNING" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn  < /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorHasNoData${today}.log
fi

echo ${rows}

