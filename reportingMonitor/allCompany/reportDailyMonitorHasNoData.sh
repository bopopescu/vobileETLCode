#!/bin/bash
#Date: 2015-10-12 15:01:42
#

mysql_conf_123="mysql -h54.67.114.123 -ukettle -pkettle "
mysql_conf_123_arch="mysql -h54.67.114.123 -ukettle -pkettle "

mysql_conf_114="mysql -h192.168.110.114 -ukettle -pk3UTLe "
mysql_conf_114_arch="mysql -h192.168.110.114 -ukettle -pk3UTLe "


viacomTableList=(DurationDistribution_Daily P2PTitle_ByNoticedDate_Daily SelfService_Aggregate_ByNoticedDate TrackingTitle_Daily YouTube_Match_Daily)
manwin2TableList=(InfringingVideosFound InfringingVideosRemoveOrNot NoticeSendDaily)
disneyTableList=(DisneySelfService)
tvbTableList=(UGC_Daily UGC_Picasa_Daily Cyberlocker_Daily Hybrid_Daily P2P_Daily)
yesterday=`date -d "1 days ago" +%Y-%m-%d`
today=`date -d now +%Y%m%d%H%M%S`
currentdate=`date -d now +%Y-%m-%d`
viacomCount=1
for table in ${viacomTableList[*]}
do
    if [ "${table}" = "YouTube_Match_Daily" ];then
        linesNum=`$mysql_conf_123 DM_VIACOM -e "select count(*) as '' from $table where DateID = '$yesterday'"`
    else
        linesNum=`$mysql_conf_123 DM_VIACOM -e "select count(*) as '' from $table where Date_ID = '$yesterday'"`
    fi

    echo "VIACOM Table: ${table}; Row Number:"${linesNum} >> /Job/reportingMonitor/allCompany/log/reportDailyHasNoData${today}.log
    echo ------------------------------------------------------------------------ >> /Job/reportingMonitor/allCompany/log/reportDailyHasNoData${today}.log
    echo  >> /Job/reportingMonitor/allCompany/log/reportDailyHasNoData${today}.log
#reportDailyHasNoData.sh
    if [ $linesNum -ne 0 ];then
        linesNum=1
    fi

    viacomCount=$((${linesNum}*${viacomCount}))
done

manwin2Count=1
for table in ${manwin2TableList[*]}
do
    linesNum=`$mysql_conf_114 DM_MANWIN2 -e "select count(*) as '' from $table where DateID = '$yesterday'"`
    if [ $linesNum -ne 0 ];then
        linesNum=1
    fi

    echo "MANWIN2 Table: ${table}; Row Number:"${linesNum} >> /Job/reportingMonitor/allCompany/log/reportDailyHasNoData${today}.log
    echo ------------------------------------------------------------------------ >> /Job/reportingMonitor/allCompany/log/reportDailyHasNoData${today}.log
    echo  >> /Job/reportingMonitor/allCompany/log/reportDailyHasNoData${today}.log

    manwin2Count=$((${linesNum}*${manwin2Count}))
done

disneyCount=1
for table in ${disneyTableList[*]}
do
    linesNum=`$mysql_conf_123 DM_DISNEY -e "select count(*) as '' from $table where DateID = '$yesterday'"`
    echo "DISNEY Table: ${table}; Row Number:"${linesNum} >> /Job/reportingMonitor/allCompany/log/reportDailyHasNoData${today}.log
    echo ------------------------------------------------------------------------ >> /Job/reportingMonitor/allCompany/log/reportDailyHasNoData${today}.log
    echo  >> /Job/reportingMonitor/allCompany/log/reportDailyHasNoData${today}.log


    if [ $linesNum -ne 0 ];then
        linesNum=1
    fi
    
    disneyCount=$((${linesNum}*${disneyCount}))
done
# TVB
tvbCount=1
for table in ${tvbTableList[*]}
do
    linesNum=`$mysql_conf_123 DM_TVB -e "select count(*) as '' from $table where date_format(ETL_DTE,'%Y-%m-%d') = '$currentdate'"`
    echo "TVB Table: ${table}; Row Number:"${linesNum} >> /Job/reportingMonitor/allCompany/log/reportDailyHasNoData${today}.log
    echo ------------------------------------------------------------------------ >> /Job/reportingMonitor/allCompany/log/reportDailyHasNoData${today}.log
    echo  >> /Job/reportingMonitor/allCompany/log/reportDailyHasNoData${today}.log


    if [ $linesNum -ne 0 ];then
        linesNum=1
    fi

    tvbCount=$((${linesNum}*${tvbCount}))
done    

lines=$((${disneyCount}*${manwin2Count}*${viacomCount}*${tvbCount}))

if [ ${lines} -ne 0 ];then
    mail -s "Daily Report Monitor Have Or Not Have Data OK" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn < /Job/reportingMonitor/allCompany/log/reportDailyHasNoData${today}.log
    python /Job/reportingMonitor/heartbeart.py  dbpc.ops.vobile.org 5800 reporting_portal data_no_data.daily 
else
    mail -s "Daily Report Monitor Have Or Not Have Data WARNING" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn  < /Job/reportingMonitor/allCompany/log/reportDailyHasNoData${today}.log
fi


