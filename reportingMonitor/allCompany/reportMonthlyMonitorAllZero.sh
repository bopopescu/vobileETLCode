#!/bin/bash
#Date:2015-10-16 14:34:29
#Author: cwj
#

mysql_conf_123="mysql -h54.67.114.123 -ukettle -pkettle "
#viacomTableList=(Estimated_Summary_Monthly P2PISPSUM_Monthly P2PISPSUM_Yearly PartialTitle_Monthly SiteDetail_Monthly SiteDetail_Yearly SiteInfringements_Monthly TitleDetail_Monthly TitleDetail_Yearly TopTitle_Monthly Torrent_Summary_Monthly Torrent_Summary_Yearly)

viacomTableList=(Estimated_Summary_Monthly P2PISPSUM_Monthly P2PISPSUM_Yearly PartialTitle_Monthly SiteDetail_Monthly SiteDetail_Yearly SiteInfringements_Monthly TitleDetail_Monthly TitleDetail_Yearly TopTitle_Monthly)
manwin2TableList=(Manwin2_Monthly)
#CBSTableList=()
foxTableList=(Fox_Complaince_Duration_Monthly Fox_Infringement Fox_Infringment_level_dropping Fox_Low_Compliant_SiteTitle Fox_Low_Compliant_Title Fox_Site_Monthly Fox_Title_Monthly)
HBOTableList=(FilteringDataMonthly SiteDetailMonthly TitleDetailMonthly)
summitTableList=()
lastMonth=`date -d "1 month ago" +%Y-%m`

#
viacom_Estimated_Summary_MonthlyKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(if(ReportedViews is null,0, ReportedViews))+sum(if(EstimatedNum is null, 0, EstimatedNum))) as '' from Estimated_Summary_Monthly where YM = '$lastMonth'"`

viacom_P2PISPSUM_MonthlyKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(InfringingIPs)) as '' from P2PISPSUM_Monthly where YM = '$lastMonth'"`

viacom_P2PISPSUM_YearlyKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(InfringingIPs)) as '' from P2PISPSUM_Yearly where YM = '$lastMonth'"`

viacom_PartialTitle_MonthlyKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(InfringingFiles+InfringingViews+InfringingIPs)) as '' from PartialTitle_Monthly where date_format(Date_ID, '%Y-%m') = '$lastMonth'"`

viacom_SiteDetail_MonthlyKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(InfringingNum)) as '' from SiteDetail_Monthly where YM = '$lastMonth'"`

viacom_SiteDetail_YearlyKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(InfringingNum)) as '' from SiteDetail_Yearly where YM = '$lastMonth'"`

viacom_SiteInfringements_MonthlyKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(InfringingNum_Noticed+InfringingNum_Reported)) as '' from SiteInfringements_Monthly where YM = '$lastMonth'"`

viacom_TitleDetail_MonthlyKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(InfringingNum)) as '' from TitleDetail_Monthly where YM = '$lastMonth'"`

viacom_TitleDetail_YearlyKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(InfringingNum)) as '' from TitleDetail_Yearly where YM = '$lastMonth'"`
viacom_TopTitle_MonthlyKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(Estimated_Consumption)) as '' from TopTitle_Monthly where YM = '$lastMonth'"`

#viacom_Torrent_Summary_MonthlyKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(InfringingNum)) as '' from Torrent_Summary_Monthly where YM = '$lastMonth'"`

#viacom_Torrent_Summary_YearlyKPISum=`$mysql_conf_123 DM_VIACOM -e "select round(sum(InfringingNum)) as '' from Torrent_Summary_Yearly where YM = '$lastMonth'"`

manwin2_Manwin2_MonthlyKPISum=`$mysql_conf_123 DM_MANWIN2 -e "select round(sum(TotalSentNum)) as '' from Manwin2_Monthly where YM = '$lastMonth'"`

Fox_Complaince_Duration_MonthlyKPISum=`$mysql_conf_123 DM_FOX -e "select round(sum(RemovedTotal)) as '' from Fox_Complaince_Duration_Monthly  where YM = '$lastMonth'"`

Fox_InfringementKPISum=`$mysql_conf_123 DM_FOX -e "select round(sum(Removed+Reported)) as '' from Fox_Infringement  where YM = '$lastMonth'"`

Fox_Infringment_level_droppingKPISum=`$mysql_conf_123 DM_FOX -e "select round(sum(Reported)) as '' from Fox_Infringment_level_dropping  where YM = '$lastMonth'"`

Fox_Low_Compliant_SiteTitleKPISum=`$mysql_conf_123 DM_FOX -e "select round(sum(Reported+Removed)) as '' from Fox_Low_Compliant_SiteTitle  where YM = '$lastMonth'"`

Fox_Low_Compliant_TitleKPISum=`$mysql_conf_123 DM_FOX -e "select round(sum(Removed+Reported)) as '' from Fox_Low_Compliant_Title  where YM = '$lastMonth'"`

Fox_Site_MonthlyKPISum=`$mysql_conf_123 DM_FOX -e "select round(sum(RemovedTotal+Reported)) as '' from Fox_Site_Monthly  where YM = '$lastMonth'"`

Fox_Title_MonthlyKPISum=`$mysql_conf_123 DM_FOX -e "select round(sum(Removed+Reported)) as '' from Fox_Title_Monthly  where YM = '$lastMonth'"`

HBO_FilteringDataMonthly_KPISum=`$mysql_conf_123 DM_HBO -e "select round(sum(queries+TVMatches+movieMatches)) as '' from FilteringDataMonthly  where YM = '$lastMonth'"`

HBO_SiteDetailMonthly_KPISum=`$mysql_conf_123 DM_HBO -e "select round(sum(infringingMatches+removed)) as '' from SiteDetailMonthly  where YM = '$lastMonth'"`

HBO_TitleDetailMonthly_KPISum=`$mysql_conf_123 DM_HBO -e "select round(sum(infringingMatches+removed)) as '' from TitleDetailMonthly  where YM = '$lastMonth'"`

paraList=($viacom_P2PISPSUM_MonthlyKPISum $viacom_P2PISPSUM_YearlyKPISum $viacom_PartialTitle_MonthlyKPISum $viacom_SiteDetail_MonthlyKPISum $viacom_SiteDetail_YearlyKPISum $viacom_SiteInfringements_MonthlyKPISum $viacom_TitleDetail_MonthlyKPISum $viacom_TitleDetail_YearlyKPISum $viacom_TopTitle_MonthlyKPISum $manwin2_Manwin2_MonthlyKPISum $Fox_Complaince_Duration_MonthlyKPISum $Fox_InfringementKPISum $Fox_Infringment_level_droppingKPISum $Fox_Low_Compliant_SiteTitleKPISum $Fox_Low_Compliant_TitleKPISum $Fox_Site_MonthlyKPISum $Fox_Title_MonthlyKPISum $HBO_FilteringDataMonthly_KPISum $HBO_SiteDetailMonthly_KPISum $HBO_TitleDetailMonthly_KPISum)

paraNameList=(viacom_P2PISPSUM_MonthlyKPISum viacom_P2PISPSUM_YearlyKPISum viacom_PartialTitle_MonthlyKPISum viacom_SiteDetail_MonthlyKPISum viacom_SiteDetail_YearlyKPISum viacom_SiteInfringements_MonthlyKPISum viacom_TitleDetail_MonthlyKPISum viacom_TitleDetail_YearlyKPISum viacom_TopTitle_MonthlyKPISum manwin2_Manwin2_MonthlyKPISum Fox_Complaince_Duration_MonthlyKPISum Fox_InfringementKPISum Fox_Infringment_level_droppingKPISum Fox_Low_Compliant_SiteTitleKPISum Fox_Low_Compliant_TitleKPISum Fox_Site_MonthlyKPISum Fox_Title_MonthlyKPISum HBO_FilteringDataMonthly_KPISum HBO_SiteDetailMonthly_KPISum HBO_TitleDetailMonthly_KPISum)

echo ${#paraList[@]}
echo ${#paraNameList[@]}

today=`date -d now +%Y%m%d%H%M%S`
idx=0
allZeroKPI=1
for para in ${paraList[*]}
do
    echo "${paraNameList[$idx]} monitor sum(KPI): ${paraList[$idx]}" >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorAllZero${today}.log
    echo --------------------------------------------------------------- >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorAllZero${today}.log
    echo  >> /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorAllZero${today}.log

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
    mail -s "Monthly Report Monitor All Zero IS OK" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn < /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorAllZero${today}.log
    python /Job/reportingMonitor/heartbeart.py  dbpc.ops.vobile.org 5800 reporting_portal data_all_zero.monthly
else
    mail -s "Monthly Report Monitor All Zero WARNING" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn < /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorAllZero${today}.log
fi

