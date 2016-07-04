#!/bin/bash
#Date:2015-10-16 14:34:29
#Author: cwj
#Desc: Monthly data is abnormal or not

mysql_conf_123="mysql -h54.67.114.123 -ukettle -pkettle "
#viacomTableList=(Estimated_Summary_Monthly P2PISPSUM_Monthly P2PISPSUM_Yearly PartialTitle_Monthly SiteDetail_Monthly SiteDetail_Yearly SiteInfringements_Monthly TitleDetail_Monthly TitleDetail_Yearly TopTitle_Monthly Torrent_Summary_Monthly Torrent_Summary_Yearly)

viacomTableList=(Estimated_Summary_Monthly P2PISPSUM_Monthly P2PISPSUM_Yearly PartialTitle_Monthly SiteDetail_Monthly SiteDetail_Yearly SiteInfringements_Monthly TitleDetail_Monthly TitleDetail_Yearly TopTitle_Monthly)
manwin2TableList=(Manwin2_Monthly)
foxTableList=(Fox_Complaince_Duration_Monthly Fox_Infringement Fox_Infringment_level_dropping Fox_Low_Compliant_SiteTitle Fox_Low_Compliant_Title Fox_Site_Monthly Fox_Title_Monthly)
HBOTableList=(FilteringDataMonthly SiteDetailMonthly TitleDetailMonthly)
#summitTableList=()
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

##SiteInfringements_Monthly
#Noticed
viacom_SiteInfringements_Monthly_Noticed_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(SUM(InfringingNum_Noticed)*300/(SELECT SUM(InfringingNum_Noticed) FROM SiteInfringements_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1')) AS '' FROM SiteInfringements_Monthly WHERE YM = '$lastMonth';"`


#Reported
viacom_SiteInfringements_Monthly_Reported_abnormalKPI=`$mysql_conf_123 DM_VIACOM -e "SELECT ROUND(SUM(InfringingNum_Reported)*300/(SELECT SUM(InfringingNum_Reported) FROM SiteInfringements_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1')) AS '' FROM SiteInfringements_Monthly WHERE YM = '$lastMonth';"`

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
##Manwin2_Monthly
#TotalSentNum
manwin2_Manwin2_Monthly_TotalSentNum_abnormalKPI=`$mysql_conf_123 DM_MANWIN2 -e "SELECT ROUND(SUM(TotalSentNum)*300/(SELECT SUM(TotalSentNum) FROM Manwin2_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1')) AS '' FROM Manwin2_Monthly WHERE YM = '$lastMonth';"`

#TotalRemoved
manwin2_Manwin2_Monthly_TotalRemoved_abnormalKPI=`$mysql_conf_123 DM_MANWIN2 -e "SELECT ROUND(SUM(TotalRemoved)*300/(SELECT SUM(TotalRemoved) FROM Manwin2_Monthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1')) AS '' FROM Manwin2_Monthly WHERE YM = '$lastMonth';"`

##########################################################################################################
##foxTableList=(Fox_Complaince_Duration_Monthly Fox_Infringement Fox_Infringment_level_dropping Fox_Low_Compliant_SiteTitle Fox_Low_Compliant_Title Fox_Site_Monthly Fox_Title_Monthly)
##Fox_Complaince_Duration_Monthly
#UGC
fox_Fox_Complaince_Duration_Monthly_UGC_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND(SUM(RemovedTotal)*300/(SELECT SUM(RemovedTotal) FROM Fox_Complaince_Duration_Monthly WHERE YM <= '$lastMonth_1' AND YM >= '$lastMonth_3' AND WebsiteType = 'UGC')) AS '' FROM Fox_Complaince_Duration_Monthly WHERE YM = '$lastMonth' AND WebsiteType = 'UGC';"`

#Hybrid
fox_Fox_Complaince_Duration_Monthly_Hybrid_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND(SUM(RemovedTotal)*300/(SELECT SUM(RemovedTotal) FROM Fox_Complaince_Duration_Monthly WHERE YM <= '$lastMonth_1' AND YM >= '$lastMonth_3' AND WebsiteType = 'Hybrid')) AS '' FROM Fox_Complaince_Duration_Monthly WHERE YM = '$lastMonth' AND WebsiteType = 'Hybrid';"`

##Fox_Infringement
fox_Fox_Infringement_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND(SUM(Reported)*100/SUM(Removed)/(SELECT SUM(Reported)/SUM(Removed) FROM Fox_Infringement WHERE YM <= '$lastMonth_1' AND YM >= '$lastMonth_3'))  AS '' FROM Fox_Infringement WHERE YM = '$lastMonth';"`

##Fox_Infringment_level_dropping
fox_Fox_Infringment_level_dropping_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND(SUM(Reported)*100/(SELECT SUM(Reported) FROM Fox_Infringment_level_dropping WHERE  YM <= '$lastMonth_1' AND YM >= '$lastMonth_3' )) AS '' FROM Fox_Infringment_level_dropping WHERE YM = '$lastMonth';"`
##Fox_Low_Compliant_SiteTitle 
if [ "${lastMonth}" = "${YM01}" ];then
    #UGC 1 Month
    fox_Fox_Low_Compliant_SiteTitle_MonthUGC_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND((SELECT SUM(Reported) FROM Fox_Low_Compliant_SiteTitle WHERE YM = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-01') AND WebsiteType = 'UGC')*300/(SELECT SUM(Reported) FROM  Fox_Low_Compliant_SiteTitle WHERE YM = CONCAT(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 YEAR), '%Y'), '-12') AND YMonth >= CONCAT(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 YEAR), '%Y'), '-10') AND YMonth <= CONCAT(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 YEAR), '%Y'), '-12') AND WebsiteType = 'UGC')) AS '';"`
    
    #Hybrid 1 Month
    fox_Fox_Low_Compliant_SiteTitle_MonthHybrid_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND((SELECT SUM(Reported) FROM Fox_Low_Compliant_SiteTitle WHERE YM = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-01') AND WebsiteType = 'Hybrid')*300/(SELECT SUM(Reported) FROM  Fox_Low_Compliant_SiteTitle WHERE YM = CONCAT(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 YEAR), '%Y'), '-12') AND YMonth >= CONCAT(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 YEAR), '%Y'), '-10') AND YMonth <= CONCAT(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 YEAR), '%Y'), '-12') AND WebsiteType = 'Hybrid')) AS '';"`

elif [ "${lastMonth}" = "${YM02}" ];then
    #UGC 2 Month
    fox_Fox_Low_Compliant_SiteTitle_MonthUGC_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND((SELECT SUM(Reported) FROM Fox_Low_Compliant_SiteTitle WHERE YM = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-02') AND YMonth = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-02') AND WebsiteType = 'UGC')*300/(SELECT SUM(Reported) FROM  Fox_Low_Compliant_SiteTitle WHERE ((YM = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-02') AND YMonth = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-01')) OR (YM = CONCAT(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 YEAR), '%Y'), '-12') AND YMonth >= CONCAT(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 YEAR), '%Y'), '-11'))) AND WebsiteType = 'UGC')) AS '';"`

    #Hybrid 2 Month
    fox_Fox_Low_Compliant_SiteTitle_MonthHybrid_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND((SELECT SUM(Reported) FROM Fox_Low_Compliant_SiteTitle WHERE YM = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-02') AND YMonth = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-02') AND WebsiteType = 'Hybrid')*300/(SELECT SUM(Reported) FROM  Fox_Low_Compliant_SiteTitle WHERE ((YM = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-02') AND YMonth = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-01')) OR (YM = CONCAT(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 YEAR), '%Y'), '-12') AND YMonth >= CONCAT(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 YEAR), '%Y'), '-11'))) AND WebsiteType = 'Hybrid')) AS '';"`
    

elif [ "${lastMonth}" = "${YM03}" ];then
    #UGC 3 Month
    fox_Fox_Low_Compliant_SiteTitle_MonthUGC_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND((SELECT SUM(Reported) FROM Fox_Low_Compliant_SiteTitle WHERE YM = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-03') AND YMonth = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-03') AND WebsiteType = 'UGC')*300/(SELECT SUM(Reported) FROM  Fox_Low_Compliant_SiteTitle WHERE ((YM = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-03') AND YMonth = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-02')) OR (YM = CONCAT(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 YEAR), '%Y'), '-12') AND YMonth >= CONCAT(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 YEAR), '%Y'), '-12'))) AND WebsiteType = 'UGC')) AS '';"`

    #Hybrid 3 Month
    fox_Fox_Low_Compliant_SiteTitle_MonthHybrid_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND((SELECT SUM(Reported) FROM Fox_Low_Compliant_SiteTitle WHERE YM = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-03') AND YMonth = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-03') AND WebsiteType = 'Hybrid')*300/(SELECT SUM(Reported) FROM  Fox_Low_Compliant_SiteTitle WHERE ((YM = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-03') AND YMonth = CONCAT(DATE_FORMAT(NOW(), '%Y'), '-02')) OR (YM = CONCAT(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 YEAR), '%Y'), '-12') AND YMonth >= CONCAT(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 YEAR), '%Y'), '-12'))) AND WebsiteType = 'Hybrid')) AS '';"`

else
    #UGC OTHER Month
    fox_Fox_Low_Compliant_SiteTitle_MonthUGC_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND((SELECT SUM(Reported) FROM Fox_Low_Compliant_SiteTitle WHERE YM = '$lastMonth' AND YMonth = '$lastMonth' AND WebsiteType = 'UGC')*300/(SELECT SUM(Reported) FROM  Fox_Low_Compliant_SiteTitle WHERE YM = '$lastMonth' AND YMonth >= '$lastMonth_3' AND YMonth <= '$lastMonth_1' AND WebsiteType = 'UGC')) AS '';"`
    
    #Hybrid OTHER Month
    fox_Fox_Low_Compliant_SiteTitle_MonthHybrid_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND((SELECT SUM(Reported) FROM Fox_Low_Compliant_SiteTitle WHERE YM = '$lastMonth' AND YMonth = '$lastMonth' AND WebsiteType = 'Hybrid')*300/(SELECT SUM(Reported) FROM  Fox_Low_Compliant_SiteTitle WHERE YM = '$lastMonth' AND YMonth >= '$lastMonth_3' AND YMonth <= '$lastMonth_1' AND WebsiteType = 'Hybrid')) AS '';"`

fi



##Fox_Low_Compliant_Title only two monthes data
#Hybrid
fox_Fox_Low_Compliant_Title_Hybrid_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND(SUM(Reported)*100/(SELECT SUM(Reported) FROM Fox_Low_Compliant_Title WHERE YM = '$lastMonth_1' AND WebsiteType = 'Hybrid')) AS '' FROM Fox_Low_Compliant_Title WHERE YM = '$lastMonth' AND WebsiteType = 'Hybrid';"`

#UGC
fox_Fox_Low_Compliant_Title_UGC_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND(SUM(Reported)*100/(SELECT SUM(Reported) FROM Fox_Low_Compliant_Title WHERE YM = '$lastMonth_1' AND WebsiteType = 'UGC')) AS '' FROM Fox_Low_Compliant_Title WHERE YM = '$lastMonth' AND WebsiteType = 'UGC';"`

##Fox_Site_Monthly 
#hybrid
fox_Fox_Site_Monthly_hybrid_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND(SUM(Reported)*300/(SELECT SUM(Reported) FROM Fox_Site_Monthly WHERE YM <= '$lastMonth_1' AND YM >= '$lastMonth_3'  AND WebsiteType = 'Hybrid')) AS '' FROM Fox_Site_Monthly WHERE YM = '$lastMonth' AND WebsiteType = 'Hybrid';"`

#UGC
fox_Fox_Site_Monthly_UGC_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND(SUM(Reported)*300/(SELECT SUM(Reported) FROM Fox_Site_Monthly WHERE YM <= '$lastMonth_1' AND YM >= '$lastMonth_3' AND WebsiteType = 'UGC')) AS '' FROM Fox_Site_Monthly WHERE YM = '$lastMonth' AND WebsiteType = 'UGC';"`

##Fox_Title_Monthly
#Movie
fox_Fox_Title_Monthly_Movie_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND(SUM(Reported)*300/(SELECT SUM(Reported) FROM Fox_Title_Monthly WHERE YM <= '$lastMonth_1' AND YM >= '$lastMonth_3'  AND ContentType = 'Movie')) AS '' FROM Fox_Title_Monthly WHERE YM = '$lastMonth' AND ContentType = 'Movie';"`

#TV
fox_Fox_Title_Monthly_TV_abnormalKPI=`$mysql_conf_123 DM_FOX -e "SELECT ROUND(SUM(Reported)*300/(SELECT SUM(Reported) FROM Fox_Title_Monthly WHERE YM <= '$lastMonth_1' AND YM >= '$lastMonth_3' AND ContentType = 'TV')) AS '' FROM Fox_Title_Monthly WHERE YM = '$lastMonth' AND ContentType = 'TV';"`

##########################################################################################################
##HBOTableList=(FilteringDataMonthly SiteDetailMonthly TitleDetailMonthly)
##FilteringDataMonthly 
HBO_FilteringDataMonthly_abnormalKPI=`$mysql_conf_123 DM_HBO -e "SELECT ROUND(SUM(queries)*300/(SELECT SUM(queries) FROM FilteringDataMonthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1')) AS '' FROM FilteringDataMonthly WHERE YM = '$lastMonth';"`

##SiteDetailMonthly 
#UGC IMOFlag=0
HBO_SiteDetailMonthly_0UGC_abnormalKPI=`$mysql_conf_123 DM_HBO -e "SELECT ROUND(SUM(infringingMatches)*300/(SELECT SUM(infringingMatches) FROM SiteDetailMonthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND  IMOFlag=0 and websiteType= 'UGC' and websiteName= 'total')) AS '' FROM SiteDetailMonthly WHERE YM = '$lastMonth' AND  IMOFlag=0 and websiteType= 'UGC' and websiteName= 'total';"`

#UGC IMOFlag=1
HBO_SiteDetailMonthly_1UGC_abnormalKPI=`$mysql_conf_123 DM_HBO -e "SELECT ROUND(SUM(infringingMatches)*300/(SELECT SUM(infringingMatches) FROM SiteDetailMonthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND  IMOFlag=1 and websiteType= 'UGC' and websiteName= 'total')) AS '' FROM SiteDetailMonthly WHERE YM = '$lastMonth' AND  IMOFlag=1 and websiteType= 'UGC' and websiteName= 'total';"`

#Cyberlocker IMOFlag=0
HBO_SiteDetailMonthly_0Cyberlocker_abnormalKPI=`$mysql_conf_123 DM_HBO -e "SELECT ROUND(SUM(infringingMatches)*300/(SELECT SUM(infringingMatches) FROM SiteDetailMonthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND  IMOFlag=0 and websiteType= 'Cyberlocker' and websiteName= 'total')) AS '' FROM SiteDetailMonthly WHERE YM = '$lastMonth' AND  IMOFlag=0 and websiteType= 'Cyberlocker' and websiteName= 'total';"`

#Cyberlocker IMOFlag=1
HBO_SiteDetailMonthly_1Cyberlocker_abnormalKPI=`$mysql_conf_123 DM_HBO -e "SELECT ROUND(SUM(infringingMatches)*300/(SELECT SUM(infringingMatches) FROM SiteDetailMonthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND  IMOFlag=1 and websiteType= 'Cyberlocker' and websiteName= 'total')) AS '' FROM SiteDetailMonthly WHERE YM = '$lastMonth' AND  IMOFlag=1 and websiteType= 'Cyberlocker' and websiteName= 'total';"`

#Hybrid IMOFlag=1
HBO_SiteDetailMonthly_1Hybrid_abnormalKPI=`$mysql_conf_123 DM_HBO -e "SELECT ROUND(SUM(infringingMatches)*300/(SELECT SUM(infringingMatches) FROM SiteDetailMonthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND  IMOFlag=1 and websiteType= 'Hybrid' and websiteName= 'total')) AS '' FROM SiteDetailMonthly WHERE YM = '$lastMonth' AND  IMOFlag=1 and websiteType= 'Hybrid' and websiteName= 'total';"`

##TitleDetailMonthly
#UGC IMOFlag=0
HBO_TitleDetailMonthly_0UGC_abnormalKPI=`$mysql_conf_123 DM_HBO -e "SELECT ROUND(SUM(infringingMatches)*300/(SELECT SUM(infringingMatches) FROM TitleDetailMonthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND  IMOFlag=0 and websiteType= 'UGC')) AS '' FROM TitleDetailMonthly WHERE YM = '$lastMonth' AND  IMOFlag=0 and websiteType= 'UGC';"`


#UGC IMOFlag=1
HBO_TitleDetailMonthly_1UGC_abnormalKPI=`$mysql_conf_123 DM_HBO -e "SELECT ROUND(SUM(infringingMatches)*300/(SELECT SUM(infringingMatches) FROM TitleDetailMonthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND  IMOFlag=1 and websiteType= 'UGC')) AS '' FROM TitleDetailMonthly WHERE YM = '$lastMonth' AND  IMOFlag=1 and websiteType= 'UGC';"`

#Cyberlocker IMOFlag=0
HBO_TitleDetailMonthly_0Cyberlocker_abnormalKPI=`$mysql_conf_123 DM_HBO -e "SELECT ROUND(SUM(infringingMatches)*300/(SELECT SUM(infringingMatches) FROM TitleDetailMonthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND  IMOFlag=0 and websiteType= 'Cyberlocker')) AS '' FROM TitleDetailMonthly WHERE YM = '$lastMonth' AND  IMOFlag=0 and websiteType= 'Cyberlocker';"`

#Cyberlocker IMOFlag=1
HBO_TitleDetailMonthly_1Cyberlocker_abnormalKPI=`$mysql_conf_123 DM_HBO -e "SELECT ROUND(SUM(infringingMatches)*300/(SELECT SUM(infringingMatches) FROM TitleDetailMonthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND  IMOFlag=1 and websiteType= 'Cyberlocker')) AS '' FROM TitleDetailMonthly WHERE YM = '$lastMonth' AND  IMOFlag=1 and websiteType= 'Cyberlocker';"`

#Hybrid IMOFlag=1
HBO_TitleDetailMonthly_1Hybrid_abnormalKPI=`$mysql_conf_123 DM_HBO -e "SELECT ROUND(SUM(infringingMatches)*300/(SELECT SUM(infringingMatches) FROM TitleDetailMonthly WHERE YM >= '$lastMonth_3' AND YM <= '$lastMonth_1' AND  IMOFlag=1 and websiteType= 'Hybrid')) AS '' FROM TitleDetailMonthly WHERE YM = '$lastMonth' AND  IMOFlag=1 and websiteType= 'Hybrid';"`


paraNameList=(viacom_Estimated_Summary_Monthly_abnormalKPI viacom_P2PISPSUM_Monthly_abnormalKPI viacom_PartialTitle_Monthly_abnormalKPI viacom_SiteDetail_Monthly_UGC_abnormalKPI viacom_SiteDetail_Monthly_Hybrid_abnormalKPI viacom_SiteDetail_Monthly_Cyberlocker_abnormalKPI viacom_SiteDetail_Monthly_linkingsite_abnormalKPI viacom_SiteDetail_Monthly_Torrent_abnormalKPI viacom_SiteDetail_Monthly_searchEngine_abnormalKPI viacom_SiteInfringements_Monthly_Noticed_abnormalKPI viacom_SiteInfringements_Monthly_Reported_abnormalKPI viacom_TitleDetail_Monthly_UGC_abnormalKPI viacom_TitleDetail_Monthly_hybrid_abnormalKPI viacom_TitleDetail_Monthly_Cyberlocker_abnormalKPI viacom_TitleDetail_Monthly_p2p_abnormalKPI viacom_TopTitle_Monthly_Consumption_abnormalKPI viacom_TopTitle_Monthly_Streams_abnormalKPI viacom_TopTitle_Monthly_Downloads_abnormalKPI manwin2_Manwin2_Monthly_TotalSentNum_abnormalKPI manwin2_Manwin2_Monthly_TotalRemoved_abnormalKPI fox_Fox_Complaince_Duration_Monthly_UGC_abnormalKPI fox_Fox_Complaince_Duration_Monthly_Hybrid_abnormalKPI fox_Fox_Infringement_abnormalKPI fox_Fox_Infringment_level_dropping_abnormalKPI fox_Fox_Low_Compliant_SiteTitle_MonthUGC_abnormalKPI fox_Fox_Low_Compliant_SiteTitle_MonthHybrid_abnormalKPI fox_Fox_Low_Compliant_Title_Hybrid_abnormalKPI fox_Fox_Low_Compliant_Title_UGC_abnormalKPI fox_Fox_Site_Monthly_hybrid_abnormalKPI fox_Fox_Site_Monthly_UGC_abnormalKPI fox_Fox_Title_Monthly_Movie_abnormalKPI fox_Fox_Title_Monthly_TV_abnormalKPI HBO_FilteringDataMonthly_abnormalKPI HBO_SiteDetailMonthly_0UGC_abnormalKPI HBO_SiteDetailMonthly_1UGC_abnormalKPI HBO_SiteDetailMonthly_0Cyberlocker_abnormalKPI HBO_SiteDetailMonthly_1Cyberlocker_abnormalKPI HBO_SiteDetailMonthly_1Hybrid_abnormalKPI HBO_TitleDetailMonthly_0UGC_abnormalKPI HBO_TitleDetailMonthly_1UGC_abnormalKPI HBO_TitleDetailMonthly_0Cyberlocker_abnormalKPI HBO_TitleDetailMonthly_1Cyberlocker_abnormalKPI HBO_TitleDetailMonthly_1Hybrid_abnormalKPI)

paraList=(${viacom_Estimated_Summary_Monthly_abnormalKPI} ${viacom_P2PISPSUM_Monthly_abnormalKPI} ${viacom_PartialTitle_Monthly_abnormalKPI} ${viacom_SiteDetail_Monthly_UGC_abnormalKPI} ${viacom_SiteDetail_Monthly_Hybrid_abnormalKPI} ${viacom_SiteDetail_Monthly_Cyberlocker_abnormalKPI} ${viacom_SiteDetail_Monthly_linkingsite_abnormalKPI} ${viacom_SiteDetail_Monthly_Torrent_abnormalKPI} ${viacom_SiteDetail_Monthly_searchEngine_abnormalKPI} ${viacom_SiteInfringements_Monthly_Noticed_abnormalKPI} ${viacom_SiteInfringements_Monthly_Reported_abnormalKPI} ${viacom_TitleDetail_Monthly_UGC_abnormalKPI} ${viacom_TitleDetail_Monthly_hybrid_abnormalKPI} ${viacom_TitleDetail_Monthly_Cyberlocker_abnormalKPI} ${viacom_TitleDetail_Monthly_p2p_abnormalKPI} ${viacom_TopTitle_Monthly_Consumption_abnormalKPI} ${viacom_TopTitle_Monthly_Streams_abnormalKPI} ${viacom_TopTitle_Monthly_Downloads_abnormalKPI} ${manwin2_Manwin2_Monthly_TotalSentNum_abnormalKPI} ${manwin2_Manwin2_Monthly_TotalRemoved_abnormalKPI} ${fox_Fox_Complaince_Duration_Monthly_UGC_abnormalKPI} ${fox_Fox_Complaince_Duration_Monthly_Hybrid_abnormalKPI} ${fox_Fox_Infringement_abnormalKPI} ${fox_Fox_Infringment_level_dropping_abnormalKPI} ${fox_Fox_Low_Compliant_SiteTitle_MonthUGC_abnormalKPI} ${fox_Fox_Low_Compliant_SiteTitle_MonthHybrid_abnormalKPI} ${fox_Fox_Low_Compliant_Title_Hybrid_abnormalKPI} ${fox_Fox_Low_Compliant_Title_UGC_abnormalKPI} ${fox_Fox_Site_Monthly_hybrid_abnormalKPI} ${fox_Fox_Site_Monthly_UGC_abnormalKPI} ${fox_Fox_Title_Monthly_Movie_abnormalKPI} ${fox_Fox_Title_Monthly_TV_abnormalKPI} ${HBO_FilteringDataMonthly_abnormalKPI} ${HBO_SiteDetailMonthly_0UGC_abnormalKPI} ${HBO_SiteDetailMonthly_1UGC_abnormalKPI} ${HBO_SiteDetailMonthly_0Cyberlocker_abnormalKPI} ${HBO_SiteDetailMonthly_1Cyberlocker_abnormalKPI} ${HBO_SiteDetailMonthly_1Hybrid_abnormalKPI} ${HBO_TitleDetailMonthly_0UGC_abnormalKPI} ${HBO_TitleDetailMonthly_1UGC_abnormalKPI} ${HBO_TitleDetailMonthly_0Cyberlocker_abnormalKPI} ${HBO_TitleDetailMonthly_1Cyberlocker_abnormalKPI} ${HBO_TitleDetailMonthly_1Hybrid_abnormalKPI})
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
    mail -s "Report Monthly Monitor Data abnormal is OK" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn < /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorDataAbnormal${today}.log
    python /Job/reportingMonitor/heartbeart.py  dbpc.ops.vobile.org 5800 reporting_portal data_abnormal.monthly
else
    mail -s "Report Monthly Monitor Data abnormal WARNING" chen_weijie@vobile.cn du_li@vobile.cn sun_cong@vobile.cn < /Job/reportingMonitor/allCompany/log/reportMonthlyMonitorDataAbnormal${today}.log
fi

