#!/bin/bash
#Date: 2015-11-3 16:49:11
#

mysql_conf_114="mysql -h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM " 
CMS_start_month="2014-12"
start_month=`date -d '12 month ago' +%Y-%m`
last_month=`date -d '2 month ago' +%Y-%m`
#echo ${last_month}

curr_InfringingNum_Yearly_Youtube=`${mysql_conf_114} -e "select infringingNum as '' from SiteDetail_Yearly where YM = '${last_month}' and websiteName = 'YouTube' and websiteType='UGC'"`
echo curr_InfringingNum_Yearly_Youtube=${curr_InfringingNum_Yearly_Youtube}

curr_InfringingNum_Yearly_Total=`${mysql_conf_114} -e "select infringingNum as '' from SiteDetail_Yearly where YM = '${last_month}' and websiteName = 'Total' and websiteType='UGC'"`
echo curr_InfringingNum_Yearly_Total=${curr_InfringingNum_Yearly_Total}

curr_TakedownNum_Yearly_Youtube=`${mysql_conf_114} -e "select TakedownNum as '' from SiteDetail_Yearly where YM = '${last_month}' and websiteName = 'YouTube' and websiteType='UGC'"`

echo curr_TakedownNum_Yearly_Youtube=${curr_TakedownNum_Yearly_Youtube}

curr_TakedownNum_Yearly_Total=`${mysql_conf_114} -e "select TakedownNum as '' from SiteDetail_Yearly where YM = '${last_month}' and websiteName = 'Total' and websiteType='UGC'"`
echo curr_TakedownNum_Yearly_Total=${curr_TakedownNum_Yearly_Total}

##------------------------------
curr_InfringingNum_Monthly_Youtube=`${mysql_conf_114} -e "select infringingNum as '' from SiteDetail_Monthly where YM = '${last_month}' and websiteName = 'YouTube' and websiteType='UGC'"`

echo curr_InfringingNum_Monthly_Youtube=${curr_InfringingNum_Monthly_Youtube}

curr_InfringingNum_Monthly_Total=`${mysql_conf_114} -e "select infringingNum as '' from SiteDetail_Monthly where YM = '${last_month}' and websiteName = 'Total' and websiteType='UGC'"`

echo curr_InfringingNum_Monthly_Total=${curr_InfringingNum_Monthly_Total}

curr_TakedownNum_Monthly_Youtube=`${mysql_conf_114} -e "select TakedownNum as '' from SiteDetail_Monthly where YM = '${last_month}' and websiteName = 'YouTube' and websiteType='UGC'"`

echo curr_TakedownNum_Monthly_Youtube=${curr_TakedownNum_Monthly_Youtube}

curr_TakedownNum_Monthly_Total=`${mysql_conf_114} -e "select TakedownNum as '' from SiteDetail_Monthly where YM = '${last_month}' and websiteName = 'Total' and websiteType='UGC'"`

echo curr_TakedownNum_Monthly_Total=${curr_TakedownNum_Monthly_Total}

last_twelve_month_CMS=`${mysql_conf_114} -e "select sum(InfringingNums) AS '' from CMS_Youtube_Info where date_format(date_id, '%Y-%m') >= '${CMS_start_month}' and date_format(date_id, '%Y-%m') >= '${start_month}' and date_format(date_id, '%Y-%m') <= '${last_month}'"`

echo last_twelve_month_CMS=${last_twelve_month_CMS}

current_month_CMS=`${mysql_conf_114} -e "select sum(InfringingNums) AS '' from CMS_Youtube_Info where date_format(date_id, '%Y-%m') = '${last_month}'"`

echo current_month_CMS=${current_month_CMS}

TakedownNum_youtube_Yearly=$((${curr_TakedownNum_Yearly_Youtube}+${last_twelve_month_CMS}))
TakedownNum_total_Yearly=$((${curr_TakedownNum_Yearly_Total}+${last_twelve_month_CMS}))

TakedownNum_youtube_Monthly=$((${curr_TakedownNum_Monthly_Youtube}+${current_month_CMS}))
TakedownNum_total_Monthly=$((${curr_TakedownNum_Monthly_Total}+${current_month_CMS}))

echo "TakedownNum_youtube_Yearly="${TakedownNum_youtube_Yearly}
echo "TakedownNum_total_Yearly="${TakedownNum_total_Yearly}

echo "TakedownNum_youtube_Monthly="${TakedownNum_youtube_Monthly}
echo "TakedownNum_total_Monthly="${TakedownNum_total_Monthly}
