#!/bin/bash
#Date: 2016-02-15
#author: 


source /etc/profile
source /Job/MANWIN2/ManWinDaily/Manwin2ETLMonitor.sh
##########################################################################################################

# update WebsiteDomainList
bash /root/data-integration/kitchen.sh -file=/Job/MANWIN2/ManWinDaily/MWD_Files20160114/ManwinDaily_UpdateWebsiteDomainList.kjb
JOBNAME1="ManwinDaily_UpdateWebsiteDomainList"
monitorETLFunc $JOBNAME1
##########################################################################################################

#generate domain list from table websiteDomain
mysql_conf_114="  -h192.168.110.114 -ukettle -pk3UTLe DM_MANWIN2 "
list=`mysql $mysql_conf_114 -e "select websiteDomain as '' from websiteDomainList"`
i=1
domainList=''
len=`echo $list|wc -w`
count=0
for domain in $list
    do
        count=$(($count+1))
        if [ 1 -eq $count ];then
            domain1="$domain', "
            domainList=$domainList$domain1
        elif [ $len -ne $count ];then
            domain1="'$domain', "
            domainList=$domainList$domain1
        else
            domain1="'$domain"
            domainList=$domainList$domain1
        fi
    done

########################################################################################

# delete yesterday data if exists
delete_date=`date -d "now 1 days ago" +%Y-%m-%d`
mysql ${mysql_conf_114} -e "delete from InfringingVideosFound where DateID >= '${delete_date}';delete from InfringingVideosRemoveOrNot where DateID >= '${delete_date}'; delete from NoticeSendDaily where DateID >= '${delete_date}';"

#######################################################################################
# generate new yesterday data
today=`date -d "now 0 days ago" +%Y-%m-%d`" 08:00:00"
yesterday=`date -d "now 1 days ago" +%Y-%m-%d`" 08:00:00"
#bash /root/data-integration/kitchen.sh -file=/Job/MANWIN2/tmpDaily/ManWinDaily.kjb  -param:YESTERDAY="$yesterday" -param:TODAY="$today" -param:WEBSITE_DOMAIN="$domainList"
bash /root/data-integration/kitchen.sh -file=/Job/MANWIN2/ManWinDaily/MWD_Files20160114/ManWinDaily.kjb  -param:YESTERDAY="$yesterday" -param:TODAY="$today" -param:WEBSITE_DOMAIN="$domainList"
JOBNAME2="ManWinDaily"
monitorETLFunc $JOBNAME2

echo $today
echo $yesterday
#echo $domainList
# update display_name
mysql ${mysql_conf_114} -e "update InfringingVideosFound as a , websiteDomainList as b set a.displayName = b.display_name  where a.trackingWebsite_id = b.trackingWebsite_id; update InfringingVideosRemoveOrNot as a , websiteDomainList as b set a.displayName = b.display_name where a.trackingWebsite_id = b.trackingWebsite_id; update NoticeSendDaily as a , websiteDomainList as b set a.displayName = b.display_name where a.trackingWebsite_id = b.trackingWebsite_id; update siteList as a, websiteDomainList as b set a.site = b.display_name where a.trackingWebsite_id = b.trackingWebsite_id;" 
########################################################################################################
#generate Excel file 
dateNow=`date -d 'now' +%Y%m%d`
fileName=`ls /Job/R/MANWIN2/data|grep "ManwinVobileDailyReports_"$dateNow`
cd /Job/R/MANWIN2/data
if [ -n "$fileName" ];then
    rm $fileName 
fi
RLogTime=`date -d "now" +%Y%m%d%H%M%S`

Rscript /Job/R/MANWIN2/script/genExcelManWin2_currentMonthData.R &> /Job/R/MANWIN2/log/genExcelManWin2${RLogTime}.log
monitorRFunc ${RLogTime}
########################################################################################################

# send mail
fileName=`ls /Job/R/MANWIN2/data|grep "ManwinVobileDailyReports_"$dateNow`
cd /Job/R/MANWIN2/data
dateYest=`date -d 'now 1 days ago' +%Y%m%d`
mv $fileName "ManwinVobileDailyReports_"$dateYest".xlsx"
fileName=`ls /Job/R/MANWIN2/data|grep "ManwinVobileDailyReports_"$dateYest`
echo $fileName

file_path="/Job/R/MANWIN2/data/${fileName}"
mail_report_date="`date -d "now 1 day ago" +%Y-%m-%d`"
mail_subject="Manwin_2 Daily Report_`date -d 'now 1 day ago' +%Y.%m.%d`"

if [ -z "$fileName" ];then
    echo "ManWin daily report Excel file task failed"|mail -s "ManWin2 Daily Report send mail Error"$dateNow chen_weijie@vobile.cn
else
    #uuencode  $fileName $fileName | mail -s ManWinDailyReport${dateNow} chen_weijie@vobile.cn
    bash  /root/data-integration/kitchen.sh -file=/Job/MANWIN2/ManWinDaily/MWD_Files20160114/ManWinDailySendMail_commandLineParameter.kjb -param:FILE_PATH=$file_path  -param:REPORT_DATE=${mail_report_date}  -param:SUBJECT="${mail_subject}"

    JOBNAME3="ManWinDailySendMail_commandLineParameter"
    monitorETLFunc $JOBNAME3
fi

