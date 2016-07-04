#!/bin/bash
#Date: 2015-09-10
#author: 


#generate domain list from table websiteDomain
mysql_conf_123=" -h54.67.114.123 -ukettle -pkettle DM_MANWIN2 "
list=`mysql $mysql_conf_123 -e "select websiteDomain as '' from websiteDomainList"`
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

# domainList="bangyoulater.com', 'cliphunter.com',  'drtuber.com', 'empflix.com', 'eporner.com', 'fapdu.com', 'gayforit.com', 'gaywatch.com',   'hardsextube.com', 'itsallgay.com', 'nuvid.com', 'playvid.com',  'porndig.com','ornmaki.com', 'pornoxo.com','serviporno.com','slutload.com', 'spankbang.com','vporn.com','xhamster.com','xvideos.com', 'xxxbunker.com', 'youjizz.com', 'Tubenn.com', 'sexu.com', 'tube8.com', 'redtube.com', 'youporn.com','keezmovies.com', 'pornhub.com', 'gaytube.com', 'xtube.com',   'extremetube.com',  'spankwire.com"

# history data
source /etc/profile
for ((i=0;i<0;i++))
  do
     today=`date -d "$i days ago" +%Y-%m-%d`" 08:00:00"
     j=$(($i+1))
     yesterday=`date -d "now $j  days ago" +%Y-%m-%d`" 08:00:00"
     echo "================================================="

     bash /root/data-integration/kitchen.sh -file=/Job/MANWIN2/ManWinDaily/MWD_Files/ManWinDaily.kjb  -param:YESTERDAY="$yesterday" -param:TODAY="$today" -param:WEBSITE_DOMAIN="$domainList"

     if [ "$yesterday" = "2015-09-25 08:00:00" ]
     then
         break
     fi
  done

# yesterday data
today=`date -d "now" +%Y-%m-%d`" 08:00:00"
yesterday=`date -d "now 1 days ago" +%Y-%m-%d`" 08:00:00"
#bash /root/data-integration/kitchen.sh -file=/Job/MANWIN2/ManWinDaily/MWD_Files/ManWinDaily.kjb  -param:YESTERDAY="$yesterday" -param:TODAY="$today" -param:WEBSITE_DOMAIN="$domainList"

#monitor ManWin2 Daily Report JOB


#generate Excel file
dateNow=`date -d 'now' +%Y%m%d`
fileName=`ls /Job/MANWIN2/ManWinDaily/sql2excel/data|grep "ManwinVobileDailyReports_"$dateNow`
cd /Job/MANWIN2/ManWinDaily/sql2excel/data
if [ -n "$fileName" ]
 then
    rm $fileName
 fi
cd /Job/MANWIN2/ManWinDaily/sql2excel
java -jar sql2excel.jar


# send mail
fileName=`ls /Job/MANWIN2/ManWinDaily/sql2excel/data|grep "ManwinVobileDailyReports_"$dateNow`
cd /Job/MANWIN2/ManWinDaily/sql2excel/data
dateYest=`date -d 'now 1 days ago' +%Y%m%d`
mv $fileName "ManwinVobileDailyReports_"$dateYest".xls"
fileName=`ls /Job/MANWIN2/ManWinDaily/sql2excel/data|grep "ManwinVobileDailyReports_"$dateYest`
if [ -z "$fileName" ];then
    echo "ManWin daily report Excel file task failed"|mail -s "ManWin2 Daily Report send mail Error"$dateNow chen_weijie@vobile.cn 
else
    #uuencode  $fileName $fileName | mail -s ManWinDailyReport"$dateNow" chen_weijie@vobile.cn
    #bash  /root/data-integration/kitchen.sh -file=/Job/MANWIN2/ManWinDaily/sendMail/ManWinDailySendMail.kjb
    mysql ${mysql_conf_123} -e "select * from KLog where JOBNAME = 'ManWinDailySendMail' and date_format(ENDDATE, "%Y-%m-%d") = date_format(now(), "%Y-%m-%d")  order by ENDDATE desc limit 1" > /Job/MANWIN2/ManWinDaily/log/ManWinDailySendMail.log
    if [ -n "$(grep -i error /Job/MANWIN2/ManWinDaily/log/ManWinDailySendMail.log| grep -v 'ERRORS: 0')" ];then
       cat /Job/MANWIN2/ManWinDaily/log/ManWinDailySendMail.log|mail -s "ManWin2 Daily Report send mail failed"$dateNow chen_weijie@vobile.cn
    else
       echo "Good day, commander. Manwin2 mail OK. What a beautiful day."|mail -s "ManWin2 Daily Report ETL OK "$dateNow chen_weijie@vobile.cn
    fi
fi 


