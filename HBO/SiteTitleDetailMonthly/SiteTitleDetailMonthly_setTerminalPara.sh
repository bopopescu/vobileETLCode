#!/bin/bash

#Date: 2015-09-10
#author: cwj

source /etc/profile

for ((i=2;i<=9;i++))
  do
 	startDate=`date -d "2015-09-01 $i months ago" +%Y-%m-%d`
        j=$(($i-1))
	endDate=`date -d "2015-09-01 $j months ago" +%Y-%m-%d`
        lastMonth=`date -d "$startDate" +%Y-%m`
	echo "$startDate ; $endDate ; $lastMonth"
        echo "================================================="
	bash /root/data-integration/kitchen.sh -file=/Job/HBO/SiteTitleDetailMonthly/SDM_STP_Files/SiteTitleDetailMonthly_setTerminalPara.kjb -param:SELF_STARTDATE="$startDate" -param:SELF_ENDDATE="$endDate" -param:LAST_MONTH="$lastMonth"
  done

#-param          = Set a named parameter <NAME>=<VALUE>. For example -param:FILE=customers.csv

