#!/bin/bash
#author: suncong
#date: 2016-01-05

cd /Job/NETFLIX2

python JobNetflix2.py

DateID=`date +%Y-%m-%d`

mysql -h54.67.114.123 -ukettle -pkettle DM_NETFLIX -e"set names utf8; select title,linkingSite,linkingURL from torrnetSiteMatch where DateID='${DateID}'" > 'netflix2_weekly_report.csv'

python sendMail.py
