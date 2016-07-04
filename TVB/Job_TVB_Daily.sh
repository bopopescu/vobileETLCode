#!/bin/bash

#Date: 2015-12-25
#author: duli

source /etc/profile
bash /root/data-integration/kitchen.sh -file=/Job/TVB/Job_TVB_Daily.kjb

cd /Job/TVB/tvb_daily/

mysql -h54.67.114.123 -ukettle -pkettle DM_TVB << EOF > UGC_Daily.csv
 set names utf8;
 select ReportDate, VideoTitle, ifnull(WebsiteURL, 'None') as WebsiteURL, HostingURL, ClipTitle, if(Views = 0, '-', Views) as Views, ifnull(Poster, '') as Poster, MatchType, SEC_TO_TIME(ClipDuration) as ClipDuration, VPercent, APercent, date_format(date(PostDate), '%m/%d/%Y') as PostDate, Status, TakeDownSentNO, ifnull(LastSentDateTime, '') as LastSentDateTime, ifnull(TakeoffTime, '') as TakeoffTime, Site, SiteHost, DataCenter, Classification from UGC_Daily order by ReportDate;
 exit
EOF
mysql -h54.67.114.123 -ukettle -pkettle DM_TVB << EOF > UGC_Picasa_Daily.csv
 set names utf8;
 select ReportDate, VideoTitle, ifnull(WebsiteURL, 'None') as WebsiteURL, HostingURL, ClipTitle, if(Views = 0, '-', Views) as Views, ifnull(Poster, '') as Poster, MatchType, SEC_TO_TIME(ClipDuration) as ClipDuration, VPercent, APercent, date_format(date(PostDate), '%m/%d/%Y') as PostDate, Status, TakeDownSentNO, ifnull(LastSentDateTime, '') as LastSentDateTime, ifnull(TakeoffTime, '') as TakeoffTime, Site, SiteHost, DataCenter, Classification from UGC_Picasa_Daily order by ReportDate;
 exit
EOF
mysql -h54.67.114.123 -ukettle -pkettle DM_TVB  << EOF > Cyberlocker_Daily.csv
 set names utf8;
 select ReportDate, VideoTitle, ifnull(WebsiteURL, 'None') as WebsiteURL, HostingURL, ClipTitle, concat(FileSize,'MB') as FileSize, Status, TakeDownSentNO, ifnull(LastSentDateTime, '') as LastSentDateTime, ifnull(TakeoffTime, '') as TakeoffTime, Site, SiteHost, DataCenter, Classification from Cyberlocker_Daily order by ReportDate;
 exit
EOF
mysql -h54.67.114.123 -ukettle -pkettle DM_TVB  << EOF > Hybrid_Daily.csv
 set names utf8;
 select ReportDate, VideoTitle, ifnull(WebsiteURL, 'None') as WebsiteURL, HostingURL, ClipTitle, MatchType, VPercent, APercent, Status, TakeDownSentNO, ifnull(LastSentDateTime, '') as LastSentDateTime, ifnull(TakeoffTime, '') as TakeoffTime, Site, SiteHost, DataCenter, Classification from Hybrid_Daily order by ReportDate;
 exit
EOF
mysql -h54.67.114.123 -ukettle -pkettle DM_TVB  << EOF > P2P_Daily.csv
 set names utf8;
 select ReportDate, VideoTitle, ifnull(WebsiteURL, 'None') as WebsiteURL, SourceURL, FileName, Infringers, MatchType, SEC_TO_TIME(ClipDuration) as ClipDuration, VPercent, APercent, if(Status='sent-Not Removed','Take-down Sent',Status) as Status, TakeDownSentNO, ifnull(LastSentDateTime, '') as LastSentDateTime, Protocol, ISPCountry, Classification from P2P_Daily order by ReportDate;
 exit
EOF

zip -r tvb_daily.zip ../tvb_daily/

ftp -n<<- EOF
open 206.99.94.101
user reporter 3on\`Twyop9
ascii
cd upload
put tvb_daily.zip
bye
EOF

