#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#Low-compliant Sites-Summary
conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 

Lowcur=conn.cursor()
Lowcur.execute('set @baseTime = now();select @baseTime, sum(if(compliant <= 80,1,0)) Low_compliant_Sites_Num, sum(if(compliant <= 80,1,0)) / count(*) * 100 Low_compliant_Sites_per from (select trackingWebsite_id, sum(if (takeoff_time > 0,1,0)) / count(*) * 100 compliant from matchedVideo where  hide_flag = 2 and count_send_notice > 0 and first_send_notice_date >= @baseTime - interval 80 day and first_send_notice_date < @baseTime  group by 1) tmp_a')

Lowrows = Lowcur.fetchall()

#Reported Matches - UGC;Matches with Notices Sent - UGC
UGCcur=conn.cursor()
UGCcur.execute('set @baseTime = now();select b.website_type, date(a.created_at), count(a.id) Matched_URLs, sum(if(a.count_send_notice > 0,1,0)) send_Notices from matchedVideo a, mddb.trackingWebsite b where a.trackingWebsite_id = b.id  and b.website_type in ("ugc") and a.created_at >= date(@baseTime) - interval 80 day and a.created_at < date(@baseTime)  group by 1,2')

UGCrows = UGCcur.fetchall()

#Reported Matches - Hybrid;Matches with Notices Sent - Hybrid
Hycur=conn.cursor()
Hycur.execute('set @baseTime = now();select b.website_type, date(a.created_at), count(a.id) Matched_URLs, sum(if(a.count_send_notice > 0,1,0)) send_Notices from matchedVideo a, mddb.trackingWebsite b where a.trackingWebsite_id = b.id and b.website_type in ("hybrid") and a.created_at >= date(@baseTime) - interval 80 day and a.created_at < date(@baseTime)  group by 1,2')
 
Hyrows = Hycur.fetchall()


#Reported Matches - Cyberlocker;Matches with Notices Sent - Cyberlocker
cycur=conn.cursor()
cycur.execute('set @baseTime = now(); select date(a.created_at), count(c.id) Matched_URLs, sum(if(a.count_send_notice > 0,1,0)) send_Notices from matchedVideo a, matchedFileItem c where c.matchedFile_id = a.matchedFile_id and a.created_at >= date(@baseTime) - interval 80 day and a.created_at < date(@baseTime)  group by 1')

cyrows = cycur.fetchall()


#Reported Linking URLs - Search Engine;URLs with Notices Sent - Search Engine
secur=conn.cursor()
secur.execute('set @baseTime = now(); select date(c.created_at), count(c.id) Matched_URLs, sum(if(a.count_send_notice > 0,1,0)) send_Notices from matchedVideo a, matchedVideo_searchEngine c where c.matchedVideo_id = a.id and c.created_at >= date(@baseTime) - interval 80 day and c.created_at < date(@baseTime) group by 1')

serows = secur.fetchall()

#Reported Linking URLs - Linking Site;URLs with Notices Sent - Linking Site

lscur=conn.cursor()
lscur.execute('set @baseTime = now();select date(c.created_at), count(c.id) Matched_URLs, sum(if(c.count_send_notice > 0,1,0)) send_Notices from matchedVideo a, matchedVideo_linkURL c where c.matchedVideo_id = a.id and c.created_at >= "2016-01-01" and c.created_at >= date(@baseTime) - interval 80 day and c.created_at < date(@baseTime) group by 1')

lsrows = lscur.fetchall()

#IPs - P2P;Notices Sent - P2P

pcur=conn.cursor()
pcur.execute('set @baseTime = now();select date(a.created_at), sum(a.view_count) as IPs, sum(a.count_send_notice) Notices_Sent from matchedVideo a, mddb.trackingWebsite b where  a.trackingWebsite_id = b.id and b.website_type in ("p2p") and a.created_at >= date(@baseTime) - interval 80 day and a.created_at < date(@baseTime)  group by 1')

prows = pcur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)    
Lowcur=conn.cursor()
UGCcur=conn.cursor()
Hycur=conn.cursor()
cycur=conn.cursor()
secur=conn.cursor()
lscur=conn.cursor()
pcur=conn.cursor()

conn.select_db('DW_VTMetrics')


for e in Lowrows:
    Low_insert = "insert into TempMetricsReportAll(reportedDate,Low_compliant_Sites_Num,Low_compliant_Sites_Percent) values('%s','%s','%s')" %(e[0],e[1],e[2])
    Lowcur.execute(Low_insert)
              
conn.commit()
Lowcur.close()

for e in UGCrows:
    UGC_update = "update VTMetricsReport set Reported_Matches_UGC = '%s', Matches_Notices_Sent_UGC = '%s' where reportedDate in ('%s')" %(e[2],e[3],e[1])
    UGCcur.execute(UGC_update)
    conn.commit()

UGCcur.close()

for e in Hyrows:

    Hybrid_update = "update VTMetricsReport set Reported_Matches_Hybrid = '%s', Matches_Notices_Sent_Hybrid = '%s' where reportedDate in ('%s')" %(e[2],e[3],e[1])
    Hycur.execute(Hybrid_update)    
    conn.commit()

Hycur.close()

for e in cyrows:
   cyberlocker_update = "update VTMetricsReport set Reported_Matches_Cyberlocker = '%s', Matches_Notices_Sent_Cyberlocker = '%s' where reportedDate in ('%s')" %(e[1],e[2],e[0])
   cycur.execute(cyberlocker_update)
   conn.commit()

cycur.close()

for e in serows:

    search_update = "update VTMetricsReport set Reported_Linking_URLs_Search_Engine = '%s', URLs_Notices_Sent_Search_Engine = '%s' where reportedDate in ('%s')" %(e[1],e[2],e[0])
    secur.execute(search_update)
    conn.commit()

secur.close()

for e in lsrows:

    linking_update = "update VTMetricsReport set Reported_Linking_URLs_Linking_Sites = '%s', URLs_Notices_Sent_Linking_Sites = '%s' where reportedDate in ('%s')" %(e[1],e[2],e[0])
    lscur.execute(lingking_update)
    conn.commit()

lscur.close()

for e in prows:

    p2p_update = "update VTMetricsReport set IPs_P2P = '%s', Notices_Sent_P2P = '%s' where reportedDate in ('%s')" %(e[1],e[2],e[0])
    pcur.execute(p2p_update)
    conn.commit()

pcur.close()
conn.close()
