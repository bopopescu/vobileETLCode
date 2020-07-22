#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime


conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2')

#matchedURLs / matches with notices sent / removed
cur=conn.cursor()
cur.execute('select date(c.created_at), count(distinct a.id) Matched_URLs, sum(if(c.count_send_notice > 0,1,0)) send_Notices, sum(if(c.count_send_notice > 0 and c.takeoff_time > 0,1,0)) Removed from matchedVideo a, matchedVideo_linkURL c where a.company_id = 14 and a.hide_flag = 2 and c.matchedVideo_id = a.id and c.company_id = 14 and a.trackingWebsite_id = 54 and c.created_at >= "2015-07-01" and c.created_at < "2016-03-01" group by 1')

rows = cur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)    
cur=conn.cursor()
conn.select_db('DW_VTMetrics')


for e in rows:
    insert = "insert into VTMetricsReport(company_id,website_type,reportedDate,matchedURLs_IPs,matchedWithNoticeSent,removed) values('%s','%s','%s','%s','%s','%s')"  %(14,'Torrent Linking Sites',e[0],e[1],e[2],e[3])
    cur.execute(insert)
              
conn.commit()
cur.close()

conn.close()
