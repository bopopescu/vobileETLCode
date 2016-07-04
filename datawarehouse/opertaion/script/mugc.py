#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

start = 0
stop = 1
for i in range(1,80):

#Low-compliant Sites-Summary
 conn=MySQLdb.connect(host='eqx-vtweb-slave-db',user='kettle',passwd='k3UTLe',port=3306)
 conn.select_db('tracker2') 

 cur=conn.cursor()
 get_data = "select b.website_type, date(a.created_at), count(a.id) Matched_URLs, sum(if(a.count_send_notice > 0,1,0)) send_Notices from matchedVideo a, mddb.trackingWebsite b where a.trackingWebsite_id = b.id  and b.website_type in ('%s') and a.created_at >= date_sub(curdate(),interval '%s' day) and a.created_at < date_sub(curdate(),interval '%s' day)  group by 1,2" %('ugc',stop,start)
 print get_data
 cur.execute(get_data)
 rows = cur.fetchall()

 conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

 cur=conn.cursor()
 conn.select_db('DW_VTMetrics')

 for e in rows:
    insert = "update TempMetricsReportAll set Reported_Matches_UGC = '%s', Matches_Notices_Sent_UGC = '%s' where reportedDate in ('%s')" %(e[2],e[3],e[1])
    
    cur.execute(insert)
              
 conn.commit()
 cur.close()

 conn.close()


 start = start + 1
 stop = stop + 1

