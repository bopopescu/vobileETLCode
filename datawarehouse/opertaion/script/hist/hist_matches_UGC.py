#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#matches UGC
conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 

cur=conn.cursor()
match_data = "select b.website_type, date(a.created_at), count(a.id) Matched_URLs from matchedVideo a, mddb.trackingWebsite b where a.trackingWebsite_id = b.id  and b.website_type in ('%s') and a.created_at >= date_sub(curdate(),interval 1 day) and a.created_at < date_sub(curdate(),interval 0 day) group by 1,2" %('ugc')

cur.execute(match_data)
rows = cur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

cur=conn.cursor()
conn.select_db('DW_VTMetrics')

for e in rows:
    update = "update TempMetricsReportAll set Reported_Matches_UGC = '%s' where reportedDate in ('%s')" %(e[2],e[1])
    print update
    cur.execute(update)
    conn.commit()

cur.close()
conn.close()

