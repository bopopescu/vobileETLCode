#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#matches cyberlocker
conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 

cur=conn.cursor()
get_data = "select date(a.created_at), count(c.id) Matched_URLs from matchedVideo a, matchedFileItem c where c.matchedFile_id = a.matchedFile_id and a.created_at >= date_sub(curdate(),interval 1 day) and a.created_at < date_sub(curdate(),interval 0 day) group by 1"
print get_data
cur.execute(get_data)
rows = cur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

cur=conn.cursor()
conn.select_db('DW_VTMetrics')

for e in rows:
    update = "update TempMetricsReportAll set Reported_Matches_Cyberlocker = '%s' where reportedDate in ('%s')" %(e[1],e[0]) 
    print update
    cur.execute(update)
    conn.commit()

cur.close()
conn.close()



