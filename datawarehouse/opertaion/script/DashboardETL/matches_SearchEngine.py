#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#matches search Engine
conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 

cur=conn.cursor()
get_data = "select date(c.created_at), count(c.id) 'Matched URLs' from matchedVideo_searchEngine c where c.created_at >= date_sub(curdate(),interval 1 day) and c.created_at < date_sub(curdate(),interval 0 day) group by 1" 
print get_data
cur.execute(get_data)
rows = cur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

cur=conn.cursor()
conn.select_db('DW_VTMetrics')

for e in rows:
    update = "update TempMetricsReportAll set Reported_Linking_URLs_Search_Engine = '%s' where reportedDate in ('%s')" %(e[1],e[0])
    print update
    cur.execute(update)
    conn.commit()
 
cur.close()
conn.close()



