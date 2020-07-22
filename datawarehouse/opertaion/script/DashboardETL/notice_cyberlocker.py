#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#matches cyberlocker
conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 

cur=conn.cursor()
get_data = "select date(a.first_send_notice_date), sum(if(a.count_send_notice > 0,1,0)) send_Notices from matchedVideo a, mddb.trackingWebsite b where a.trackingWebsite_id=b.id and b.website_type in ('cyberlocker') and a.first_send_notice_date >= date_sub(curdate(),interval 1 day) and a.first_send_notice_date < date_sub(curdate(),interval 0 day) group by 1"
print get_data
cur.execute(get_data)
rows = cur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

cur=conn.cursor()
conn.select_db('DW_VTMetrics')

for e in rows:
    update = "update TempMetricsReportAll set Matches_Notices_Sent_Cyberlocker = '%s' where reportedDate in ('%s')" %(e[1],e[0]) 
    print update
    cur.execute(update)
    conn.commit()

cur.close()
conn.close()



