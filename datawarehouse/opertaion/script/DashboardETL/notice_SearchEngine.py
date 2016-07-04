#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#matches search Engine
conn=MySQLdb.connect(host='eqx-vtweb-slave-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 

cur=conn.cursor()
get_data = "select date(c.first_send_notice_date), sum(if(c.count_send_notice > 0,1,0)) 'send Notices' from matchedVideo_searchEngine c where c.first_send_notice_date >= date_sub(curdate(),interval 1 day) and c.first_send_notice_date < date_sub(curdate(),interval 0 day) group by 1" 
print get_data
cur.execute(get_data)
rows = cur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

cur=conn.cursor()
conn.select_db('DW_VTMetrics')

for e in rows:
    insert = "update TempMetricsReportAll set URLs_Notices_Sent_Search_Engine = '%s' where reportedDate in ('%s')" %(e[1],e[0])
    print insert
    cur.execute(insert)
    conn.commit()
 
cur.close()
conn.close()



