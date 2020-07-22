#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

start = 0
stop = 1
#notices linking sites
conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
cur=conn.cursor()
conn.select_db('tracker2') 

for i in range(1,55):
  get_data = "select date(c.first_send_notice_date), sum(if(c.count_send_notice > 0,1,0)) 'send Notices' from matchedVideo a, matchedVideo_linkURL c where c.matchedVideo_id = a.id and c.first_send_notice_date >= date_sub(curdate(),interval '%s' day) and c.first_send_notice_date < date_sub(curdate(),interval '%s' day) and a.company_id=c.company_id group by 1" %(stop,start)
  print get_data
  cur.execute(get_data)
  rows = cur.fetchall()

  dwconn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

  dwcur=dwconn.cursor()
  dwconn.select_db('DW_VTMetrics')

  for e in rows:
    update = "update TempMetricsReportAll set URLs_Notices_Sent_Linking_Sites = '%s' where reportedDate in ('%s')" %(e[1],e[0])
    print update    
    dwcur.execute(update)
    dwconn.commit()

  stop = stop + 1
  start = start + 1
 
cur.close()
conn.close()



