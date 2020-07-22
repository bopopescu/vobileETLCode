#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime


#Matches P2P
conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 

cur=conn.cursor()
start = 0
stop = 1
for i in range(1,40):
  get_data = "SELECT date(a.first_notice_send_time), COUNT(a.id)  InfringingIPs_Noticed FROM takedownNoticeItemP2PDetail a WHERE  a.first_notice_send_time >= date_sub(curdate(),interval '%s' day) and a.first_notice_send_time < date_sub(curdate(),interval '%s' day)" %(stop,start)
  print get_data
  cur.execute(get_data)
  rows = cur.fetchall()

  dwconn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

  dwcur=dwconn.cursor()
  dwconn.select_db('DW_VTMetrics')

  for e in rows:
    insert = "update TempMetricsReportAll set Notices_Sent_P2P = '%s' where reportedDate in ('%s')" %(e[1],e[0])    
    dwcur.execute(insert)
    dwconn.commit()
 
  stop = stop + 1
  start = start + 1

cur.close()
conn.close()



