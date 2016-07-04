#!/usr/bin/python


import MySQLdb
import sys
import time
import datetime

#matches UGC
conn=MySQLdb.connect(host='eqx-vtweb-slave-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 

cur=conn.cursor()
start = 0
stop = 1
for i in range(1,100):
  get_data = "select b.id, date(a.created_at), count(*), sum(if(hide_flag=2,1,0)) infringing from matchedVideo a, mddb.trackingWebsite b where  a.created_at >= date_sub(curdate(),interval '%s' day) and a.created_at < date_sub(curdate(),interval '%s' day) and a.trackingWebsite_id = b.id and b.id in (1,137,1425,63092) group by 1;" %(stop,start)
  print start,stop
  cur.execute(get_data)
  rows = cur.fetchall()

  dwconn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

  dwcur=dwconn.cursor()
  dwconn.select_db('DW_VTMetrics')

  for e in rows:
#   print e[2],e[3]
#   print "===================="
   vk_update = "update TempMetricsReportAll set VK_Infringing_Matches = '%s' where reportedDate in ('%s') and %s = 137" %(e[3],e[1],e[0])
   print vk_update
   dwcur.execute(vk_update)

   Baidu_update = "update TempMetricsReportAll set Baidu_Pan_Infringing_Matches = '%s' where reportedDate in ('%s') and %s = 1425" %(e[3],e[1],e[0])
   print Baidu_update
   dwcur.execute(Baidu_update)   

   facebook_update = "update TempMetricsReportAll set Facebook_Infringing_Matches = '%s' where reportedDate in ('%s') and %s = 63092" %(e[3],e[1],e[0])
   print facebook_update
   dwcur.execute(facebook_update)

   youtube_update = "update TempMetricsReportAll set YouTube_Infringing_Matches = '%s' where reportedDate in ('%s') and %s = 1" %(e[3],e[1],e[0])
   print youtube_update
   dwcur.execute(youtube_update)

   dwconn.commit()

  stop = stop + 1
  start = start + 1

cur.close()
conn.close()

