#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#Matches All hosts
conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

cur=conn.cursor()
conn.select_db('DW_VTMetrics')
stop = 1
for i in range(1,45):

  match_update = "update TempMetricsReportAll set Reported_Matches_All_Hosting = Reported_Matches_UGC + Reported_Matches_Hybrid + Reported_Matches_Cyberlocker where reportedDate = date_sub(curdate(),interval '%s' day)" %(stop)   

  cur.execute(match_update)
  conn.commit()
 
  send_update = "update TempMetricsReportAll set Matches_Notices_Sent_All_Hosting = Matches_Notices_Sent_UGC + Matches_Notices_Sent_Hybrid + Matches_Notices_Sent_Cyberlocker where reportedDate = date_sub(curdate(),interval '%s' day)" %(stop)

  cur.execute(send_update)
  conn.commit()

  stop = stop + 1

cur.close()
conn.close()



