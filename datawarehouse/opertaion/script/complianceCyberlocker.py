#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#Compliance Cyberlocker 
conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 

cur=conn.cursor()
get_data ="Select sum(if(b.takeoff_time >= date_sub(curdate(),interval 9 day) and b.takeoff_time < date_sub(curdate(),interval 2 day),1,0))/count(*) Compliance_percent, avg(timestampdiff(second,a.first_send_notice_date,if(b.takeoff_time  >= date_sub(curdate(),interval 9 day) and b.takeoff_time < date_sub(curdate(),interval 2 day),a.takeoff_time,a.first_send_notice_date))) avg_Compliance_Time from matchedVideo a, matchedFileItem b where a.matchedFile_id = b.matchedFile_id and a.first_send_notice_date >= date_sub(curdate(),interval 9 day) and a.first_send_notice_date < date_sub(curdate(),interval 2 day)"   

cur.execute(get_data)
rows = cur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

cur=conn.cursor()
conn.select_db('DW_VTMetrics')

for e in rows:
    update = "update TempMetricsReportAll set Compliance_Percent_Cyberlocker = '%s', Compliance_Time_Cyberlocker = '%s' where reportedDate = date_sub(curdate(),interval 2 day)" %(e[0],e[1])     
    cur.execute(update)
    conn.commit()

cur.close()
conn.close()



