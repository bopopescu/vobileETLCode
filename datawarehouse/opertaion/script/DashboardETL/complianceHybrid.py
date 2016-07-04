#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#Compliance Hybrid
conn=MySQLdb.connect(host='eqx-vtweb-slave-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 

cur=conn.cursor()
get_data ="Select b.website_type, sum(if(a.takeoff_time >= date_sub(curdate(),interval 7 day) and a.takeoff_time < date_sub(curdate(),interval 0 day),1,0))/count(*) Compliance_percent, avg(timestampdiff(second,a.first_send_notice_date,if(a.takeoff_time >= date_sub(curdate(),interval 7 day) and a.takeoff_time < date_sub(curdate(),interval 0 day),a.takeoff_time,a.first_send_notice_date))) avg_Compliance_Time from matchedVideo a, mddb.trackingWebsite b where a.trackingWebsite_id = b.id and b.website_type in ('%s') and a.first_send_notice_date >= date_sub(curdate(),interval 7 day) and a.first_send_notice_date < date_sub(curdate(),interval 0 day) group by 1" %('hybrid')   

cur.execute(get_data)
rows = cur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

cur=conn.cursor()
conn.select_db('DW_VTMetrics')

for e in rows:
    update = "update TempMetricsReportAll set Compliance_Percent_Hybrid = '%s', Compliance_Time_Hybrid = '%s' where reportedDate = date_sub(curdate(),interval 1 day)" %(e[1],e[2])    
    print update
    cur.execute(update)
    conn.commit()

cur.close()
conn.close()




