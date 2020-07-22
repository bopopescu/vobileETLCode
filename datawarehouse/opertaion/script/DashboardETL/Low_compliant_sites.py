#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#Low-compliant Sites-Summary
conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 

cur=conn.cursor()
get_data ="select date_sub(curdate(),interval 1 day), sum(if(compliant <= 80,1,0)) Low_compliant_Sites_Num, sum(if(compliant <= 80,1,0)) / count(*) * 100 Low_compliant_Sites_per from (select trackingWebsite_id, sum(if (takeoff_time > 0,1,0)) / count(*) * 100 compliant from matchedVideo where hide_flag = 2 and count_send_notice > 0 and first_send_notice_date >= date_sub(curdate(),interval 8 day) and first_send_notice_date < date_sub(curdate(),interval 1 day) group by 1) tmp_a" 

cur.execute(get_data)
rows = cur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

cur=conn.cursor()
conn.select_db('DW_VTMetrics')

for e in rows:
    insert = "insert into TempMetricsReportAll(reportedDate,Low_compliant_Sites_Num,Low_compliant_Sites_Percent) values('%s','%s','%s')" %(e[0],e[1],e[2])
    
    cur.execute(insert)
              
conn.commit()
cur.close()

conn.close()
