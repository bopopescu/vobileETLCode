#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime


conn=MySQLdb.connect(host='eqx-vtweb-slave-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2')

#matchedURLs / matches with notices sent
mtcur=conn.cursor()
mtcur.execute('select date(a.created_at), count(distinct(c.id)) Matched_URLs, sum(if(a.count_send_notice > 0,1,0)) send_Notices from matchedVideo a, matchedFileItem c where a.company_id = 14 and a.hide_flag = 2 and c.matchedFile_id = a.matchedFile_id and  a.created_at >= date_sub(curdate(),interval 1 day) and a.created_at < curdate() group by 1')

mtrows = mtcur.fetchall()

#removed
rmcur=conn.cursor()
rmcur.execute('select date(c.takeoff_time), count(distinct a.id) removed from matchedVideo a, matchedFileItem c where a.company_id = 14 and c.matchedFile_id = a.matchedFile_id and  c.takeoff_time >= date_sub(curdate(),interval 1 day) and c.takeoff_time < curdate() group by 1')

rmrows = rmcur.fetchall()


conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)    
mtcur=conn.cursor()
rmcur=conn.cursor()
conn.select_db('DW_VTMetrics')


for e in mtrows:
    matches_insert = "insert into VTMetricsReport(company_id,website_type,reportedDate,matchedURLs_IPs,matchedWithNoticeSent) values('%s','%s','%s','%s','%s')" %(14,'Cyberlocker',e[0],e[1],e[2])
    mtcur.execute(matches_insert)
              
conn.commit()
mtcur.close()

for e in rmrows:
    removed_insert = "update VTMetricsReport set removed = '%s' where company_id = 14 and website_type='%s' and reportedDate in ('%s')" %(e[1],'Cyberlocker',e[0])
    rmcur.execute(removed_insert)

conn.commit()
rmcur.close()
conn.close()
