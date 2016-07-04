#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime


conn=MySQLdb.connect(host='eqx-vtweb-slave-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2')

#matchedURLs / matches with notices sent
mtcur=conn.cursor()
mtcur.execute('select date(c.created_at), count(a.id) Matched_URLs, sum(if(a.count_send_notice > 0,1,0)) send_Notices from matchedVideo a, matchedVideo_searchEngine c where a.company_id = 14 and a.hide_flag = 2 and c.matchedVideo_id = a.id and c.company_id = 14 and  c.created_at >= "2015-07-01" and c.created_at < "2016-03-01" group by 1')

mtrows = mtcur.fetchall()

#removed
rmcur=conn.cursor()
rmcur.execute('select date(a.takeoff_time), count(distinct matchedVideo_id) removed from matchedVideo_searchEngine a where company_id = 14 and a.takeoff_time >= "2015-07-01" and a.takeoff_time < "2016-03-01" group by 1')

rmrows = rmcur.fetchall()


conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)    
mtcur=conn.cursor()
rmcur=conn.cursor()
conn.select_db('DW_VTMetrics')


for e in mtrows:
    matches_insert = "insert into VTMetricsReport(company_id,website_type,reportedDate,matchedURLs_IPs,matchedWithNoticeSent) values('%s','%s','%s','%s','%s')" %(14,'Search Engine',e[0],e[1],e[2])
    mtcur.execute(matches_insert)
              
conn.commit()
mtcur.close()

for e in rmrows:
    removed_insert = "update VTMetricsReport set removed = '%s' where company_id = 14 and website_type='%s' and reportedDate in ('%s')" %(e[1],'Search Engine',e[0])
    rmcur.execute(removed_insert)

conn.commit()
rmcur.close()
conn.close()
