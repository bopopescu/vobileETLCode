#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

# downloads 
conn=MySQLdb.connect(host='dna-212',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('pentaho')   

dlcur=conn.cursor()
dlcur.execute('select website_type,date(report_at),sum(discovery_num) downloads from videoDailyStat a,webSite b where trackingWebsite_id=b.id and report_at >= "2015-07-01" and report_at < "2016-03-01" and b.website_type in ("ugc") group by 1,2')

dlrows = dlcur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)    
dlcur=conn.cursor()
conn.select_db('DW_VTMetrics')
              
for e in dlrows:
    downloads_insert = "insert into VTMetricsReport(company_id,website_type,reportedDate,downloads) values('%s','%s','%s','%s')" %(14,e[0],e[1],e[2])
    dlcur.execute(downloads_insert)
    
conn.commit()
dlcur.close()
conn.close()



#matchedURLs / matches with notices sent
conn=MySQLdb.connect(host='eqx-vtweb-slave-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 
mtcur=conn.cursor()
mtcur.execute('select b.website_type, date(a.created_at), count(a.id) Matched_URLs, sum(if(a.count_send_notice > 0,1,0)) send_Notices from matchedVideo a, mddb.trackingWebsite b where a.company_id = 14 and a.trackingWebsite_id = b.id and a.hide_flag = 2 and b.website_type in ("ugc") and a.created_at >= "2015-07-01" and a.created_at < "2016-03-01" group by 1,2')

mtrows = mtcur.fetchall()

#removed
rmcur=conn.cursor()
rmcur.execute('select b.website_type, date(a.takeoff_time), count(*) removed from matchedVideo a, mddb.trackingWebsite b where a.company_id = 14 and a.trackingWebsite_id = b.id and b.website_type in ("ugc") and a.takeoff_time >= "2015-07-01" and a.takeoff_time < "2016-03-01" group by 1,2')

rmrows = rmcur.fetchall()


conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)    
mtcur=conn.cursor()
rmcur=conn.cursor()
conn.select_db('DW_VTMetrics')


for e in mtrows:
    matches_insert = "update VTMetricsReport set matchedURLs_IPS = '%s', matchedWithNoticeSent = '%s' where company_id = 14 and website_type='%s' and reportedDate in ('%s')" %(e[2],e[3],e[0],e[1])
    mtcur.execute(matches_insert)
              
conn.commit()
mtcur.close()

for e in rmrows:
    removed_insert = "update VTMetricsReport set removed = '%s' where company_id = 14 and website_type='%s' and reportedDate in ('%s')" %(e[2],e[0],e[1])
    rmcur.execute(removed_insert)

conn.commit()
rmcur.close()
conn.close()
