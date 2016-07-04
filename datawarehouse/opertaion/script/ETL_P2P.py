#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

# downloads
conn=MySQLdb.connect(host='p2p-1-replica.c85gtgxi0qgc.us-west-1.rds.amazonaws.com',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('hubble_stat')
 
dlcur=conn.cursor()
dlcur.execute('select date_format(finished_at, "%Y-%m-%d"), count(*) from finishedDNAIdentifyTask where error_code = 0 and download_time > 0 and finished_at >="2015-07-01" and finished_at <"2016-03-01" and dna_generate_time > 0 group by 1')

dlrows = dlcur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)
dlcur=conn.cursor()
conn.select_db('DW_VTMetrics')
 
for e in dlrows:
    downloads_insert = "insert into VTMetricsReport(company_id,website_type,reportedDate,downloads) values('%s','%s','%s','%s')" %(14,'P2P',e[0],e[1])
    dlcur.execute(downloads_insert)

conn.commit()
dlcur.close()
conn.close()

#matched seed/ matches/ Ips of matches / matches with notices sent
conn=MySQLdb.connect(host='eqx-vtweb-slave-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2')

mtcur=conn.cursor()
mtcur.execute('select date(a.created_at), count(distinct a.key_id) Matched_Seed, count(a.id) Matches, sum(a.view_count), sum(if(a.count_send_notice > 0,1,0)) send_Notices from matchedVideo a, mddb.trackingWebsite b where a.company_id = 14 and a.trackingWebsite_id = b.id and a.hide_flag = 2 and b.website_type in ("p2p") and a.created_at >= "2015-07-01" and a.created_at < "2016-03-01" group by 1')

mtrows = mtcur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 
mtcur=conn.cursor()
conn.select_db('DW_VTMetrics')
              
for e in mtrows:
    matches_insert = "update VTMetricsReport set matchedSeeds = '%s', matches = '%s', matchedURLs_IPs = '%s', matchedWithNoticeSent = '%s' where company_id = 14 and website_type='%s' and reportedDate in ('%s')" %(e[1],e[2],e[3],e[4],'P2P',e[0])
    mtcur.execute(matches_insert)
              
conn.commit()
mtcur.close()
conn.close()
