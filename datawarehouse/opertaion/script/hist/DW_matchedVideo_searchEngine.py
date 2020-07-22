#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#matchedVideo_searchEngine
conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 
cur=conn.cursor()
stop = 0
start = 1

for i in range(1,420):
  get_data = "select id,matchedVideo_id,company_id,trackingWebsite_id,matchedVideo_linkURL_id,matchedFileItem_id,result_source,confirm_status,is_in_takedown_queue,in_takedown_queue_date,count_send_notice,first_send_notice_date,last_send_notice_date,notice_send_by,takeoff_time,created_at,updated_at from matchedVideo_searchEngine where updated_at >= date_sub(curdate(),interval '%s' day) and updated_at < date_sub(curdate(),interval '%s' day)" %(start,stop)

  print get_data
  cur.execute(get_data)
  rows = cur.fetchall()  

  dwconn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

  dwcur=dwconn.cursor()
  dwconn.select_db('DW_VTMetrics')

  for e in rows:
    insert = "insert into DW_matchedVideo_searchEngine(id,matchedVideo_id,company_id,trackingWebsite_id,matchedVideo_linkURL_id,matchedFileItem_id,result_source,confirm_status,is_in_takedown_queue,in_takedown_queue_date,count_send_notice,first_send_notice_date,last_send_notice_date,notice_send_by,takeoff_time,created_at,updated_at) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" %(e[0],e[1],e[2],e[3],e[4],e[5],e[6],e[7],e[8],e[9],e[10],e[11],e[12],e[13],e[14],e[15],e[16])

    print insert
    dwcur.execute(insert)
    dwconn.commit()

  start = start + 1
  stop = stop + 1

cur.close()
conn.close()



