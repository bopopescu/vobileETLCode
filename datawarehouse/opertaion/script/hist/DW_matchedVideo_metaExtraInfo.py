#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#matchedVideo_linkURL

conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 
cur=conn.cursor()

stop = 0
start = 1

for i in range(1,420):
  get_data = "select meta_id,company_id,subCompany_id,is_subscription,replace(display_name,'" + "\\'" + "','" + "\"" + "'),priority_type,is_deleted,is_need_confirm,is_hybrid_need_confirm,trackingSetting_id,contentRule_id,alertRule_id,is_filter_postdate_enabled,start_at,stop_at,published_date,created_at,created_by,updated_at,updated_by from metaExtraInfo where updated_at >= date_sub(curdate(),interval '%s' day) and updated_at < date_sub(curdate(),interval '%s' day)" %(start,stop)
  print get_data
  cur.execute(get_data)
  rows = cur.fetchall()  


  dwconn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

  dwcur=dwconn.cursor()
  dwconn.select_db('DW_VTMetrics')

  for e in rows:
    insert = "insert into DW_metaExtraInfo(meta_id,company_id,subCompany_id,is_subscription,display_name,priority_type,is_deleted,is_need_confirm,is_hybrid_need_confirm,trackingSetting_id,contentRule_id,alertRule_id,is_filter_postdate_enabled,start_at,stop_at,published_date,created_at,created_by,updated_at,updated_by) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" %(e[0],e[1],e[2],e[3],e[4],e[5],e[6],e[7],e[8],e[9],e[10],e[11],e[12],e[13],e[14],e[15],e[16],e[17],e[18],e[19])

    print insert
    dwcur.execute(insert)
    dwconn.commit()
  
  start = start + 1
  stop = stop + 1

cur.close()
conn.close()



