#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#matchedVideo_linkURL

conn=MySQLdb.connect(host='eqx-vtweb-slave-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 
cur=conn.cursor()
stop = 0
start = 1

for i in range(1,420):

  get_data = "select id,replace(website_name,'" + "\\'" + "','" + "\"" + "'),replace(website_domain,'" + "\\'" + "','" + "\"" + "'),replace(homepage,'" + "\\'" + "','" + "\"" + "'),website_type,region,category,music_website_classification,IDC,trackingWebsiteGroup_id,language_id,country_id,manga_percent,replace(display_name,'" + "\\'" + "','" + "\"" + "'),x,y,replace(logo,'" + "\\'" + "','" + "\"" + "'),time_zone,replace(screenshot,'" + "\\'" + "','" + "\"" + "'),replace(description,'" + "\\'" + "','" + "\"" + "'),is_enabled,need_proxy_check,created_at,created_by,updated_at,updated_by from mddb.trackingWebsite where updated_at >= date_sub(curdate(),interval '%s' day) and updated_at < date_sub(curdate(),interval '%s' day)" %(start,stop)

  print get_data
  cur.execute(get_data)
  rows = cur.fetchall()  


  dwconn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

  dwcur=dwconn.cursor()
  dwconn.select_db('DW_VTMetrics')

  for e in rows:
    insert = "insert into DW_trackingWebsite(id,website_name,website_domain,homepage,website_type,region,category,music_website_classification,IDC,trackingWebsiteGroup_id,language_id,country_id,manga_percent,display_name,x,y,logo,time_zone,screenshot,description,is_enabled,need_proxy_check,created_at,created_by,updated_at,updated_by) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" %(e[0],e[1],e[2],e[3],e[4],e[5],e[6],e[7],e[8],e[9],e[10],e[11],e[12],e[13],e[14],e[15],e[16],e[17],e[18],e[19],e[20],e[21],e[22],e[23],e[24],e[25])

    print insert
    dwcur.execute(insert)
    dwconn.commit()
  
  start = start + 1
  stop = stop + 1

cur.close()
conn.close()



