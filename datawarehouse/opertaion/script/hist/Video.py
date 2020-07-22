#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#taisan Video
conn=MySQLdb.connect(host='eqx-taisan-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('taisan') 

cur=conn.cursor()
get_data = "select * from video where clip_title is not null order by created_at limit 10000"

cur.execute(get_data)
rows = cur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

cur=conn.cursor()
conn.select_db('DW_VTMetrics')

for e in rows:
  if not e is None:
    insert = 'insert into video (id,key_id,trackingWebsite_id,is_textIdentified,is_valid,is_slideshow,is_index,source_type,clip_title,clip_size,file_type,clip_url,clip_duration,download_duration,storage_date,storage_domain_path,manga_storage_date,manga_domain_path,slideshow_dna_count,img_url,download_url,post_date,rating_score,poster_id,view_count,audio_takeoff_time,created_at,updated_at) values("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")' %(e[0],e[1],e[2],e[3],e[4],e[5],e[6],e[7],e[8].replace('"',''),e[9],e[10],e[11],e[12],e[13],e[14],e[15],e[16],e[17],e[18],e[19],e[20],e[21],e[22],e[23],e[24],e[25],e[26],e[27])
    print insert
    cur.execute(insert)
    conn.commit()

cur.close()
conn.close()

