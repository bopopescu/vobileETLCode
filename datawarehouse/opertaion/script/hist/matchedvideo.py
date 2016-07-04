#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#taisan Video
conn=MySQLdb.connect(host='eqx-taisan-slave-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('taisan') 

cur=conn.cursor()

stop = 4170000000
colname = ''
for i in range(0,11049):

  colname += "'" + str(stop) + "',"
  stop = stop + 1
colname = colname[0:-1]
get_data = "select * from matchedVideo where video_id in (%s)" %(colname)
cur.execute(get_data)
rows = cur.fetchall()  

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

cur=conn.cursor()
conn.select_db('DW_VTMetrics')

for e in rows:
   print e 
   if not e is None:
      insert = 'insert into matchedVideo(video_id,id,trackingWebsite_id,key_id,updated_time,sync_viewcount_time,view_count,clip_url,hybrid_download_url,clip_takeoff_time,audio_takeoff_time,related_status,is_dna_full_length,is_media_rebuild_index,manga_matched_storage_date,manga_matched_domain_path,matched_domain_path,matched_storage_date,matched_media_suffix,vmid,tvsr_check_code,tvsr_video_type,created_at,updated_at) values("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")' %(e[0],e[1],e[2],e[3],e[4],e[5],e[6],e[7],e[8],e[9],e[10],e[11],e[12],e[13],e[14],e[15],e[16],e[17],e[18],e[19],e[20],e[21],e[22],e[23])

      print insert
      cur.execute(insert)
      conn.commit()

cur.close()
conn.close()



