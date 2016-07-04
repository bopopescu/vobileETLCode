#!/usr/bin/python

import MySQLdb
import os
import sys
import time
import datetime
import csv
import boto
import gcs_oauth2_boto_plugin
import shutil
import StringIO


#taisan Video
conn=MySQLdb.connect(host='eqx-vtweb-slave-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2')
cur=conn.cursor()

start = 6
stop = 7
upload = ''

info='/Job/datawarehouse/opertaion/script/hist/archive/'


for i in range(1,450):
  get_data = "select * from matchedVideo where updated_at >= date_sub(curdate(),interval '%s' day) and updated_at < date_sub(curdate(),interval '%s' day)" %(stop,start)
  print get_data
  cur.execute(get_data)
  curTime = time.strftime("%Y%m%d", time.localtime(time.time()-stop*24*60*60))
  print curTime
  csvfile = file('/Job/datawarehouse/opertaion/script/hist/archive/matchedVideo' + curTime + '.csv','wb')
  writers = csv.writer(csvfile)
  writers.writerow(['id','company_id','trackingMeta_id','trackingWebsite_id','key_id','relationGroup_id','view_count','poster','post_date','relationGroup_status','is_contentRule_applied','is_snapshot_generated','is_in_takedown_queue','is_media_file_exist','in_takedown_queue_date','count_send_notice','first_send_notice_date','last_send_notice_date','notice_send_by','takeoff_time','content_type','content_sub_type','takeoff_type','hide_flag','hide_by','hide_date','clip_duration','clip_size','clip_url_reverse','clip_url','download_url','clip_title','clip_offset','meta_offset','score_video','score_audio','matched_duration','matched_video_duration','matched_audio_duration','verification','posterID_url','matched_storage_domain','matched_storage_date','season_number','episode_number','instance_title','instance_duration','matchedFile_id','meta_uuid','meta_title','meta_duration','vddb_title','notes','start_at','created_at','updated_at','contributor','is_poster_whitelist_filtered','last_refresh_at'])

  while 1:
    lines = cur.fetchall()
    if len(lines) ==0:
      break
    for i in lines:
      writers.writerows([i])

  csvfile.close()
  stop = stop + 1
  start = start + 1
  print stop
  print start
  listfile = os.listdir(info)
  for line in listfile:
    lines = info + line 
    upload = 'gsutil cp ' + lines + ' gs://vobile-data-analysis/matchedVideo/'  
    os.system(upload)
  
  os.remove(lines)

cur.close()
conn.close()





