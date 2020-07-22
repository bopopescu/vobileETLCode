#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime
import types

#matches UGC
conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 

cur=conn.cursor()
start = 0
stop = 1
for i in range(1,4):
  get_data = "select id,company_id,trackingMeta_id,trackingWebsite_id,replace(key_id,''',''),relationGroup_id,view_count,replace(poster,''',''),post_date,relationGroup_status,is_contentRule_applied,is_snapshot_generated,is_in_takedown_queue,is_media_file_exist,in_takedown_queue_date,count_send_notice,first_send_notice_date,last_send_notice_date,notice_send_by,takeoff_time,content_type,content_sub_type,takeoff_type,hide_flag,hide_by,hide_date,clip_duration,clip_size,replace(clip_url_reverse,''',''),replace(clip_url,''',''),replace(download_url,''',''),replace(clip_title,''',''),clip_offset,meta_offset,score_video,score_audio,matched_duration,matched_video_duration,matched_audio_duration,verification,replace(posterID_url,''',''),matched_storage_domain,matched_storage_date,season_number,episode_number,replace(instance_title,''',''),instance_duration,matchedFile_id,replace(meta_uuid,''',''),replace(meta_title,''',''),meta_duration,replace(vddb_title,''',''),replace(notes,''',''),start_at,created_at,updated_at,contributor,is_poster_whitelist_filtered from matchedVideo where updated_at >= date_sub(curdate(),interval '%s' day) and updated_at < date_sub(curdate(),interval '%s' day)" %(stop,start)
  print start,stop
  print get_data
  cur.execute(get_data)
  rows = cur.fetchall()

  dwconn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

  dwcur=dwconn.cursor()
  dwconn.select_db('DW_VTMetrics')
  
  for e in rows:
    insert = "insert into DW_matchedVideo (id,company_id,trackingMeta_id,trackingWebsite_id,key_id,relationGroup_id,view_count,poster,post_date,relationGroup_status,is_contentRule_applied,is_snapshot_generated,is_in_takedown_queue,is_media_file_exist,in_takedown_queue_date,count_send_notice,first_send_notice_date,last_send_notice_date,notice_send_by,takeoff_time,content_type,content_sub_type,takeoff_type,hide_flag,hide_by,hide_date,clip_duration,clip_size,clip_url_reverse,clip_url,download_url,clip_title,clip_offset,meta_offset,score_video,score_audio,matched_duration,matched_video_duration,matched_audio_duration,verification,posterID_url,matched_storage_domain,matched_storage_date,season_number,episode_number,instance_title,instance_duration,matchedFile_id,meta_uuid,meta_title,meta_duration,vddb_title,notes,start_at,created_at,updated_at,contributor,is_poster_whitelist_filtered) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" %(e[0],e[1],e[2],e[3],e[4],e[5],e[6],e[7],e[8],e[9],e[10],e[11],e[12],e[13],e[14],e[15],e[16],e[17],e[18],e[19],e[20],e[21],e[22],e[23],e[24],e[25],e[26],e[27],e[28],e[29],e[30],e[31],e[32],e[33],e[34],e[35],e[36],e[23],e[38],e[39],e[40],e[41],e[42],e[43],e[44],e[45],e[46],e[47],e[48],e[49],e[50],e[51],e[52],e[53],e[54],e[55],e[56],e[57])
    print insert
    dwcur.execute(insert)
    dwconn.commit()

  stop = stop + 1
  start = start + 1

cur.close()
conn.close()

