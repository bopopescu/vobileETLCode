#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#taisan Video
conn=MySQLdb.connect(host='eqx-taisan-slave-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('taisan') 

cur=conn.cursor()
get_data = "select * from trackingWebsite where id in ('1','137','60054','63099','62170','60537','62420','60300','55317','805','62183','63392','63385','63374','63380','63379','48319','63017','60189','47923','411','136','62995','47917','62989','1562','60302','62787','20','62796','63395','47922','9','1452','47925','268','2','22829')"
print get_data
cur.execute(get_data)
rows = cur.fetchall()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

cur=conn.cursor()
conn.select_db('DW_VTMetrics')

for e in rows:
  if not e is None:
    insert = 'insert into trackingWebsite(id,website_name,website_domain,website_type,music_website_classification,is_enabled,group_id,is_KeywordSearch,is_KeywordSearchRelevance,is_ugcRelate,is_ugcPoster,is_allRelate,is_allPoster,is_kw_full_query,is_reprocess_enabled,is_media_rebuild_index_needed,is_evidence_needed,is_text_verification_enabled,is_full_length_new_vdna_enabled,is_cookie_use_wise_proxy,is_tvsr_sync_enabled,max_evidence_size,tvsr_sync_min_duration,recover_blocked_cookie_interval,cookie_expired_interval,is_post_task_enabled,is_dna_diagram_task_enabled,kw_only_full_query,colander_kw_only_full_query,relate_only_full_query,kw_exist_video_no_binding,is_SpeedChange_enabled,rating_longvideo_addscore,rating_filter_binding_score,created_at,updated_at) values("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")' %(e[0],e[1],e[2],e[3],e[4],e[5],e[6],e[7],e[8],e[9],e[10],e[11],e[12],e[13],e[14],e[15],e[16],e[17],e[18],e[19],e[20],e[21],e[22],e[23],e[24],e[25],e[26],e[27],e[28],e[29],e[30],e[31],e[32],e[33],e[34],e[35])
    print insert
    cur.execute(insert)
    conn.commit()

cur.close()
conn.close()

