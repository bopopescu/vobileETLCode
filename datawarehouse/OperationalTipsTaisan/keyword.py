#!/usr/bin/env python
# coding: utf-8

import os
import sys 
import datetime
import bigquery
import ConfigParser
sys.path.append("/Job/Test/service")
#from connectService import ConnectionService, MailService
reload(sys) 
sys.setdefaultencoding('utf8')

def upload_to_gs():
    #beg = datetime.date(2015,05,29)
    #end = datetime.date(2015,05,29)
    #for i in xrange((end- beg).days+1):
	#date = str(end - datetime.timedelta(days=i))
	#date1 = str(end - datetime.timedelta(days=i-1)) 
	#f = "matchedVideo%s.csv" %date.replace("-","")
	f = "keyword.csv"
        SQL = """select * from keyword""" 
	#SQL = """select id, company_id, trackingMeta_id, trackingWebsite_id, key_id, relationGroup_id, view_count, poster, post_date, relationGroup_status, is_contentRule_applied, is_snapshot_generated, is_in_takedown_queue, is_media_file_exist, in_takedown_queue_date, count_send_notice, first_send_notice_date, last_send_notice_date, notice_send_by, takeoff_time, content_type, content_sub_type, takeoff_type, hide_flag, hide_by, hide_date, clip_duration, clip_size, clip_url_reverse, clip_url, download_url, replace(clip_title,'\r','\') as clip_title, clip_offset, meta_offset, score_video, score_audio, matched_duration, matched_video_duration, matched_audio_duration, verification, posterID_url, matched_storage_domain, matched_storage_date, season_number, episode_number, instance_title, instance_duration, matchedFile_id, meta_uuid, meta_title, meta_duration, vddb_title, notes, start_at, created_at, updated_at, contributor, is_poster_whitelist_filtered, last_refresh_at from matchedVideo where created_at >= '%s' and created_at < '%s' and id=267868732;""" %(date, date1)
	cmd_csv = """mysql -heqx-taisan-subordinate-db -ukettle -pk3UTLe taisan -e"%s">> %s""" %(SQL,f)
	print cmd_csv
	cmd_upload = "gsutil cp %s gs://vobile-data-analysis/OperationalTipsTaisan/keyword" %f
	load_bigquery = "bq load --skip_leading_rows=1 --field_delimiter='\t' --quote='' OperationalTipsTaisan.keyword gs://vobile-data-analysis/OperationalTipsTaisan/keyword/%s" %f 
	os.system(cmd_csv)
	os.system(cmd_upload)
	#os.system("rm %s"%f)
        os.system(load_bigquery)

def main():
     #conf = ConfigParser.ConfigParser()
     #conf.read("/Job/datawarehouse/upload_data/conf/conf")
     #db=ConnectionService(conf.get("db_conf","ip"), conf.get("db_conf","user"), conf.get("db_conf","passwd"), "tracker2")
     upload_to_gs()

if __name__=="__main__":
     main()
