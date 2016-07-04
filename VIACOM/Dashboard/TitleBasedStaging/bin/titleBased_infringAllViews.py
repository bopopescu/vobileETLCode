#!/usr/bin/env python
#coding:utf8
#Date: 2016-03-14
#Author: cwj
#Desc: extral data from insight2.1 
#


from mysqlHelp import MySQLHelper
import ConfigParser
from parseConfig import CfgParser
import sys
import os
import logging
import time
from titleBased import getConfMysqlInfo, getMinDatePara
from sendMail import sendToMe

logger = logging.getLogger("titleBased_infringAllViews")
logger.setLevel(logging.DEBUG)
log_file = '/Job/VIACOM/Dashboard/TitleBased/log/titleBased_infringAllViews.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

cfg_file = "/Job/VIACOM/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
if not os.path.exists(cfg_file):
	logging.debug(": config file not exists") 
	sendToMe(subject = "titleBased_infringAllViews ERROR", body = "config file not exists")
	sys.exit(0)
#################################################################################################################################
target_server_section = "target_server_staging"
target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)

insight_server_section = "insight"
insight_host, insight_user, insight_passwd, insight_port, insight_db= getConfMysqlInfo(insight_server_section)

vtweb_tracker2_section = "vtweb_staging"		
vt_host, vt_user, vt_passwd, vt_port, vt_db = getConfMysqlInfo(vtweb_tracker2_section)

vtweb_mysql = MySQLHelper(host=vt_host, user=vt_user,passwd=vt_passwd, port = vt_port, db_name = vt_db)
#################################################################################################################################

def getInsightViews(start_date, end_date):
	logger.info(" extral data from Dashboard.matchedVideoViewCountCompletion start")
	date_dict  ={"start_date": start_date, "end_date": end_date}
	try:	
		get_data_sql = """
			select matchedVideo_id, trackingWebsite_id, trackingMeta_id, 
				company_id, report_at, view_count, current_timestamp as ETLDate
			from matchedVideoViewCountCompletion 
			where report_at  > "%(start_date)s" and report_at <= "%(end_date)s" 
		""" %date_dict
		insight_mysql = MySQLHelper(host=insight_host, user=insight_user,passwd=insight_passwd, port = insight_port, db_name = insight_db)
		data = insight_mysql.queryCMD(get_data_sql)

		target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, \
			db_name = target_db, port = target_port, charset = 'utf8')
	        insert_sql = """insert into matchedVideoViewCountCompletion 
        		(matchedVideo_id, trackingWebsite_id, trackingMeta_id, company_id, report_at, view_count, ETLDate)
        		values(%s, %s, %s, %s, %s, %s, %s)
		"""
		target_mysql.executeManyCMD(insert_sql, data)
		target_mysql.commit()	
	except Exception, e:
		logger.debug("extral data from Dashboard.matchedVideoViewCountCompletion, %s" %e)
		sendToMe(subject = "titleBased_infringAllViews ERROR", body = str(e).replace("\"", "").replace("'", "").replace("!", ""))
		sys.exit(0)
	finally:
		insight_mysql.closeConn()
		insight_mysql.closeCur()
		target_mysql.closeCur()
		target_mysql.closeConn()
	logger.info(" extral data from Dashboard.matchedVideoViewCountCompletion end")

def getMatchedVideo(start_date, end_date):
	logger.info(" extral data from tracker2.matchedVideo start")
	date_dict  ={"start_date": start_date, "end_date": end_date}
	try:	
		get_data_sql = """
			select
			  a.id as matchedVideo_id, trackingMeta_id, trackingWebsite_id, 
			  date_format(first_send_notice_date, "%%Y-%%m-%%d") as firstSendNoticeDate,
			  date_format(a.created_at, "%%Y-%%m-%%d") as created_at,
			  CURRENT_TIMESTAMP as ETLDate
			from matchedVideo as a, tracker2.metaExtraInfo as b , mddb.trackingWebsite as c
			where a.company_id  =14
			  and b.company_id = 14
			  and c.website_type = "ugc"
			  and a.trackingMeta_id = b.meta_id
			  and a.trackingWebsite_id = c.id
			  and a.hide_flag = 2
			  and a.created_at >= "2015-03-01"
			  and count_send_notice > 0
			  and date_format(a.first_send_notice_date, "%%Y-%%m-%%d") > "%(start_date)s"
			  and date_format(a.first_send_notice_date, "%%Y-%%m-%%d") <= "%(end_date)s"
		""" %date_dict
		vtweb_mysql = MySQLHelper(host=vt_host, user=vt_user,passwd=vt_passwd, port = vt_port, db_name = vt_db)
		vtweb_mysql.queryCMD("set time_zone  = '-8:00'")
		data = vtweb_mysql.queryCMD(get_data_sql)

		target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, \
			db_name = target_db, port = target_port, charset = 'utf8')
        	insert_sql = """insert into matchedVideo
        		(matchedVideo_id, trackingMeta_id, trackingWebsite_id, firstSendNoticeDate, reportDate, ETLDate)
        		values(%s, %s, %s, %s, %s, %s)
		"""
		target_mysql.executeManyCMD(insert_sql, data)
		target_mysql.commit()	
	except Exception, e:
		logger.debug("extral data from tracker2.matchedVideo, %s" %e)
		sendToMe(subject = "titleBased_infringAllViews ERROR", body = str(e).replace("\"", "").replace("'", "").replace("!", ""))
		sys.exit(0)
	finally:
		vtweb_mysql.closeConn()
		vtweb_mysql.closeCur()
		target_mysql.closeCur()
		target_mysql.closeConn()
	logger.info(" extral data from tracker2.matchedVideo end")	

def main():
	sendToMe(subject = "titleBased_infringAllViews start", body = "titleBased_infringAllViews start")

	matchedVideo_start_date = getMinDatePara("matchedVideo", "firstSendNoticeDate")
	if not matchedVideo_start_date:
		matchedVideo_start_date = "2015-02-28"

	while True:
		if str(matchedVideo_start_date) >= str(time.strftime('%Y-%m-%d',time.localtime(time.time() - 24 * 60 * 60 * 365))):
			break
		matchedVideo_end_date = time.strftime("%Y-%m-%d", time.localtime(time.mktime(time.strptime(str(matchedVideo_start_date), "%Y-%m-%d")) + 1 * 24 * 60 * 60))

		getMatchedVideo(start_date = matchedVideo_start_date, end_date = matchedVideo_end_date)
		matchedVideo_start_date = getMinDatePara("matchedVideo", "firstSendNoticeDate")
	# ---------------------------------------------------------------------------------------------------------------------
	insight_start_date = getMinDatePara("matchedVideoViewCountCompletion", "report_at")
	if not insight_start_date:
		insight_start_date = "2015-02-28"

	while True:
		if str(insight_start_date) >= str(time.strftime('%Y-%m-%d',time.localtime(time.time() - 24 * 60 * 60 * 365))):
			break
		insight_end_date = time.strftime("%Y-%m-%d", time.localtime(time.mktime(time.strptime(str(insight_start_date), "%Y-%m-%d")) + 1 * 24 * 60 * 60))
		
		getInsightViews(start_date = insight_start_date, end_date = insight_end_date)
		insight_start_date = getMinDatePara("matchedVideoViewCountCompletion", "report_at")

	sendToMe(subject = "titleBased_infringAllViews end", body = "titleBased_infringAllViews end")

if __name__ == "__main__":
	main()
	

