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
import re

logger = logging.getLogger("titleBased_matchedVideo")
logger.setLevel(logging.DEBUG)
log_file = '/Job/VIACOM/Dashboard/TitleBased/log/titleBased_matchedVideo.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

cfg_file = "/Job/VIACOM/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
if not os.path.exists(cfg_file):
	logging.debug(": config file not exists") 
	sendToMe(subject = "titleBased_matchedVideo ERROR", body = "config file not exists")
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
def getMatchedVideo(min_reportDate, max_reportDate, min_updateDate):
	logger.info(" extral data from tracker2.matchedVideo start")
	date_dict  ={"min_reportDate": min_reportDate, "max_reportDate": max_reportDate, "min_updateDate": min_updateDate}
	try:	
		get_data_sql = """
			select
			  a.id as matchedVideo_id,
			  a.trackingMeta_id,
			  a.trackingWebsite_id,
			  date_format(a.first_send_notice_date, "%%Y-%%m-%%d") as firstSendNoticeDate,
			  a.hide_flag as hideFlag,
			  count_send_notice as countSendNotice,
			  date_format(a.created_at, "%%Y-%%m-%%d") as reportDate,
			  date_format(a.updated_at, "%%Y-%%m-%%d") as updateDate,
			  CURRENT_TIMESTAMP as ETLDate
			from matchedVideo as a, mddb.trackingWebsite as b
			where date_format(a.created_at, "%%Y-%%m-%%d") > "%(min_reportDate)s" 
			  and date_format(a.created_at, "%%Y-%%m-%%d") <= "%(max_reportDate)s"
			  and date_format(a.updated_at, "%%Y-%%m-%%d") <= "%(max_reportDate)s"
			  and a.company_id  =14
			  and b.website_type = "ugc"
			  and a.created_at >= "2015-03-01" 
			  and a.trackingWebsite_id = b.id
			union all
			select
			  a.id as matchedVideo_id,
			  a.trackingMeta_id,
			  a.trackingWebsite_id,
			  date_format(a.first_send_notice_date, "%%Y-%%m-%%d") as firstSendNoticeDate,
			  a.hide_flag as hideFlag,
			  count_send_notice as countSendNotice,
			  date_format(a.created_at, "%%Y-%%m-%%d") as reportDate,
			  date_format(a.updated_at, "%%Y-%%m-%%d") as updateDate,
			  CURRENT_TIMESTAMP as ETLDate
			from matchedVideo as a, mddb.trackingWebsite as b
			where date_format(a.created_at, "%%Y-%%m-%%d") <= "%(min_reportDate)s" 
			  and date_format(a.updated_at, "%%Y-%%m-%%d") > "%(min_updateDate)s"
			  and date_format(a.updated_at, "%%Y-%%m-%%d") <= "%(max_reportDate)s"
			  and a.company_id  =14
			  and b.website_type = "ugc"
			  and a.created_at >= "2015-03-01" 
			  and a.trackingWebsite_id = b.id
		""" %date_dict
		vtweb_mysql = MySQLHelper(host=vt_host, user=vt_user,passwd=vt_passwd, port = vt_port, db_name = vt_db)
		vtweb_mysql.queryCMD("set time_zone  = '-8:00'")
		data = vtweb_mysql.queryCMD(get_data_sql)

		target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, \
			db_name = target_db, port = target_port, charset = 'utf8')
	        insert_sql = """
	    	    insert into matchedVideo
	        	(matchedVideo_id, trackingMeta_id, trackingWebsite_id, firstSendNoticeDate, hideFlag, 
	        		countSendNotice,  reportDate, updateDate, ETLDate)
			values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
			ON DUPLICATE KEY UPDATE firstSendNoticeDate = values(firstSendNoticeDate), hideFlag = values(hideFlag), 
				countSendNotice = values(countSendNotice), reportDate = values(reportDate), 
				updateDate = values(updateDate), ETLDate  =values(ETLDate)		
		"""
		target_mysql.executeManyCMD(insert_sql, data)
		target_mysql.commit()	
	except Exception, e:
		logger.debug("extral data from tracker2.matchedVideo, %s" %e)
		sendToMe(subject = "titleBased_infringAllViews ERROR", body = re.sub(r'\'|"|!', "", str(e)))
		sys.exit(0)
	finally:
		vtweb_mysql.closeConn()
		vtweb_mysql.closeCur()
		target_mysql.closeCur()
		target_mysql.closeConn()
	logger.info(" extral data from tracker2.matchedVideo end")	

def main():
	sendToMe(subject = "matchedVideo start", body = "matchedVideo start")

	while True:
		min_reportDate = getMinDatePara("matchedVideo", "reportDate")
		if not min_reportDate:
			min_reportDate = "2015-02-28"
		max_reportDate = time.strftime("%Y-%m-%d", \
			time.localtime(time.mktime(time.strptime(str(min_reportDate), "%Y-%m-%d")) + 1 * 24 * 60 * 60))

		min_updateDate = getMinDatePara("matchedVideo", "updateDate")
		if not min_updateDate:
			min_updateDate = "2015-02-28"
			
		if str(max_reportDate) >= str(time.strftime('%Y-%m-%d',time.localtime(time.time() - 0 * 24 * 60 * 60))):
			break

		getMatchedVideo(min_reportDate, max_reportDate, min_updateDate)

	sendToMe(subject = "matchedVideo end", body = "matchedVideo end")
	# ---------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
	main()
	

