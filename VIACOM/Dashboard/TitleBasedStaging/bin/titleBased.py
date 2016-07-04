#!/usr/bin/env python
#coding:utf8
#Date: 2016-03-14
#Author: cwj
#Desc: data from vtweb to TitleBased
#
#

from mysqlHelp import MySQLHelper
import ConfigParser
from parseConfig import CfgParser
import sys
import os
import time
import logging
import logging.handlers
 
logging.basicConfig(format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
#logging.setFormatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
logger = logging.getLogger("titleBased")

logger.setLevel(logging.DEBUG)
log_file = '/Job/VIACOM/Dashboard/TitleBasedStaging/log/titleBased.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)


cfg_file = "/Job/VIACOM/Dashboard/TitleBasedStaging/conf/viacom_dashboard.cfg"
if not os.path.exists(cfg_file):
	logger.debug(": config file not exists") 
	sys.exit(0)

def getConfMysqlInfo(section_name):
	conf = CfgParser(cfg_file).parse()
	try:
		host = conf[section_name]["host"]
		user = conf[section_name]["user"]
		passwd = conf[section_name]["passwd"]
		port = conf[section_name]["port"]
		db = conf[section_name]["db"]

		return host, user, passwd, port, db
	except KeyError, e:
		logger.debug(": mysql config file section or option not exists, %s" %e)
		sys.exit(0)

def getMinDatePara(table_name, date_para):
	#get max report date from table TitleBased
	target_server_section = "target_server_staging"
	target_host, target_user, target_passwd, target_port, target_db = getConfMysqlInfo(target_server_section)
	try:
		target_mysql = MySQLHelper(host=target_host, user=target_user,passwd=target_passwd, port = target_port, db_name = target_db)
		
		sql = "select max(%s) from %s" %(date_para, table_name)
		min_date = target_mysql.queryCMD(sql)[0][0]
		#if min_date == None:
		#	min_date =  time.strftime('%Y-%m-%d',time.localtime(time.time() - 24 * 60 * 60 * 365))
	except Exception , e:
		logger.debug(": get last max report date from %s, %s" %(table_name, e))
	finally:
		target_mysql.closeCur()
		target_mysql.closeConn()

	return min_date
#################################################################################################################################	 
# get date parameter
if __name__ == "__main__":
	logger.info(": extract data from tracker2 reportDate start")
	date_para_TitleBased_reportDate_min = getMinDatePara("TitleBased", "reportDate")
	date_para_TitleBased_firstSendNoticeDate_min = getMinDatePara("TitleBased", "firstSendNoticeDate")
	if date_para_TitleBased_reportDate_min == None:
		date_para_TitleBased_reportDate_min = "2015-02-28"
	date_para_TitleBased_reportDate_max = time.strftime("%Y-%m-%d", time.localtime(time.time() - 0 * 24 * 60 * 60))
	date_para_TitleBased_report_dict = {"date_para_TitleBased_reportDate_min":date_para_TitleBased_reportDate_min, 
							"date_para_TitleBased_reportDate_max":date_para_TitleBased_reportDate_max}
	vt_TitleBased_SQL = """
		select 
		  date_format(a.created_at, "%%Y-%%m-%%d") as reportDate, 
		  date_format(a.first_send_notice_date, "%%Y-%%m-%%d") as firstSendNoticeDate,
		  a.trackingWebsite_id,  a.trackingMeta_id, 
		  case when b.priority_type = "tier 1"  then "tier1" 
		    when  b.priority_type = "tier 2"  then "tier2" 
		    when b.priority_type = "tier 3"  then "tier3"  
		    else "unknown" end  as tier,
		  count(*) as matchedNum, sum(if(a.hide_flag = 2 and a.count_send_notice > 0, 1, 0)) as infringingNum, 
		  sum(a.clip_duration) as clipDurationSum,
		  CURRENT_TIMESTAMP as ETLDate
		from tracker2.matchedVideo as a, tracker2.metaExtraInfo as b , mddb.trackingWebsite as c
		where a.trackingMeta_id = b.meta_id
		  and a.trackingWebsite_id = c.id
		  and b.company_id = 14
		  and a.company_id = 14
		  and (c.website_type = "ugc" or c.website_type = "hybrid")
		  and date_format(a.first_send_notice_date, "%%Y-%%m-%%d") < "%(date_para_TitleBased_reportDate_max)s"
		  and date_format(a.created_at, "%%Y-%%m-%%d") > "%(date_para_TitleBased_reportDate_min)s"
		  and date_format(a.created_at, "%%Y-%%m-%%d") < "%(date_para_TitleBased_reportDate_max)s"
		group by 1, 2, 3, 4, 5

		UNION ALL

		select 
		  date_format(a.created_at, "%%Y-%%m-%%d") as reportDate,
		  date_format(a.first_send_notice_date, "%%Y-%%m-%%d") as firstSendNoticeDate,
		  a.trackingWebsite_id,
		  a.trackingMeta_id,
		  case when c.priority_type = "tier 1"  then "tier1" 
		    when  c.priority_type = "tier 2"  then "tier2" 
		    when c.priority_type = "tier 3"  then "tier3"  
		    else "unknown" end  as tier,
		  count(*) as matchedNum,
		  sum(if(a.hide_flag = 2 and a.count_send_notice > 0, 1, 0)) as infringingNum,
		  SUM(d.file_size/1024/1024) as  'clipDurationSum',
		  CURRENT_TIMESTAMP as ETLDate
		from tracker2.matchedVideo a, mddb.trackingWebsite b, tracker2.metaExtraInfo as c, tracker2.matchedFileItem d
		where d.matchedFile_id =  a.matchedFile_id
		  AND a.trackingWebsite_id = b.id
		  AND a.trackingMeta_id = c.meta_id
		  AND a.company_id = 14  
		  AND b.website_type = 'cyberlocker' 
		  AND date_format(a.first_send_notice_date, "%%Y-%%m-%%d") < "%(date_para_TitleBased_reportDate_max)s"
		  AND date_format(a.created_at, "%%Y-%%m-%%d") > "%(date_para_TitleBased_reportDate_min)s"
		  AND date_format(a.created_at, "%%Y-%%m-%%d") < "%(date_para_TitleBased_reportDate_max)s"
		group by 1, 2, 3, 4, 5
	""" %date_para_TitleBased_report_dict
	try:
		#vtweb_tracker2_section = "vtweb_tracker2"
		vtweb_tracker2_section = "vtweb_staging"		
		vt_host, vt_user, vt_passwd, vt_port, vt_db = getConfMysqlInfo(vtweb_tracker2_section)
		vtweb_mysql = MySQLHelper(host=vt_host, user=vt_user,passwd=vt_passwd, port = vt_port, db_name = vt_db)

		# get data from vtweb
		vtweb_mysql.queryCMD("set time_zone = '-5:00'")
		result_report = vtweb_mysql.queryCMD(vt_TitleBased_SQL)
	except Exception, e:
		logger.debug(": mysql config file section or option not exists, %s" %e)
		sys.exit(0)
	finally:
		vtweb_mysql.closeCur()
		vtweb_mysql.closeConn()
		logger.info(": extract data from tracker2 reportDate  end")	

	# put data to TitleBased
	logger.info(":load data to TitleBased reportDate start")
	try:
		target_server_section = "target_server_staging"
		target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
		target_mysql = MySQLHelper(host=target_host, user=target_user, 
			passwd=target_passwd, db_name = target_db, port = target_port, charset = 'utf8')

		target_TitleBased_SQL = """
		 insert into TitleBased (reportDate, firstSendNoticeDate, 
		 	trackingWebsite_id, trackingMeta_id, tier, matchedNum, 
		 	infringingNum, clipDurationSum, ETLDate)
		 values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
		"""
		target_mysql.executeManyCMD(target_TitleBased_SQL, result_report)
		target_mysql.commit()
	except Exception, e:
		logger.debug(": mysql data to TitleBased,  %s" %e)
		sys.exit(0)
	finally:
		target_mysql.closeCur()
		target_mysql.closeConn()
		logger.info(": data to TitleBased reportDate end")
#################################################################################################################################
	logger.info(": extract data from tracker2 firstSendNoticeDate start")
#	date_para_TitleBased_firstSendNoticeDate_min = getMinDatePara("TitleBased", "firstSendNoticeDate")
	if date_para_TitleBased_firstSendNoticeDate_min == None:
		date_para_TitleBased_firstSendNoticeDate_min = "2015-01-01"
	date_para_TitleBased_firstSendNoticeDate_max = time.strftime("%Y-%m-%d", time.localtime(time.time() - 0 * 24 * 60 * 60))
	date_para_TitleBased_notice_dict = {"date_para_TitleBased_firstSendNoticeDate_min": date_para_TitleBased_firstSendNoticeDate_min,
							"date_para_TitleBased_firstSendNoticeDate_max": date_para_TitleBased_firstSendNoticeDate_max,
							"date_para_TitleBased_report_min": "2015-03-01", 
							"date_para_TitleBased_report_max": date_para_TitleBased_reportDate_min}
	vt_TitleBased_SQL = """
		select 
		  date_format(a.created_at, "%%Y-%%m-%%d") as reportDate, 
		  date_format(a.first_send_notice_date, "%%Y-%%m-%%d") as firstSendNoticeDate,
		  a.trackingWebsite_id,  a.trackingMeta_id, 
		  case when b.priority_type = "tier 1"  then "tier1" 
		    when  b.priority_type = "tier 2"  then "tier2" 
		    when b.priority_type = "tier 3"  then "tier3"  
		    else "unknown" end  as tier,
		  count(*) as matchedNum, sum(if(a.hide_flag = 2 and a.count_send_notice > 0, 1, 0)) as infringingNum, 
		  sum(a.clip_duration) as clipDurationSum,
		  CURRENT_TIMESTAMP as ETLDate
		from tracker2.matchedVideo as a, tracker2.metaExtraInfo as b , mddb.trackingWebsite as c
		where a.trackingMeta_id = b.meta_id
		  and a.trackingWebsite_id = c.id
		  and b.company_id = 14
		  and a.company_id = 14
		  and (c.website_type = "ugc" or c.website_type = "hybrid")
		  and date_format(a.created_at, "%%Y-%%m-%%d") >= "%(date_para_TitleBased_report_min)s"
		  and date_format(a.created_at, "%%Y-%%m-%%d") <= "%(date_para_TitleBased_report_max)s"
		  and date_format(a.first_send_notice_date, "%%Y-%%m-%%d") > "%(date_para_TitleBased_firstSendNoticeDate_min)s"
		  and date_format(a.first_send_notice_date, "%%Y-%%m-%%d") < "%(date_para_TitleBased_firstSendNoticeDate_max)s"
		group by 1, 2, 3, 4, 5
		UNION ALL
		select 
		  date_format(a.created_at, "%%Y-%%m-%%d") as reportDate,
		  date_format(a.first_send_notice_date, "%%Y-%%m-%%d") as firstSendNoticeDate,
		  a.trackingWebsite_id,
		  a.trackingMeta_id,
		  case when c.priority_type = "tier 1"  then "tier1" 
		    when  c.priority_type = "tier 2"  then "tier2" 
		    when c.priority_type = "tier 3"  then "tier3"  
		    else "unknown" end  as tier,
		  count(*) as matchedNum,
		  sum(if(a.hide_flag = 2 and a.count_send_notice > 0, 1, 0)) as infringingNum,
		  SUM(d.file_size/1024/1024) as  'clipDurationSum',
		  CURRENT_TIMESTAMP as ETLDate
		from tracker2.matchedVideo a, mddb.trackingWebsite b, tracker2.metaExtraInfo as c, tracker2.matchedFileItem d
		where d.matchedFile_id =  a.matchedFile_id
		  AND a.trackingWebsite_id = b.id
		  AND a.trackingMeta_id = c.meta_id
		  AND a.company_id = 14  
		  AND b.website_type = 'cyberlocker' 
		  and date_format(a.created_at, "%%Y-%%m-%%d") >= "%(date_para_TitleBased_report_min)s"
		  and date_format(a.created_at, "%%Y-%%m-%%d") <= "%(date_para_TitleBased_report_max)s"
		  AND date_format(a.first_send_notice_date, "%%Y-%%m-%%d") > "%(date_para_TitleBased_firstSendNoticeDate_min)s"
		  AND date_format(a.first_send_notice_date, "%%Y-%%m-%%d") < "%(date_para_TitleBased_firstSendNoticeDate_max)s"
		group by 1, 2, 3, 4, 5
	""" %date_para_TitleBased_notice_dict
	try:
		#vtweb_tracker2_section = "vtweb_tracker2"
		vtweb_tracker2_section = "vtweb_staging"		
		vt_host, vt_user, vt_passwd, vt_port, vt_db = getConfMysqlInfo(vtweb_tracker2_section)
		vtweb_mysql = MySQLHelper(host=vt_host, user=vt_user,passwd=vt_passwd, port = vt_port, db_name = vt_db)

		# get data from vtweb
		vtweb_mysql.queryCMD("set time_zone = '-5:00'")
		result_notice = vtweb_mysql.queryCMD(vt_TitleBased_SQL)
	except Exception, e:
		logger.debug(": mysql config file section or option not exists, %s" %e)
		sys.exit(0)
	finally:
		vtweb_mysql.closeCur()
		vtweb_mysql.closeConn()
		logger.info(": extract data from tracker2 firstSendNoticeDate end")	

	# put data to TitleBased
	logger.info(":load data to TitleBased firstSendNoticeDate start")
	try:
		target_server_section = "target_server_staging"
		target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
		target_mysql = MySQLHelper(host=target_host, user=target_user, 
			passwd=target_passwd, db_name = target_db, 
			port = target_port, charset = 'utf8')

		target_TitleBased_SQL = """
		 insert into TitleBased 
		 	(reportDate, firstSendNoticeDate, 
		 	trackingWebsite_id, trackingMeta_id, tier, matchedNum, 
		 	infringingNum, clipDurationSum, ETLDate)
		 values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
		"""
		target_mysql.executeManyCMD(target_TitleBased_SQL, result_notice)
		target_mysql.commit()
	except Exception, e:
		logger.debug(": mysql data to TitleBased firstSendNoticeDate,  %s" %e)
		sys.exit(0)
	finally:
		target_mysql.closeCur()
		target_mysql.closeConn()
		logger.info(": data to TitleBased firstSendNoticeDate end")
#################################################################################################################################


