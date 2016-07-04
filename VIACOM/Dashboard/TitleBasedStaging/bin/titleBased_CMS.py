#!/usr/bin/env python
#coding:utf8
#Date: 2016-03-14
#Author: cwj
#Desc: data from vtweb to TitleBasedReportedViews and TitleBased
#


from mysqlHelp import MySQLHelper
import ConfigParser
from parseConfig import CfgParser
import sys
import os
import logging
import time
from titleBased import getConfMysqlInfo, getMinDatePara

logger = logging.getLogger("titleBased_views")
logger.setLevel(logging.DEBUG)
log_file = '/Job/VIACOM/Dashboard/TitleBasedStaging/log/titleBased_views.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

cfg_file = "/Job/VIACOM/Dashboard/TitleBasedStaging/conf/viacom_dashboard.cfg"
if not os.path.exists(cfg_file):
	logging.debug(": config file not exists") 
	sys.exit(0)
#################################################################################################################################
target_server_section = "target_server_staging"
target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
#################################################################################################################################

##CMS data and reportedViews (UGC) from SelfService_Aggregate_ByNoticedDate aggregate to TitleBased1
#--------------------------------------------------------------------------------------------------------------------------------
logger.info(" aggregate data from DM_VIACOM.SelfService_Aggregate_ByNoticedDate (CMS data)  start")

date_para_CMS_min = time.strftime("%Y-%m-%d", time.localtime(time.time()- (65) * 24 * 60 * 60))
date_para_CMS_min = "2015-03-01" 
date_para_CMS_max = time.strftime("%Y-%m-%d", time.localtime(time.time()- 1 * 24 * 60 * 60))

date_para_CMS_dict = {"date_para_CMS_min":date_para_CMS_min, "date_para_CMS_max": date_para_CMS_max}

try:
	target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, 
		db_name = target_db, port = target_port, charset = 'utf8')
	aggregate_CMS_SQL = """
		select 
		  Date_ID as reportDate,
		  b.trackingWebsite_id,
		  b.websiteName,
		  b.websiteType,
		  title,
		  sum(CMSInfringingNum) as infringingNumCMS,
		  sum(CMSReportViews) as reportedViewsCMS,
		  current_timestamp as ETLDate
		from DM_VIACOM.SelfService_Aggregate_ByNoticedDate as a, TitleBasedTrackingWebsite as b
		where  a.trackingWebsite_id = b.trackingWebsite_id
		  and a.WebsiteType = 'ugc'
		  and b.WebsiteType = 'ugc'
		  and a.trackingWebsite_id = 1
		  and a.Date_ID >= '%(date_para_CMS_min)s'
		  and a.Date_ID < '%(date_para_CMS_max)s'
		group by 1, 2, 3, 4, 5
	""" %date_para_CMS_dict

	CMS_result = target_mysql.queryCMD(aggregate_CMS_SQL)

	insert_CMS_SQL = """
		INSERT INTO TitleBased1 
			(reportDate, trackingWebsite_id, websiteName, websiteType, title, 
				infringingNumCMS, reportedViewsCMS, ETLDate) 
  		VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
  		ON DUPLICATE KEY UPDATE infringingNumCMS = VALUES(infringingNumCMS),
  			reportedViewsCMS = VALUES(reportedViewsCMS), ETLDate = VALUES(ETLDate)
	"""
	target_mysql.insertUpdateCMD(insert_CMS_SQL, CMS_result)
	target_mysql.commit()
except Exception, e:
	logger.debug(": load data to TitleBased1, %s" %e)
	sys.exit(0)
finally:
	target_mysql.closeCur()
	target_mysql.closeConn()
logger.info(" aggregate data from DM_VIACOM.SelfService_Aggregate_ByNoticedDate (CMS data) to  TitleBased1 end")

