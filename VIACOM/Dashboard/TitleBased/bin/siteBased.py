#!/usr/bin/env python
#coding:utf8
#Date: 2016-03-14
#Author: cwj
#Desc: data from TitleBased1 to SiteBased, TitleBasedRemoveNum to  SiteBasedRemoveNum
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

logger = logging.getLogger("siteBased")
logger.setLevel(logging.DEBUG)
log_file = '/Job/VIACOM/Dashboard/TitleBased/log/SiteBased.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

cfg_file = "/Job/VIACOM/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
if not os.path.exists(cfg_file):
	logging.debug(": config file not exists") 
	sys.exit(0)
#################################################################################################################################
target_server_section = "target_server_staging"
target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
#################################################################################################################################
#--------------------------------------------------------------------------------------------------------------------------------
logger.info(" aggregate data from TitleBased1 to  SiteBased start")
sendToMe(subject = "SiteBased start", body = "aggregate data from TitleBased1 to  SiteBased start")
try:
	target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, 
		db_name = target_db, port = target_port, charset = 'utf8')
	aggregate_TitleBased1_SQL = """
		select
			a.reportDate,
			a.trackingWebsite_id,
			b.websiteName,
			b.websiteType,
			b.websiteDomain,
			b.country_id,
			c.countryName as hostCountry,
			a.title,
			sum(matchedNum) as matchedNum,
			sum(matchedNumDurationNoZero) as matchedNumDurationNoZero,
			sum(infringingNum) as infringingNum,
			sum(infringingNumDurationNoZero) as infringingNumDurationNoZero,
			sum(infringingNumCMS) as infringingNumCMS,
			sum(clipDurationSum) as clipDurationSum,
			sum(clipDurationInfringingSum) as clipDurationInfringingSum,
			sum(reportedViews) as reportedViews,
			sum(infringingViews) as infringingViews,
			sum(reportedViewsCMS) as reportedViewsCMS,
			CURRENT_TIMESTAMP as ETLDate
		from VIACOM_DASHBOARD.TitleBased1 as a, VIACOM_DASHBOARD.TitleBasedTrackingWebsite as b, VIACOM_DASHBOARD.TitleBasedCountry as c
		where a.trackingWebsite_id = b.trackingWebsite_id
		  and b.country_id = c.country_id
		  and(a.websiteType = "ugc" or a.websiteType = 'cyberlocker' or a.websiteType = 'hybrid')
		  -- and a.reportDate >= date_add(now(), interval -3 month)
		group by 1, 2, 3, 4, 5, 6, 7, 8
	"""
	aggregate_TitleBased1_result = target_mysql.queryCMD(aggregate_TitleBased1_SQL)

	insert_SiteBased_SQL = """
		INSERT INTO SiteBased
			(reportDate, trackingWebsite_id, websiteName, websiteType, 
			websiteDomain, country_id, hostCountry, title, matchedNum, 
			matchedNumDurationNoZero, infringingNum, infringingNumDurationNoZero,
			infringingNumCMS, clipDurationSum, clipDurationInfringingSum,
			reportedViews, infringingViews, reportedViewsCMS, ETLDate) 
  		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
  		ON DUPLICATE KEY UPDATE 
  			websiteName = VALUES(websiteName), websiteDomain = VALUES(websiteDomain), 
  			country_id = VALUES(country_id), hostCountry = VALUES(hostCountry), 
  			matchedNum = VALUES(matchedNum), matchedNumDurationNoZero = values(matchedNumDurationNoZero), 
  			infringingNum = VALUES(infringingNum), infringingNumDurationNoZero = values(infringingNumDurationNoZero),
  			infringingNumCMS = VALUES(infringingNumCMS), 
  			clipDurationSum = VALUES(clipDurationSum),clipDurationInfringingSum = VALUES(clipDurationInfringingSum),
  			reportedViews = VALUES(reportedViews), infringingViews = values(infringingViews), 
			reportedViewsCMS = VALUES(reportedViewsCMS),
  			ETLDate = VALUES(ETLDate)
	"""
	target_mysql.insertUpdateCMD(insert_SiteBased_SQL, aggregate_TitleBased1_result)
	target_mysql.commit()
except Exception, e:
	logger.debug("load data to SiteBased, %s" %e)
	sendToMe(subject = "SiteBased Error", body = re.sub(r'\'|"|!', "", str(e)))
	sys.exit(0)
finally:
	target_mysql.closeCur()
	target_mysql.closeConn()
logger.info(" aggregate data from TitleBased1 to  SiteBased end")
#################################################################################################################################
#--------------------------------------------------------------------------------------------------------------------------------
logger.info(" aggregate data from TitleBasedRemoveNum to  SiteBasedRemoveNum start")
try:
	target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, 
		db_name = target_db, port = target_port, charset = 'utf8')
	aggregate_TitleBasedRemoveNum_SQL = """
		select
			a.reportDate,
			a.takeoffDate,
			a.trackingWebsite_id,
			a.title,
			sum(removedNum) as removedNum,
			sum(complianceTime) as complianceTime,
			CURRENT_TIMESTAMP as ETLDate
		from VIACOM_DASHBOARD.TitleBasedRemoveNum1 as a, VIACOM_DASHBOARD.TitleBasedTrackingWebsite as b
		where a.trackingWebsite_id = b.trackingWebsite_id
		  and (b.websiteType = "ugc" or b.websiteType = 'cyberlocker' or b.websiteType = 'hybrid')
		group by 1, 2, 3, 4
	"""
	aggregate_TitleBasedRemoveNum_result = target_mysql.queryCMD(aggregate_TitleBasedRemoveNum_SQL)

	insert_SiteBasedRemoveNum_SQL = """
		INSERT INTO SiteBasedRemoveNum 
			(reportDate, takeoffDate, trackingWebsite_id, title, removedNum, complianceTime, ETLDate) 
  		VALUES (%s, %s, %s, %s, %s, %s, %s) 
  		ON DUPLICATE KEY UPDATE 
  			removedNum = VALUES(removedNum), complianceTime = VALUES(complianceTime), ETLDate = VALUES(ETLDate)
	"""
	target_mysql.queryNoData("delete from SiteBasedRemoveNum")
	target_mysql.insertUpdateCMD(insert_SiteBasedRemoveNum_SQL, aggregate_TitleBasedRemoveNum_result)
	target_mysql.commit()
except Exception, e:
	logger.debug("load data to SiteBasedRemoveNum, %s" %e)
	sendToMe(subject = "SiteBased Error", body = re.sub(r'\'|"|!', "", str(e)))
	sys.exit(0)
finally:
	target_mysql.closeCur()
	target_mysql.closeConn()
logger.info(" aggregate data from TitleBasedRemoveNum to  SiteBasedRemoveNum end")
sendToMe(subject = "SiteBased End", body = "aggregate data from TitleBasedRemoveNum to  SiteBasedRemoveNum end")

