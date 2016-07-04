#!/usr/bin/env python
#coding:utf8
#Date: 2016-03-14
#Author: cwj
#Desc: data from vtweb to TitleBasedCountry
#
#

from mysqlHelp import MySQLHelper
import ConfigParser
from parseConfig import CfgParser
import sys
import os
import logging
from titleBased import getConfMysqlInfo

logger = logging.getLogger("titleBased_country")
logger.setLevel(logging.DEBUG)
log_file = '/Job/VIACOM/Dashboard/TitleBased/log/titleBased_country.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

cfg_file = "/Job/VIACOM/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
if not os.path.exists(cfg_file):
	logging.debug(": config file not exists; file_name %s" %cfg_file) 
	sendToMe(subject = "titleBased_country ERROR", body = "config file not exists")
	sys.exit(0)

logger.info(": extract data from tracker2 start")
sendToMe(subject = "titleBased_country start", body = "extract data from tracker2 start")
# extract dat from vtweb
vt_TitleBasedTrackingWebsite_SQL = """
	select
	  id as country_id,
	  region,
	  country_name as countryName,
	  national_flag as nationalFlag,
	  CURRENT_TIMESTAMP as ETLDate
	from mddb.country
"""
#vtweb_tracker2_section = "vtweb_tracker2"
vtweb_tracker2_section = "vtweb_staging"
try:
	vt_host, vt_user, vt_passwd, vt_port, vt_db = getConfMysqlInfo(vtweb_tracker2_section)
	vtweb_mysql = MySQLHelper(host=vt_host, user=vt_user,passwd=vt_passwd, port = vt_port, db_name = vt_db)
	result = vtweb_mysql.queryCMD(vt_TitleBasedTrackingWebsite_SQL)
except Exception, e:
	logger.debug(": extract data from vt for dimension country, %s" %e)
	sendToMe(subject = "titleBased_country ERROR", body = e)
	sys.exit(0)
finally:
	vtweb_mysql.closeCur()
	vtweb_mysql.closeConn()
	logger.info(": extract data from tracker2 start")

logger.info(":load data to TitleBasedCountry  start")
target_server_section = "target_server_staging"
try:
	target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
	target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, db_name = target_db, port = target_port, charset = 'utf8')
	insertUpdate_SQL = """
		INSERT INTO TitleBasedCountry
		(country_id, region, countryName, nationalFlag, ETLDate) 
		VALUES(%s, %s, %s, %s, %s) 
		on duplicate  key update 
			region = values(region), ETLDate = values(ETLDate), 
			countryName = values(countryName), nationalFlag = VALUES(nationalFlag)
	"""

	target_mysql.insertUpdateCMD(insertUpdate_SQL, [(t[0], t[1], t[2].title(), t[3], t[4]) for t in result])
	target_mysql.commit()
except Exception, e:
	logger.debug(": load data to TitleBasedCountry, %s" %e)
	sendToMe(subject = "titleBased_country ERROR", body = e)
	sys.exit(0)
finally:
	target_mysql.closeCur()
	target_mysql.closeConn()
	logger.info(":load data to TitleBasedCountry  end")
sendToMe(subject = "TitleBasedCountry end", body = "load data to TitleBasedCountry  end")	
#################################################################################################################################
