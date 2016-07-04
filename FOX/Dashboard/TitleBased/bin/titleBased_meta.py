#!/usr/bin/env python
#coding:utf8
#Date: 2016-03-14
#Author: cwj
#Desc: data from vtweb to TitleBasedMeta
#
#

from mysqlHelp import MySQLHelper
import ConfigParser
from parseConfig import CfgParser
import sys
import os
import logging
import time
from titleBased import getConfMysqlInfo, commitInTurn
from sendMail import sendToMe

logger = logging.getLogger("titleBased_meta")
logger.setLevel(logging.DEBUG)
log_file = '/Job/FOX/Dashboard/TitleBased/log/titleBased_meta.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

cfg_file = "/Job/FOX/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
if not os.path.exists(cfg_file):
	logging.debug(": config file not exists") 
	sendToMe(subject = "titleBased_meta Error", body = "config file not exists")
	sys.exit(0)
#################################################################################################################################
def getDataFromVT():
	logger.info(": extract data from tracker2 start")
	# extract dat from vtweb
	vt_TitleBasedMeta_SQL = """
	select a.id as trackingMeta_id, a.meta_type, a.meta_title as title, CURRENT_TIMESTAMP as ETLDate 
	from meta as a, metaExtraInfo as b
	where a.company_id = 10
	  and b.company_id = 10
	  and a.id = b.meta_id
	"""
	#vtweb_tracker2_section = "vtweb_tracker2"
	vtweb_tracker2_section = "vtweb_staging"
	try:
		vt_host, vt_user, vt_passwd, vt_port, vt_db = getConfMysqlInfo(vtweb_tracker2_section)
		vtweb_mysql = MySQLHelper(host=vt_host, user=vt_user,passwd=vt_passwd, port = vt_port, db_name = vt_db)
		result = vtweb_mysql.queryCMD(vt_TitleBasedMeta_SQL)
	except Exception, e:
		logger.debug(": extract data from vt for dimension meta, %s" %e)
		sendToMe(subject = "titleBased_meta ERROR", body = e)
		sys.exit(0)
	finally:
		vtweb_mysql.closeCur()
		vtweb_mysql.closeConn()
		logger.info(": extract data from tracker2 start")

	return result

def loadDataTo123(vtweb_data):
	logger.info(":load data to TitleBasedMeta  start")
	target_server_section = "target_server_staging"
	try:
		target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
		target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, db_name = target_db, port = target_port, charset = 'utf8')
		insertUpdate_SQL = """
			INSERT INTO TitleBasedMeta(trackingMeta_id, metaType, title, ETLDate) VALUES(%s, %s, %s, %s) on duplicate  key update title = values(title),
			ETLDate = values(ETLDate), metaType = values(metaType)
		"""
		commitInTurn(commit_num = 50000, data = vtweb_data, executeFun = target_mysql.insertUpdateCMD, \
			commitFun = target_mysql.commit, executeSQL = insertUpdate_SQL)
		#target_mysql.insertUpdateCMD(insertUpdate_SQL, result)
		#target_mysql.commit()
	except Exception, e:
		logger.debug(": load data to TitleBasedMeta, %s" %e)
		sendToMe(subject = "titleBased_meta ERROR", body = e)
		sys.exit(0)
	finally:
		target_mysql.closeCur()
		target_mysql.closeConn()
		logger.info(":load data to TitleBasedMeta  end")

def main():
	sendToMe(subject = "titleBased_meta start", body = "extract data from tracker2 start")
	vtweb_data = getDataFromVT()
	loadDataTo123(vtweb_data)
	sendToMe(subject = "titleBased_meta End", body = "load data to TitleBasedMeta  end")

if __name__ == "__main__":
	main()
#################################################################################################################################

