#!/usr/bin/env python
#coding:utf8
#Date: 2016-03-14
#Author: cwj
#Desc: data from TitleBased to TitleBased1
#


from mysqlHelp import MySQLHelper
import ConfigParser
from parseConfig import CfgParser
import sys
import os
import logging
import time
from titleBased import getConfMysqlInfo, getMinDatePara, commitInTurn
from sendMail import sendToMe
import re

logger = logging.getLogger("titleBased_titleBased")
logger.setLevel(logging.DEBUG)
log_file = '/Job/HBO/Dashboard/TitleBased/log/titleBased_titleBased.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

cfg_file = "/Job/HBO/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
if not os.path.exists(cfg_file):
	logging.debug(": config file not exists") 
	sendToMe(subject = "titleBased_titleBased ERROR", body = "config file not exists")
	sys.exit(0)
#################################################################################################################################
#target_server_section = "target_server_staging"
#target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
#################################################################################################################################
def aggregateDataToTitleBased1():
	logger.info(" aggregate data from TitleBased  to  TitleBased1  start")
	target_server_section = "target_server_staging"
	target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
	try:
		target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, 
			db_name = target_db, port = target_port, charset = 'utf8')
		aggregate_TitleBased1_SQL = """
			select
			  a.reportDate,
			  a.trackingWebsite_id,
			  ifnull(c.displayName, a.websiteName) as websiteName,
			  a.websiteType,
			  ifnull(b.mapTitle, a.title) title,
			  min(a.tier) as tier,
			  sum(matchedNum) as matchedNum,
			  sum(matchedNumDurationNoZero) as matchedNumDurationNoZero,
			  sum(infringingNum) as infringingNum,
			  sum(infringingNumDurationNoZero) as infringingNumDurationNoZero,
			  sum(clipDurationSum) as clipDurationSum,
			  sum(clipDurationInfringingSum) as clipDurationInfringingSum,
			  current_timestamp as ETLDate
			from 
			  (select 
			    a.reportDate,
			    a.trackingWebsite_id,
			    c.websiteName,
			    c.websiteType,
			    b.title,
			    min(a.tier) tier,
			    sum(matchedNum) as matchedNum,
			    sum(matchedNumDurationNoZero) as matchedNumDurationNoZero,
			    sum(infringingNum) as infringingNum,
			    sum(infringingNumDurationNoZero) as infringingNumDurationNoZero,
			    sum(clipDurationSum) as clipDurationSum,
			    sum(clipDurationInfringingSum) as clipDurationInfringingSum,
			    current_timestamp as ETLDate
			  from TitleBasedTmp as a, TitleBasedMeta as b, TitleBasedTrackingWebsite as c
			  where a.trackingWebsite_id = c.trackingWebsite_id
			    and a.trackingMeta_id = b.trackingMeta_id
			  group by 1, 2, 3, 4, 5) as a 
			  left join MetaTitleMapTitle as b 
			  on a.title = b.metaTitle 
			  left join SiteMap as c
			  on a.trackingWebsite_id = c.trackingWebsite_id
			  group by 1, 2, 3, 4, 5
		""" #%date_para_TitleBased1_dict

		TitleBased1_result = target_mysql.queryCMD(aggregate_TitleBased1_SQL)

		target_mysql.queryNoData("delete from TitleBased1;")
	
		insert_TitleBased1_SQL = """
			INSERT INTO TitleBased1 
				(reportDate, trackingWebsite_id, websiteName, websiteType, title, tier,
					matchedNum, matchedNumDurationNoZero, infringingNum, infringingNumDurationNoZero, 
					clipDurationSum, clipDurationInfringingSum, ETLDate) 
	  		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
	  		ON DUPLICATE KEY UPDATE matchedNum = VALUES(matchedNum), clipDurationSum = VALUES(clipDurationSum),
	  			infringingNum = VALUES(infringingNum), ETLDate = VALUES(ETLDate), matchedNumDurationNoZero = VALUES(matchedNumDurationNoZero),
	  			matchedNumDurationNoZero = VALUES(matchedNumDurationNoZero), infringingNumDurationNoZero = VALUES(infringingNumDurationNoZero),
	  			clipDurationInfringingSum = VALUES(clipDurationInfringingSum),
				websiteName = VALUES(websiteName),
				tier = VALUES(tier)
		"""

		commitInTurn(commit_num = 50000, data = TitleBased1_result, executeFun = target_mysql.insertUpdateCMD, \
	        commitFun = target_mysql.commit, executeSQL = insert_TitleBased1_SQL)
		#target_mysql.insertUpdateCMD(insert_TitleBased1_SQL, TitleBased1_result)
		#target_mysql.commit()
	except Exception, e:
		logger.debug(": load data to TitleBased1, %s" %e)
		sendToMe(subject = "titleBased_titleBased ERROR", body = re.sub(r'\'|"|!', "", str(e)))
		sys.exit(0)
	finally:
		target_mysql.closeCur()
		target_mysql.closeConn()
	logger.info(" aggregate data from TitleBased  to  TitleBased1 end")

def main():
	sendToMe(subject = "titleBased_titleBased start", body = "aggregate data from TitleBased  to  TitleBased1  start")
	aggregateDataToTitleBased1()
	sendToMe(subject = "titleBased_titleBased end", body = "aggregate data from TitleBased  to  TitleBased1 end")

if __name__ == "__main__":
	main()



