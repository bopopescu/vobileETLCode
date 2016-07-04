#!/usr/bin/env python
#coding:utf8
#Date: 2016-03-14
#Author: cwj
#Desc: data from vtweb to TitleBasedRemoveNum and TitleBasedRemoveNum1
#


from mysqlHelp import MySQLHelper
import ConfigParser
import re
from parseConfig import CfgParser
import sys
import os
import logging
import time
from titleBased import getConfMysqlInfo, getMinDatePara, commitInTurn
from sendMail import sendToMe

logger = logging.getLogger("titleBased_remove")
logger.setLevel(logging.DEBUG)
log_file = '/Job/FOX/Dashboard/TitleBased/log/titleBased_remove.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

cfg_file = "/Job/FOX/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
if not os.path.exists(cfg_file):
	logging.debug(": config file not exists") 
	sendToMe(subject = "titleBased_remove ERROR", body = "config file not exists")
	sys.exit(0)
#################################################################################################################################
def getDataFromVT():
	logger.info(": extract data from tracker2 start")
	date_para_TitleBasedRemoveNum_min = getMinDatePara(table_name = "TitleBasedRemoveNum", date_para = "takeoffDate")
	if date_para_TitleBasedRemoveNum_min == None:
		date_para_TitleBasedRemoveNum_min = "2015-02-28"
	date_para_TitleBasedRemoveNum_min = "2015-02-28"
	date_para_TitleBasedRemoveNum_max = time.strftime("%Y-%m-%d", time.localtime(time.time() - 0 * 24 * 60 * 60))

	date_para_TitleBasedRemoveNum_dict = {"date_para_TitleBasedRemoveNum_min":date_para_TitleBasedRemoveNum_min, \
		"date_para_TitleBasedRemoveNum_max":date_para_TitleBasedRemoveNum_max, "min_report_date": "2015-03-01"}
	vt_TitleBasedRemoveNum_SQL = """
		select
		  date_format(a.created_at, "%%Y-%%m-%%d") as reportDate,
		  date_format(a.takeoff_time, "%%Y-%%m-%%d") as takeoffDate,
		  a.trackingWebsite_id,
		  a.trackingMeta_id,
		  count(*) removedNum,
		  sum(case when a.first_send_notice_date >0 and a.takeoff_time>0  
		  	then TIMESTAMPDIFF(MINUTE, a.first_send_notice_date, a.takeoff_time) else 0  end) as complianceTime,
		  CURRENT_TIMESTAMP as ETLDate
		from matchedVideo as a, trackingWebsite as b
		where a.trackingWebsite_id = b.id
		  and a.company_id = 10
		  and b.website_type = "ugc"
		  and a.count_send_notice > 0
		  and hide_flag = 2
		  and a.first_send_notice_date < a.takeoff_time
		  and a.first_send_notice_date > 0
		  and a.created_at >= "%(min_report_date)s"
		  and date_format(a.takeoff_time, "%%Y-%%m-%%d") > "%(date_para_TitleBasedRemoveNum_min)s"
		  and date_format(a.takeoff_time, "%%Y-%%m-%%d") < "%(date_para_TitleBasedRemoveNum_max)s"
		group by 1, 2, 3, 4
		UNION ALL
		select 
		  reportDate,
		  takeoffDate,
		  trackingWebsite_id,
		  trackingMeta_id,
		  sum(removedNum) removedNum,
		  sum(complianceTime) complianceTime,
		  CURRENT_TIMESTAMP as ETLDate
		from  (select
		    date_format(a.created_at, "%%Y-%%m-%%d") as reportDate,
		    date_format(a.takeoff_time, "%%Y-%%m-%%d") as takeoffDate,
		    a.trackingWebsite_id,
		    a.trackingMeta_id,
		    count(*) removedNum,
		    sum(case when a.first_send_notice_date >0 and a.takeoff_time>0  
		      then TIMESTAMPDIFF(MINUTE, a.first_send_notice_date, a.takeoff_time) else 0  end) as complianceTime
		  from matchedVideo as a, trackingWebsite as b
		  where a.trackingWebsite_id = b.id
		    and a.company_id = 10
		    and b.website_type = "hybrid"
		    and a.count_send_notice > 0
		    and a.matchedFile_id = 0
		    and hide_flag = 2
		    and a.first_send_notice_date < a.takeoff_time
		    and a.first_send_notice_date > 0
		    and a.created_at >= "%(min_report_date)s"
		    and date_format(a.takeoff_time, "%%Y-%%m-%%d") > "%(date_para_TitleBasedRemoveNum_min)s"
		    and date_format(a.takeoff_time, "%%Y-%%m-%%d") < "%(date_para_TitleBasedRemoveNum_max)s"
		  group by 1, 2, 3, 4
		  union all
		  select
		    date_format(a.created_at, "%%Y-%%m-%%d") as reportDate,
		    date_format(a.takeoff_time, "%%Y-%%m-%%d") as takeoffDate,
		    a.trackingWebsite_id,
		    a.trackingMeta_id,
		    count(*) removedNum,
		    sum(case when a.first_send_notice_date >0 and a.takeoff_time>0  
		      then TIMESTAMPDIFF(MINUTE, a.first_send_notice_date, a.takeoff_time) else 0  end) as complianceTime
		  from matchedVideo as a, trackingWebsite as b, matchedFileItem d
		  where a.trackingWebsite_id = b.id
		    and d.matchedFile_id =  a.matchedFile_id
		    and a.company_id = 10
		    and b.website_type = "hybrid"
		    and a.matchedFile_id > 0
		    and a.count_send_notice > 0
		    and hide_flag = 2
		    and a.first_send_notice_date < a.takeoff_time
		    and a.first_send_notice_date > 0
		    and a.created_at >= "%(min_report_date)s"
		    and date_format(a.takeoff_time, "%%Y-%%m-%%d") > "%(date_para_TitleBasedRemoveNum_min)s"
		    and date_format(a.takeoff_time, "%%Y-%%m-%%d") < "%(date_para_TitleBasedRemoveNum_max)s"
		  group by 1, 2, 3, 4) as a
		group by 1,2 ,3 ,4
		UNION ALL
		select
		  date_format(a.created_at, "%%Y-%%m-%%d") as reportDate,
		  date_format(a.takeoff_time, "%%Y-%%m-%%d") as takeoffDate,
		  a.trackingWebsite_id,
		  a.trackingMeta_id,
		  count(*) removedNum,
		  sum(case when a.first_send_notice_date >0 and a.takeoff_time>0  
		  	then TIMESTAMPDIFF(MINUTE, a.first_send_notice_date, a.takeoff_time) else 0  end) as complianceTime,
		  CURRENT_TIMESTAMP as ETLDate
		from matchedVideo as a, trackingWebsite as b, matchedFileItem d
		where a.trackingWebsite_id = b.id
		  and d.matchedFile_id =  a.matchedFile_id
		  and a.company_id = 10
		  and b.website_type = "cyberlocker"
		  and a.count_send_notice > 0
		  and hide_flag = 2
		  and a.first_send_notice_date < a.takeoff_time
		  and a.first_send_notice_date > 0
		  and a.created_at >= "%(min_report_date)s"
		  and date_format(a.takeoff_time, "%%Y-%%m-%%d") > "%(date_para_TitleBasedRemoveNum_min)s"
		  and date_format(a.takeoff_time, "%%Y-%%m-%%d") < "%(date_para_TitleBasedRemoveNum_max)s"
		group by 1, 2, 3, 4
	""" %date_para_TitleBasedRemoveNum_dict

	vtweb_tracker2_section = "vtweb_staging"
	try:
		vt_host, vt_user, vt_passwd, vt_port, vt_db = getConfMysqlInfo(vtweb_tracker2_section)
		vtweb_mysql = MySQLHelper(host=vt_host, user=vt_user,passwd=vt_passwd, port = vt_port, db_name = vt_db)
		vtweb_mysql.queryCMD("set time_zone = '-7:00'")
		result = vtweb_mysql.queryCMD(vt_TitleBasedRemoveNum_SQL)
	except Exception, e:
		logger.debug(": extract data from vt for TitleBasedRemoveNum, %s" %e)
		sendToMe(subject = "TitleBasedRemove ERROR", body = re.sub(r'\'|"|!', "", str(e)))
		sys.exit(0)
	finally:
		vtweb_mysql.closeCur()
		vtweb_mysql.closeConn()
		logger.info(": extract data from tracker2 end")

	return result

def loadDataToTitleBasedRemoveNum(vtweb_data):
	logger.info(":load data to TitleBasedRemoveNum  start")
	target_server_section = "target_server_staging"
	target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
	try:
		target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, 
			db_name = target_db, port = target_port, charset = 'utf8')
		insert_SQL = """
			INSERT INTO TitleBasedRemoveNumTmp(reportDate, takeoffDate, trackingWebsite_id, 
				trackingMeta_id, removedNum, complianceTime, ETLDate) 
			VALUES(%s, %s, %s, %s, %s, %s, %s)
		"""
		target_mysql.queryNoData("delete from TitleBasedRemoveNumTmp")
		commitInTurn(commit_num = 50000, data = vtweb_data, executeFun = target_mysql.executeManyCMD, \
	        commitFun = target_mysql.commit, executeSQL = insert_SQL)

		#target_mysql.executeManyCMD(insert_SQL, result)
		#target_mysql.commit()
	except Exception, e:
		logger.debug(": load data to TitleBasedRemoveNum, %s" %e)
		sendToMe(subject = "TitleBasedRemove ERROR", body =  e)
		sys.exit(0)
	finally:
		target_mysql.closeCur()
		target_mysql.closeConn()
		logger.info(":load data to TitleBasedRemoveNum  end")
#################################################################################################################################

def loadDataToTitleBasedRemoveNum1():
	logger.info(":extract data from TitleBasedRemoveNum start")
	target_server_section = "target_server_staging"
	target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
	try:
		target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, db_name = target_db, port = target_port, charset = 'utf8')
		aggregate_SQL = """
			select
			  a.reportDate,
			  a.takeoffDate,
			  a.trackingWebsite_id,
			  ifnull(c.displayName, a.websiteName) as websiteName,
			  a.websiteType,
			  ifnull(b.mapTitle, a.title) as title,
			  sum(removedNum) as removedNum,
			  sum(complianceTime) as complianceTime,
			  current_timestamp as ETLDate
			from
			  (select
			  a.reportDate,
			  a.takeoffDate,
			  a.trackingWebsite_id,
			  c.websiteName,
			  c.websiteType,
			  b.title,
			  sum(removedNum) as removedNum,
			  sum(complianceTime) as complianceTime
			  from TitleBasedRemoveNumTmp as a, TitleBasedMeta as b, TitleBasedTrackingWebsite as c
			  where a.trackingWebsite_id = c.trackingWebsite_id
			    and a.trackingMeta_id = b.trackingMeta_id
			  group by 1, 2, 3, 4, 5, 6) as a
			left join MetaTitleMapTitle as b
			on a.title = b.metaTitle
			left join SiteMap as c
			on a.trackingWebsite_id = c.trackingWebsite_id
			group by 1, 2, 3, 4, 5, 6
		"""
		target_mysql.queryNoData("delete from TitleBasedRemoveNum")
		aggregate_result = target_mysql.queryCMD(aggregate_SQL)
		
		insertUpdate_SQL = """
			INSERT INTO TitleBasedRemoveNum 
				(reportDate, takeoffDate, trackingWebsite_id, websiteName, 
					websiteType, title, removedNum, complianceTime,  ETLDate) 
	  		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
	  		ON DUPLICATE KEY UPDATE 
	  			removedNum = VALUES(removedNum), complianceTime = VALUES(complianceTime), ETLDate = VALUES(ETLDate)
		"""
		target_mysql.queryNoData("delete from TitleBasedRemoveNum")
		commitInTurn(commit_num = 50000, data = aggregate_result, executeFun = target_mysql.insertUpdateCMD, \
	        commitFun = target_mysql.commit, executeSQL = insertUpdate_SQL)
		#target_mysql.insertUpdateCMD(insertUpdate_SQL, aggregate_result)
		#target_mysql.commit()
	except Exception, e:
		logger.debug(" load data to TitleBasedRemoveNum, %s" %e)
		sendToMe(subject = "TitleBasedRemove ERROR", body =  re.sub(r'\'|"|!', "", str(e)))
		sys.exit(0)
	finally:
		target_mysql.closeCur()
		target_mysql.closeConn()
		logger.info(" load data to TitleBasedRemoveNum1  end")

def main():
	sendToMe(subject = "TitleBasedRemove Start", body = "extract data from tracker2 start")
	vtweb_data = getDataFromVT()
	loadDataToTitleBasedRemoveNum(vtweb_data)
	loadDataToTitleBasedRemoveNum1()
	sendToMe(subject = "TitleBasedRemove End", body =  "load data to TitleBasedRemoveNum1  end")

if __name__ == "__main__":
	main()

#################################################################################################################################
#sed  "/\/home\/vobile\/cwj\/ViacomProject\/dashboard\/job/\/Job\/FOX\/Dashboard\/TitleBased/g"
