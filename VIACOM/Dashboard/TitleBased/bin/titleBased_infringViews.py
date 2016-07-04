#!/usr/bin/env python
#coding:utf8
#Date: 2016-03-14
#Author: cwj
#Desc: update data TitleBased1 views
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

logger = logging.getLogger("titleBased_infringViews")
logger.setLevel(logging.DEBUG)
log_file = '/Job/VIACOM/Dashboard/TitleBased/log/titleBased_infringViews.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

cfg_file = "/Job/VIACOM/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
if not os.path.exists(cfg_file):
	logging.debug(": config file not exists") 
	sendToMe(subject = "titleBased_infringViews ERROR", body = "config file not exists")
	sys.exit(0)
#################################################################################################################################
target_server_section = "target_server_staging"
target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
#################################################################################################################################

def updateViews(start_date, end_date):
	logger.info(" aggregate data from matchedVideoViewCountCompletion  start")

	date_dict  ={"start_date": start_date, "end_date": end_date}
	try:	
		get_data_sql = """
			select
			  a.reportDate,
			  a.trackingWebsite_id,
			  a.websiteName,
	       	  a.websiteType,
			  ifnull(b.mapTitle, a.title) title,
			  sum(a.infringingViews) as infringingViews,
			  current_timestamp as ETLDate
			from 
			 (select 
			    a.report_at as reportDate,
			    a.trackingWebsite_id,
			    c.websiteName, 		  	
			    c.websiteType,
			    b.title,
			    sum(a.view_count) as infringingViews
			  from matchedVideoViewCountCompletion as a, TitleBasedMeta as b, TitleBasedTrackingWebsite as c
			  where a.trackingWebsite_id = c.trackingWebsite_id
			    and a.trackingMeta_id = b.trackingMeta_id
			    and c.websiteType = 'ugc'
			    and a.report_at  > "%(start_date)s" 
			    and a.report_at <= "%(end_date)s"
			  group by 1, 2, 3, 4, 5) as a left join MetaTitleMapTitle as b on a.title = b.metaTitle
			group by 1, 2, 3, 4, 5 
		""" %date_dict

		target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, \
			db_name = target_db, port = target_port, charset = 'utf8')
		data = target_mysql.queryCMD(get_data_sql)

	        insert_sql = """insert into TitleBased1 
        		(reportDate, trackingWebsite_id, websiteName, websiteType, title, infringingViews, ETLDate)
        		values(%s, %s, %s, %s, %s, %s, %s)
        		ON DUPLICATE KEY UPDATE 
        			infringingViews = values(infringingViews), ETLDate = values(ETLDate)
        		
		"""
		target_mysql.executeManyCMD(insert_sql, data)
		target_mysql.commit()
	except Exception, e:
		logger.debug("aggregate data to TitleBased1 ERROR , %s" %e)
		sendToMe(subject = "titleBased_infringViews ERROR", body = re.sub(r'\'|"|!', "", str(e)))
		sys.exit(0)
	finally:
		target_mysql.closeCur()
		target_mysql.closeConn()   
	logger.info("aggregate data to TitleBased1 end")

def main():
	sendToMe(subject = "titleBased_infringViews start", body = "titleBased_infringViews start")

	end_date = getMinDatePara("matchedVideoViewCountCompletion", "report_at")
	start_date = time.strftime("%Y-%m-%d", \
			time.localtime(time.mktime(time.strptime(str(end_date), "%Y-%m-%d")) - 1 * 24 * 60 * 60)) 
	start_date  ="2015-02-28"
	updateViews(start_date, end_date)

	sendToMe(subject = "titleBased_infringViews end", body = "titleBased_infringViews end")

if __name__ == "__main__":
	main()

