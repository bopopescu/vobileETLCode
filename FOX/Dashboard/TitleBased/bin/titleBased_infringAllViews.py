#!/usr/bin/env python
# coding:utf8
# Date: 2016-03-14
# Author: cwj
# Desc: update data TitleBased1 views
#


from mysqlHelp import MySQLHelper
import ConfigParser
from parseConfig import CfgParser
import sys
import os
import logging
import time
import datetime
from titleBased import getConfMysqlInfo, getMinDatePara, commitInTurn
from sendMail import sendToMe
import re
from titleBased_matchedVideoViewCountCompletionAll import judgeFileExist

logger = logging.getLogger("titleBased_infringAllViews")
logger.setLevel(logging.DEBUG)
log_file = '/Job/FOX/Dashboard/TitleBased/log/titleBased_infringAllViews.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5 * 1024 * 1024, backupCount=10,
                                                   mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

def updateViews(start_date, end_date):
    logger.info(" aggregate data from matchedVideoViewCountCompletion  start")
    target_server_section = "target_server_staging"
    target_host, target_user, target_passwd, target_port, target_db = getConfMysqlInfo(target_server_section)

    date_dict = {"start_date": start_date, "end_date": end_date}
    try:
        get_data_sql = """
			select
			  a.reportDate,
			  a.trackingWebsite_id,
			  ifnull(c.displayName, a.websiteName) as websiteName,
	       	  	  a.websiteType,
			  ifnull(b.mapTitle, a.title) title,
			  sum(a.infringingViews) as infringingViews,
			  sum(a.reportedViews) as reportedViews,
			  current_timestamp as ETLDate
			from
			 (select
			    a.report_at as reportDate,
			    a.trackingWebsite_id,
			    c.websiteName,
			    c.websiteType,
			    b.title,
			    sum(if(a.hide_flag = 2, a.view_count, 0)) as infringingViews,
			    sum(a.view_count) as reportedViews
			  from matchedVideoViewCountCompletionAll as a, TitleBasedMeta as b, TitleBasedTrackingWebsite as c
			  where a.trackingWebsite_id = c.trackingWebsite_id
			    and a.trackingMeta_id = b.trackingMeta_id
			    and c.websiteType = 'ugc'
			    and a.report_at  > "%(start_date)s"
			    and a.report_at <= "%(end_date)s"
			  group by 1, 2, 3, 4, 5) as a
			left join MetaTitleMapTitle as b on a.title = b.metaTitle
			left join SiteMap as c on a.trackingWebsite_id = c.trackingWebsite_id
			group by 1, 2, 3, 4, 5
		""" % date_dict

        target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, \
                                   db_name=target_db, port=target_port, charset='utf8')
        data = target_mysql.queryCMD(get_data_sql)

        insert_sql = """insert into TitleBased1
        		(reportDate, trackingWebsite_id, websiteName, websiteType, title, infringingViews, reportedViews, ETLDate)
        		values(%s, %s, %s, %s, %s, %s, %s, %s)
        		ON DUPLICATE KEY UPDATE
        			infringingViews = values(infringingViews), reportedViews = values(reportedViews), 
				ETLDate = values(ETLDate), websiteName = VALUES(websiteName)
		"""
        commitInTurn(commit_num=100000, data=data, executeFun=target_mysql.executeManyCMD, \
                 commitFun=target_mysql.commit, executeSQL=insert_sql)
    except Exception, e:
        logger.debug("aggregate data to TitleBased1 ERROR , %s" % e)
        sendToMe(subject="titleBased_infringAllViews ERROR", body=re.sub(r'\'|"|!', "", str(e)))
        sys.exit(0)
    finally:
        target_mysql.closeCur()
        target_mysql.closeConn()
    logger.info("aggregate data to TitleBased1 end")


def main():
    sendToMe(subject="titleBased_infringAllViews start", body="titleBased_infringAllViews start")

    cfg_file = "/Job/FOX/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
    judgeFileExist(cfg_file=cfg_file, job_name="titleBased_infringAllViews")

    #end_date = getMinDatePara("matchedVideoViewCountCompletionAll", "report_at")
    #end_date = "2016-05-02"
    #if not end_date:
    #    sendToMe(subject="matchedVideoViewCountCompletionAll has no data", body="matchedVideoViewCountCompletionAll has no data")
    #start_date = time.strftime("%Y-%m-%d", \
    #               time.localtime(time.mktime(time.strptime(str(end_date), "%Y-%m-%d")) - 1 * 24 * 60 * 60))
    #start_date  ="2015-02-28"
    
    beg = datetime.date(2015,3,1)
    end = datetime.date.today() - datetime.timedelta(days=2)
    for i in xrange((end- beg).days+1):
	start_date = str(beg + datetime.timedelta(days=i))
	end_date = str(beg + datetime.timedelta(days=i+1))
        updateViews(start_date, end_date)

    sendToMe(subject="titleBased_infringAllViews end", body="titleBased_infringAllViews end")

if __name__ == "__main__":
    main()

