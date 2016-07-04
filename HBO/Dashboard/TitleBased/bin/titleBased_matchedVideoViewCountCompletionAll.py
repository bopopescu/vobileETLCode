#!/usr/bin/env python
#coding:utf8
#Date: 2016-03-14
#Author: cwj
#Desc: extral data from insight2.1
#


from mysqlHelp import MySQLHelper
import re
import ConfigParser
from parseConfig import CfgParser
import sys
import os
import logging
import time
from titleBased import getConfMysqlInfo, getMinDatePara, commitInTurn
from sendMail import sendToMe

logger = logging.getLogger("titleBased_matchedVideoViewCountCompletionAll")
logger.setLevel(logging.DEBUG)
log_file = '/Job/HBO/Dashboard/TitleBased/log/titleBased_matchedVideoViewCountCompletionAll.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

def judgeFileExist(cfg_file, job_name):
    if not os.path.exists(cfg_file):
        logging.debug(": config file not exists")
        sendToMe(subject=job_name + " ERROR", body="config file not exists")
        sys.exit(0)

def getInsightViews(start_date, end_date):
    logger.info(" extral data from Dashboard.matchedVideoViewCountCompletionAll start")
    date_dict  ={"start_date": start_date, "end_date": end_date}
    target_server_section = "target_server_staging"
    insight_server_section = "insight"
    target_host, target_user, target_passwd, target_port, target_db = getConfMysqlInfo(target_server_section)
    insight_host, insight_user, insight_passwd, insight_port, insight_db = getConfMysqlInfo(insight_server_section)
    try:
        get_data_sql = """
            select matchedVideo_id, trackingWebsite_id, trackingMeta_id,
                company_id, report_at, view_count, hide_flag, current_timestamp as ETLDate
            from matchedVideoViewCountCompletionAll
            where report_at  > "%(start_date)s" and report_at <= "%(end_date)s"
        """ %date_dict
        insight_mysql = MySQLHelper(host=insight_host, user=insight_user,passwd=insight_passwd, port = insight_port, db_name = insight_db)
        data = insight_mysql.queryCMD(get_data_sql)

        target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, \
            db_name = target_db, port = target_port, charset = 'utf8')
        insert_sql = """
            insert into matchedVideoViewCountCompletionAll
                (matchedVideo_id, trackingWebsite_id, trackingMeta_id, company_id, report_at, view_count, hide_flag, ETLDate)
             values(%s, %s, %s, %s, %s, %s, %s, %s)
        """
        commitInTurn(commit_num=100000, data=data, executeFun=target_mysql.executeManyCMD, \
             commitFun=target_mysql.commit, executeSQL=insert_sql)
        #target_mysql.executeManyCMD(insert_sql, data)
        #target_mysql.commit()
    except Exception, e:
        logger.debug("extral data from Dashboard.matchedVideoViewCountCompletionAll, %s" %e)
        sendToMe(subject = "titleBased_matchedVideoViewCountCompletionAll ERROR", body = re.sub(r'\'|"|!', "", str(e)))
        sys.exit(0)
    finally:
        insight_mysql.closeConn()
        insight_mysql.closeCur()
        target_mysql.closeCur()
        target_mysql.closeConn()
    logger.info(" extral data from Dashboard.matchedVideoViewCountCompletionAll end")


def main():
    sendToMe(subject = "matchedVideoViewCountCompletionAll start", body = "matchedVideoViewCountCompletionAll start")
    cfg_file = "/Job/HBO/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
    job_name = "titleBased_matchedVideoViewCountCompletionAll"
    judgeFileExist(cfg_file, job_name)


    insight_start_date = getMinDatePara("matchedVideoViewCountCompletionAll", "report_at")
    if not insight_start_date:
        insight_start_date = "2015-02-28"

    while True:
        if str(insight_start_date) >= str(time.strftime('%Y-%m-%d',time.localtime(time.time() - 1 * 24 * 60 * 60))):
            break
        insight_end_date = time.strftime("%Y-%m-%d", time.localtime(time.mktime(time.strptime(str(insight_start_date), "%Y-%m-%d")) + 1 * 24 * 60 * 60))

        getInsightViews(start_date = insight_start_date, end_date = insight_end_date)
        tmp_date = getMinDatePara("matchedVideoViewCountCompletionAll", "report_at")
        if not tmp_date:
            tmp_date = "2015-02-28"
        if tmp_date == insight_start_date:
            sendToMe(subject = "matchedVideoViewCountCompletionAll Error", body = "insight2.1 matchedVideoViewCountCompletionAll no data")
            break
        else:
            insight_start_date = tmp_date

    sendToMe(subject = "matchedVideoViewCountCompletionAll end", body = "matchedVideoViewCountCompletionAll end")

if __name__ == "__main__":
    main()


