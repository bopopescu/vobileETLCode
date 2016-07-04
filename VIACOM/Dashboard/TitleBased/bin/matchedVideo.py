#!/usr/bin/env python
#coding:utf8
#Date: 2016-04-22
#Author: 
#Desc: extract data from vtweb tracker2.matchedVideo
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

logger = logging.getLogger("matchedVideo")
logger.setLevel(logging.DEBUG)
log_file = '/Job/VIACOM/Dashboard/TitleBased/log/matchedVideo.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

def judgeFileExist(cfg_file, job_name):
    if not os.path.exists(cfg_file):
        logging.debug(": config file not exists") 
        sendToMe(subject = job_name +" ERROR", body = "config file not exists")
        sys.exit(0)

def getMatchedVideo(min_reportDate, min_updateDate):
    logger.info(" extract data from tracker2.matchedVideo start")
    date_dict = {"min_reportDate": min_reportDate, "min_updateDate": min_updateDate}
    try:	
        get_data_sql = """
            select id, company_id, trackingMeta_id, trackingWebsite_id, view_count, 
                count_send_notice, first_send_notice_date, takeoff_time, hide_flag,
                clip_duration, matchedFile_id, meta_title, created_at, updated_at
            from tracker2.matchedVideo
            where company_id = 14
              and created_at >= "2015-03-01"
              and (created_at > "%(min_reportDate)s" 
                or (created_at <= "%(min_reportDate)s" and updated_at > "%(min_updateDate)s"))
		""" %date_dict
        vtweb_tracker2_section = "vtweb"        
        vt_host, vt_user, vt_passwd, vt_port, vt_db = getConfMysqlInfo(vtweb_tracker2_section)
        vtweb_mysql = MySQLHelper(host=vt_host, user=vt_user,passwd=vt_passwd, port = vt_port, db_name = vt_db)
        data = vtweb_mysql.queryCMD(get_data_sql)
    except Exception, e:
        logger.debug("extract data from tracker2.matchedVideo, %s" %e)
        sendToMe(subject = "matchedVideo ERROR", body = re.sub(r'\'|"|!', "", str(e)))
        sys.exit(0)
    finally:
        vtweb_mysql.closeConn()
        vtweb_mysql.closeCur()
    logger.info(" extract data from tracker2.matchedVideo end")

    return data


def dataToTarget(data, db):
    logger.info("load data target matchedVideo start")
    try:    
        target_server_section = "target_server_staging"
        target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
        target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, \
                                   db_name = db, port = target_port, charset = 'utf8')
        insert_sql = """
            insert into matchedVideo
              (id, company_id, trackingMeta_id, trackingWebsite_id, view_count, 
                count_send_notice, first_send_notice_date, takeoff_time, hide_flag,
                clip_duration, matchedFile_id, meta_title, created_at, updated_at)
             values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
             ON DUPLICATE KEY UPDATE    
                trackingMeta_id = values(trackingMeta_id), trackingWebsite_id = values(trackingWebsite_id),
                view_count = values(view_count), count_send_notice = values(count_send_notice),
                first_send_notice_date = values(first_send_notice_date), takeoff_time = values(takeoff_time),
                hide_flag = values(hide_flag), clip_duration = values(clip_duration),
                matchedFile_id = values(matchedFile_id), meta_title = values(meta_title),
                created_at = values(created_at), updated_at = values(updated_at)
        """
        commitInTurn(commit_num = 50000, data = data, executeFun = target_mysql.executeManyCMD, \
          commitFun = target_mysql.commit, executeSQL  =insert_sql)
        #target_mysql.executeManyCMD(insert_sql, data)
        #target_mysql.commit()   
    except Exception, e:
        logger.debug("extral data from tracker2.matchedVideo, %s" %e)
        sendToMe(subject = "matchedVideo ERROR", body = re.sub(r'\'|"|!', "", str(e)))
        sys.exit(0)
    finally:
        target_mysql.closeCur()
        target_mysql.closeConn()
    logger.info("load data target matchedVideo end")

def main():
    sendToMe(subject = "matchedVideo start", body = "matchedVideo start")
    cfg_file = "/Job/VIACOM/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
    job_name = "matchedVideo"
    judgeFileExist(cfg_file, job_name)

    target_db = "tracker2"
    min_reportDate = getMinDatePara("matchedVideo", "created_at", db = target_db)
    min_updateDate = getMinDatePara("matchedVideo", "updated_at", db = target_db)

    if not min_reportDate:
        min_reportDate = "2015-03-01"
    if not min_updateDate:
        min_updateDate = "0000-00-00 00:00:00"

    data = getMatchedVideo(min_reportDate, min_updateDate)
    dataToTarget(data, target_db)
    sendToMe(subject = "matchedVideo end", body = "matchedVideo end")
    
if __name__ == "__main__":
    main()

