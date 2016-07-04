#!/usr/bin/env python
#coding:utf8
#Date: 2016-04-22
#Author: 
#Desc: extract data from vtweb tracker2 and mddb
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

logger = logging.getLogger("extractDataFromVTWeb")
logger.setLevel(logging.DEBUG)
log_file = '/Job/HBO/Dashboard/TitleBased/log/extractDataFromVTWeb.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)


def get_matchedVideo(min_reportDate, min_updateDate):
    logger.info(" extract data from tracker2.matchedVideo start")
    date_dict = {"min_reportDate": min_reportDate, "min_updateDate": min_updateDate}
    try:    
        get_data_sql = """
            select id, company_id, trackingMeta_id, trackingWebsite_id, view_count, 
                count_send_notice, first_send_notice_date, takeoff_time, hide_flag,
                clip_duration, matchedFile_id, meta_title, created_at, updated_at
            from archTracker2.matchedVideo
            where company_id = 34
              and created_at >= "2015-03-01"
              and (created_at > "%(min_reportDate)s" 
                or (created_at <= "%(min_reportDate)s" and updated_at > "%(min_updateDate)s"))
        """ %date_dict
        vtweb_tracker2_section = "vtweb-arch"        
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
    data1 = [(item[0], item[1], item[2], item[3], item[4], 
      item[5], item[6] if item[6] else "0000-00-00 00:00:00", 
      item[7] if item[7] else "0000-00-00 00:00:00", 
      item[8], item[9], item[10], item[11], 
      item[12] if item[12] else "0000-00-00 00:00:00", 
      item[13] if item[13] else "0000-00-00 00:00:00") for item in data]

    return data1

def getData(sql, section = "vtweb"):
    logger.info(" extract data from tracker2 start")
    try:
        vtweb_tracker2_section = section       
        vt_host, vt_user, vt_passwd, vt_port, vt_db = getConfMysqlInfo(vtweb_tracker2_section)
        vtweb_mysql = MySQLHelper(host=vt_host, user=vt_user,passwd=vt_passwd, port = vt_port, db_name = vt_db)
        data = vtweb_mysql.queryCMD(sql)
    except Exception, e:
        logger.debug("extract data from tracker2, %s" %e)
        sendToMe(subject = "extract data from tracker2 ERROR", body = re.sub(r'\'|"|!', "", str(e)))
        sys.exit(0)
    finally:
        vtweb_mysql.closeConn()
        vtweb_mysql.closeCur()
    logger.info(" extract data from tracker2 end")

    return data


def dataToTarget(data, db, insert_sql):
    logger.info("load data target start")
    try:    
        target_server_section = "target_server_staging"
        target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
        target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, \
                                   db_name = db, port = target_port, charset = 'utf8')

        commitInTurn(commit_num = 100000, data = data, executeFun = target_mysql.executeManyCMD, \
          commitFun = target_mysql.commit, executeSQL  =insert_sql)
        #target_mysql.executeManyCMD(insert_sql, data)
        #target_mysql.commit()   
    except Exception, e:
        logger.debug("extract data from tracker2, %s" %e)
        sendToMe(subject = "extract data from tracker2 ERROR", body = re.sub(r'\'|"|!', "", str(e)))
        sys.exit(0)
    finally:
        target_mysql.closeCur()
        target_mysql.closeConn()
    logger.info("load data target  end")

def dataTo_matchedVideoTmp():
    logger.info("load data target matchedVideoTmp start")
    sendToMe(subject = "matchedVideoTmp start", body = "matchedVideoTmp start")

    target_db = "HBO_DASHBOARD"
    min_reportDate = getMinDatePara("matchedVideoTmp", "created_at", db = target_db)
    min_updateDate = getMinDatePara("matchedVideoTmp", "updated_at", db = target_db)
    if not min_reportDate:
        min_reportDate = "2015-03-01"
    if not min_updateDate:
        min_updateDate = "0000-00-00 00:00:00"

    data = get_matchedVideo(min_reportDate, min_updateDate)
    insert_sql = """
        insert into matchedVideoTmp
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
    dataToTarget(data, target_db, insert_sql)

    sendToMe(subject = "matchedVideoTmp end", body = "matchedVideoTmp end")
    logger.info("load data target matchedVideoTmp end")



def dataTo_matchedFileItem():
    logger.info("load data target matchedFileItem start")
    sendToMe(subject = "matchedFileItem start", body = "matchedFileItem start")
    get_data_sql = """
        select b.*
        from tracker2.matchedVideo as a, tracker2.matchedFileItem as b, mddb.trackingWebsite as c
        where a.matchedFile_id = b.matchedFile_id
          and (c.website_type = "cyberlocker" or c.website_type = "hybrid")
          and a.trackingWebsite_id = c.id
          and a.company_id = 34
          and a.created_at >= "2015-03-01"
    """
                
    insert_sql = """
        insert into matchedFileItem
          (id, matchedFile_id, trackingWebsite_id, key_id, file_name, file_size, clip_url, takeoff_time, takeoff_type)
         values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
         ON DUPLICATE KEY UPDATE
           file_name = values(file_name), file_size = values(file_size), clip_url = values(clip_url),
           takeoff_time = values(takeoff_time), takeoff_type = values(takeoff_type)
    """
    target_db = "HBO_DASHBOARD"
    data0 = getData(get_data_sql)
    data = [(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7] if item[7] else "0000-00-00 00:00:00", item[8]) for item in data0]
    dataToTarget(data, target_db, insert_sql)
    sendToMe(subject = "matchedFileItem end", body = "matchedFileItem end")
    logger.info("load data target matchedFileItem  end")


def main():
    cfg_file = "/Job/HBO/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
    job_name = "addDateFromArchVT"
    judgeFileExist(cfg_file, job_name)

    dataTo_matchedVideoTmp()
    dataTo_matchedFileItem()
    
if __name__ == "__main__":
    main()

