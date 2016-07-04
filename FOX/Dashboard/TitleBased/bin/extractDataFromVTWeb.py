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
log_file = '/Job/FOX/Dashboard/TitleBased/log/extractDataFromVTWeb.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

def judgeFileExist(cfg_file, job_name):
    if not os.path.exists(cfg_file):
        logging.debug(": config file not exists") 
        sendToMe(subject = job_name +" ERROR", body = "config file not exists")
        sys.exit(0)

def get_matchedVideo(min_reportDate, min_updateDate):
    logger.info(" extract data from tracker2.matchedVideo start")
    date_dict = {"min_reportDate": min_reportDate, "min_updateDate": min_updateDate}
    try:    
        get_data_sql = """
            select id, company_id, trackingMeta_id, trackingWebsite_id, view_count, 
                count_send_notice, first_send_notice_date, takeoff_time, hide_flag,
                clip_duration, matchedFile_id, meta_title, created_at, updated_at
            from tracker2.matchedVideo
            where company_id = 10
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

    target_db = "FOX_DASHBOARD"
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

def dataTo_matchedVideoSequence():
    logger.info("load data target matchedVideoSequence start")
    sendToMe(subject = "matchedVideoSequence start", body = "matchedVideoSequence start")

    #delete from matchedVideoSequence
    target_server_section = "target_server_staging"
    target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
    target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, \
                               db_name = "FOX_DASHBOARD", port = target_port, charset = 'utf8')
    target_mysql.queryNoData("delete from matchedVideoSequence")
    target_mysql.commit()
    target_mysql.closeConn()
    target_mysql.closeCur()

    # get arch matchedVideo id and insert into matchedVideoSequence
    get_data_arch_sql = """
        select id, company_id from archTracker2.matchedVideo where company_id = 10 and created_at >= "2015-03-01"
    """
    arch_data = getData(get_data_arch_sql, section = "vtweb-arch")
    insert_arch_sql = """insert into matchedVideoSequence (id, company_id)  values(%s, %s)"""
    dataToTarget(arch_data, "FOX_DASHBOARD", insert_arch_sql)

    # get matchedVideo id and insert into matchedVideoSequence
    get_data_sql = """
        select id, company_id from tracker2.matchedVideo 
        where company_id = 10 and created_at >= "2015-03-01"
    """
    data = getData(get_data_sql, section = "vtweb")
    insert_sql = """
        insert into matchedVideoSequence (id, company_id) values(%s, %s)
        ON DUPLICATE KEY UPDATE id = values(id), company_id = values(company_id)
    """
    dataToTarget(data, "FOX_DASHBOARD", insert_sql)

    sendToMe(subject = "matchedVideoSequence end", body = "matchedVideoSequence end")
    logger.info("load data target matchedVideoSequence end")

def dataTo_matchedVideo():
    logger.info("load data target matchedVideo start")
    sendToMe(subject = "matchedVideo start", body = "matchedVideo start")

    try:
        target_server_section = "target_server_staging"
        target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
        target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, \
                                   db_name = "FOX_DASHBOARD", port = target_port, charset = 'utf8')
        sql = """
            insert into FOX_DASHBOARD.matchedVideo 
                (id, company_id, trackingMeta_id, trackingWebsite_id, view_count, 
                count_send_notice, first_send_notice_date, takeoff_time, hide_flag,
                clip_duration, matchedFile_id, meta_title, created_at, updated_at)
            select 
                a.id, a.company_id, trackingMeta_id, trackingWebsite_id, view_count, 
                count_send_notice, first_send_notice_date, takeoff_time, hide_flag,
                clip_duration, matchedFile_id, meta_title, created_at, updated_at
             from FOX_DASHBOARD.matchedVideoTmp as a, FOX_DASHBOARD.matchedVideoSequence as b
             where a.id = b.id and a.company_id = b.company_id
        """
        target_mysql.queryNoData("delete from FOX_DASHBOARD.matchedVideo")
        target_mysql.queryNoData(sql)
        target_mysql.commit()
    except Exception, e:
        logger.debug("extract data from 192.168.111.235 tracker2 error, %s" %e)
        sendToMe(subject = "extract data from 192.168.111.235 tracker2", body = re.sub(r'\'|"|!', "", str(e)))
        sys.exit(0)
    finally:
        target_mysql.closeConn()
        target_mysql.closeCur()   

    sendToMe(subject = "matchedVideo end", body = "matchedVideo end")
    logger.info("load data target matchedVideo end")


def dataTo_metaExtraInfo():
    logger.info("load data target metaExtraInfo start")
    sendToMe(subject = "metaExtraInfo start", body = "metaExtraInfo start")
    get_data_sql = """
        select meta_id, company_id, display_name, priority_type, created_at, updated_at
        from tracker2.metaExtraInfo
        where company_id = 10
    """
    insert_sql = """
        insert into metaExtraInfo
          (meta_id, company_id, display_name, priority_type, created_at, updated_at)
         values(%s, %s, %s, %s, %s, %s)
         ON DUPLICATE KEY UPDATE
           display_name = values(display_name), priority_type = values(priority_type),
           created_at = values(created_at), updated_at = values(updated_at)
    """
    target_db = "FOX_DASHBOARD"
    data = getData(get_data_sql)
    dataToTarget(data, target_db, insert_sql)
    sendToMe(subject = "metaExtraInfo end", body = "metaExtraInfo end")
    logger.info("load data target metaExtraInfo  end")

def dataTo_trackingWebsiteExtraInfo():
    logger.info("load data target trackingWebsiteExtraInfo start")
    sendToMe(subject = "trackingWebsiteExtraInfo start", body = "trackingWebsiteExtraInfo start")
    get_data_sql = """
        select trackingWebsite_id, display_name, created_at, updated_at
        from tracker2.trackingWebsiteExtraInfo
    """
    insert_sql = """
        insert into trackingWebsiteExtraInfo
          (trackingWebsite_id, display_name, created_at, updated_at)
         values(%s, %s, %s, %s)
         ON DUPLICATE KEY UPDATE
           display_name = values(display_name), created_at = values(created_at), updated_at = values(updated_at)
    """
    target_db = "FOX_DASHBOARD"
    data = getData(get_data_sql)
    dataToTarget(data, target_db, insert_sql)
    sendToMe(subject = "trackingWebsiteExtraInfo end", body = "trackingWebsiteExtraInfo end")
    logger.info("load data target trackingWebsiteExtraInfo  end")

def dataTo_matchedFileItem():
    logger.info("load data target matchedFileItem start")
    sendToMe(subject = "matchedFileItem start", body = "matchedFileItem start")
    get_data_sql = """
        select b.*
        from tracker2.matchedVideo as a, tracker2.matchedFileItem as b, mddb.trackingWebsite as c
        where a.matchedFile_id = b.matchedFile_id
          and (c.website_type = "cyberlocker" or c.website_type = "hybrid")
          and a.trackingWebsite_id = c.id
          and a.company_id = 10
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
    target_db = "FOX_DASHBOARD"
    data0 = getData(get_data_sql)
    data = [(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7] if item[7] else "0000-00-00 00:00:00", item[8]) for item in data0]
    dataToTarget(data, target_db, insert_sql)
    sendToMe(subject = "matchedFileItem end", body = "matchedFileItem end")
    logger.info("load data target matchedFileItem  end")

def dataTo_meta():
    logger.info("load data target meta start")
    sendToMe(subject = "meta start", body = "meta start")
    get_data_sql = """
        select
          id, company_id, meta_type, meta_title, created_at, updated_at
        from mddb.meta
        where company_id = 10
    """
                
    insert_sql = """
        insert into meta
          (id, company_id, meta_type, meta_title, created_at, updated_at)
         values(%s, %s, %s, %s, %s, %s)
         ON DUPLICATE KEY UPDATE
           company_id = values(company_id), meta_type = values(meta_type), meta_title = values(meta_title),
           created_at = values(created_at), updated_at = values(updated_at)
    """
    target_db = "FOX_DASHBOARD"
    data = getData(get_data_sql)
    dataToTarget(data, target_db, insert_sql)
    sendToMe(subject = "meta end", body = "meta end")
    logger.info("load data target meta  end")

def dataTo_country():
    logger.info("load data target country start")
    sendToMe(subject = "country start", body = "country start")
    get_data_sql = """
        select 
            id, region, country_code, country_name, language_id, 
            national_flag, longitude, latitude, created_at, updated_at
        from mddb.country;
    """
                
    insert_sql = """
        insert into country
          (id, region, country_code, country_name, language_id, 
            national_flag, longitude, latitude, created_at, updated_at)
         values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
         ON DUPLICATE KEY UPDATE
           region = values(region), country_code = values(country_code), country_name = values(country_name),
           language_id = values(language_id), national_flag = values(national_flag), longitude = values(longitude),
           latitude = values(latitude), created_at = values(created_at), updated_at = values(updated_at)
    """
    target_db = "FOX_DASHBOARD"
    data = getData(get_data_sql)
    dataToTarget(data, target_db, insert_sql)
    sendToMe(subject = "country end", body = "country end")
    logger.info("load data target country  end")

def dataTo_trackingWebsite():
    logger.info("load data target trackingWebsite start")
    sendToMe(subject = "trackingWebsite start", body = "trackingWebsite start")
    get_data_sql = """
        select  
            id, website_name, website_domain, homepage, 
            website_type, country_id, display_name, created_at, updated_at
        from mddb.trackingWebsite
    """
                
    insert_sql = """
        insert into trackingWebsite
          (id, website_name, website_domain, homepage, website_type, country_id, display_name, created_at, updated_at)
         values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
         ON DUPLICATE KEY UPDATE
           website_name = values(website_name), website_domain = values(website_domain), homepage = values(homepage),
           website_type = values(website_type), country_id = values(country_id), display_name = values(display_name),
           created_at = values(created_at), updated_at = values(updated_at)
    """
    target_db = "FOX_DASHBOARD"
    data = getData(get_data_sql)
    dataToTarget(data, target_db, insert_sql)
    sendToMe(subject = "trackingWebsite end", body = "trackingWebsite end")
    logger.info("load data target trackingWebsite  end")


def main():
    cfg_file = "/Job/FOX/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
    job_name = "extractDataFromVTWeb"
    judgeFileExist(cfg_file, job_name)

    dataTo_matchedVideoTmp()
    dataTo_matchedVideoSequence()
    dataTo_matchedVideo()
    dataTo_metaExtraInfo()
    dataTo_trackingWebsiteExtraInfo()
    dataTo_matchedFileItem()
    dataTo_meta()
    dataTo_country()
    dataTo_trackingWebsite()
    
if __name__ == "__main__":
    main()

