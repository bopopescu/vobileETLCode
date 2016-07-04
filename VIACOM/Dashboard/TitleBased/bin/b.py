#!/usr/bin/env python
#coding:utf8
#Date: 2016-04-22
#Author: 
#Desc: extract data from vtweb tracker2.metaExtraInfo
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
from matchedVideo import judgeFileExist

logger = logging.getLogger("metaExtraInfo")
logger.setLevel(logging.DEBUG)
log_file = '/Job/VIACOM/Dashboard/TitleBased/log/metaExtraInfo.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

def getData(sql):
    logger.info(" extract data from tracker2.metaExtraInfo start")
    try:
        vtweb_tracker2_section = "vtweb"        
        vt_host, vt_user, vt_passwd, vt_port, vt_db = getConfMysqlInfo(vtweb_tracker2_section)
        vtweb_mysql = MySQLHelper(host=vt_host, user=vt_user,passwd=vt_passwd, port = vt_port, db_name = vt_db)
        data = vtweb_mysql.queryCMD(sql)
    except Exception, e:
        logger.debug("extract data from tracker2.metaExtraInfo, %s" %e)
        sendToMe(subject = "metaExtraInfo ERROR", body = re.sub(r'\'|"|!', "", str(e)))
        sys.exit(0)
    finally:
        vtweb_mysql.closeConn()
        vtweb_mysql.closeCur()
    logger.info(" extract data from tracker2.metaExtraInfo end")

    return data


def dataToTarget(data, db, insert_sql):
    logger.info("load data target metaExtraInfo start")
    try:    
        target_server_section = "target_server_staging"
        target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
        target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, \
                                   db_name = db, port = target_port, charset = 'utf8')

        commitInTurn(commit_num = 50000, data = data, executeFun = target_mysql.executeManyCMD, \
          commitFun = target_mysql.commit, executeSQL  =insert_sql)
        #target_mysql.executeManyCMD(insert_sql, data)
        #target_mysql.commit()   
    except Exception, e:
        logger.debug("extral data from tracker2.metaExtraInfo, %s" %e)
        sendToMe(subject = "metaExtraInfo ERROR", body = re.sub(r'\'|"|!', "", str(e)))
        sys.exit(0)
    finally:
        target_mysql.closeCur()
        target_mysql.closeConn()
    logger.info("load data target metaExtraInfo end")


def dataToMetaExtraInfo():
    sendToMe(subject = "metaExtraInfo start", body = "metaExtraInfo start")
    get_data_sql = """
        select meta_id, company_id, display_name, priority_type, created_at, updated_at
        from tracker2.metaExtraInfo
        where company_id = 14
    """
    insert_sql = """
        insert into metaExtraInfo
          (meta_id, company_id, display_name, priority_type, created_at, updated_at)
         values(%s, %s, %s, %s, %s, %s)
         ON DUPLICATE KEY UPDATE
           display_name = values(display_name), priority_type = values(priority_type),
           created_at = values(created_at), updated_at = values(updated_at)
    """
    target_db = "tracker2"
    data = getData(get_data_sql)
    dataToTarget(data, target_db, insert_sql)
    sendToMe(subject = "metaExtraInfo end", body = "metaExtraInfo end")


def main():
    
    cfg_file = "/Job/VIACOM/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
    job_name = "metaExtraInfo"
    judgeFileExist(cfg_file, job_name)
    dataToMetaExtraInfo()
    
    
    
if __name__ == "__main__":
    main()

