#!/usr/bin/env python
#coding:utf8
#Date: 2016-03-14
#Author: cwj
#Desc: data from vtweb to TitleBasedTrackingWebsite
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
import re

logger = logging.getLogger("titleBased_trackingWebsite")
logger.setLevel(logging.DEBUG)
log_file = '/Job/FOX/Dashboard/TitleBased/log/titleBased_trackingWebsite.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

cfg_file = "/Job/FOX/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
if not os.path.exists(cfg_file):
  logging.debug(": config file not exists; file_name %s" %cfg_file) 
  sendToMe(subject = "titleBased_trackingWebsite  ERROR", body ="config file not exists")
  sys.exit(0)

def getDataFromVT():
  logger.info(": extract data from tracker2 start")
  
  # extract dat from vtweb
  vt_TitleBasedTrackingWebsite_SQL = """
  select
    a.trackingWebsite_id,
    b.website_type as websiteType,
    a.display_name as websiteName,
    b.website_domain as websiteDomain,
    b.country_id,
    CURRENT_TIMESTAMP as ETLDate
  from trackingWebsiteExtraInfo as a, mddb.trackingWebsite as b
  where a.trackingWebsite_id = b.id
  """
  #vtweb_tracker2_section = "vtweb_tracker2"
  vtweb_tracker2_section = "vtweb_staging"
  try:
    vt_host, vt_user, vt_passwd, vt_port, vt_db = getConfMysqlInfo(vtweb_tracker2_section)
    vtweb_mysql = MySQLHelper(host=vt_host, user=vt_user,passwd=vt_passwd, port = vt_port, db_name = vt_db)
    result = vtweb_mysql.queryCMD(vt_TitleBasedTrackingWebsite_SQL)
  except Exception, e:
    logger.debug(": extract data from vt for dimension trackingWebsite, %s" %e)
    sendToMe(subject = "TitleBasedTrackingWebsite ERROR", body = re.sub(r'\'|"|!', "", str(e)))
    sys.exit(0)
  finally:
    vtweb_mysql.closeCur()
    vtweb_mysql.closeConn()
    logger.info(": extract data from tracker2 start")

  return result

def loadDataTo123(vtweb_data):
  logger.info(":load data to TitleBasedTrackingWebsite  start")
  target_server_section = "target_server_staging"
  try:
    target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
    target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, db_name = target_db, port = target_port, charset = 'utf8')
    insertUpdate_SQL = """
      INSERT INTO TitleBasedTrackingWebsite
      (trackingWebsite_id, websiteType, websiteName, websiteDomain, country_id, ETLDate) 
      VALUES(%s, %s, %s, %s, %s, %s) 
      on duplicate  key update websiteDomain = values(websiteDomain),
      websiteType = values(websiteType), ETLDate = values(ETLDate), websiteName = values(websiteName), country_id = values(country_id)
    """
    result1 = [(item[0], item[1], item[2], re.sub(r"_ugc|_hybrid|_Hybrid|_cyberlocker|_UGC|_Cy|_cy", '', item[3]), item[4], item[5]) for item in vtweb_data]
    commitInTurn(commit_num = 50000, data = result1, executeFun = target_mysql.insertUpdateCMD, \
        commitFun = target_mysql.commit, executeSQL = insertUpdate_SQL)
  except Exception, e:
    logger.debug(": load data to TitleBasedTrackingWebsite, %s" %e)
    sendToMe(subject = "TitleBasedTrackingWebsite ERROR", body = re.sub(r'\'|"|!', "", str(e)))
    sys.exit(0)
  finally:
    target_mysql.closeCur()
    target_mysql.closeConn()
    logger.info(":load data to TitleBasedTrackingWebsite  end")

def main():
  sendToMe(subject = "titleBased_trackingWebsite start", body ="extract data from tracker2 start")
  vtweb_data = getDataFromVT()
  loadDataTo123(vtweb_data)
  sendToMe(subject = "titleBased_trackingWebsite end", body = "load data to TitleBasedTrackingWebsite  end")

if __name__ == "__main__":
  main()
#################################################################################################################################

