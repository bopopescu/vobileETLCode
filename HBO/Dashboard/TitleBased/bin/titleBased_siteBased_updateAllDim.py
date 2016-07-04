#!/usr/bin/env python
#coding:utf8
#Date: 2016-04-01
#Author: cwj
#Desc: update all dim: trackingWebsite country
#

from mysqlHelp import MySQLHelper
import logging
import time
from titleBased import getConfMysqlInfo, getMinDatePara
from sendMail import sendToMe

logger = logging.getLogger("titleBased_siteBased_updateAllDim")
logger.setLevel(logging.DEBUG)
#log_file = '/home/vobile/cwj/ViacomProject/dashboard/job/log/siteBased_updateAlexa.log'
log_file = '/Job/HBO/Dashboard/TitleBased/log/titleBased_siteBased_updateAllDim.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

def updateTrackingWebsite():
    sendToMe(subject = "update TrackingWebsite  start", body = "update TrackingWebsite table: SiteBased TitleBased1 TitleBasedRemoveNum1")
    logger.info("update TrackingWebsite start")
    update_SiteBased_SQL = """
      update SiteBased as a, TitleBasedTrackingWebsite as b
      set a.websiteName = b.websiteName, a.websiteDomain = b.websiteDomain, a.websiteType = b.websiteType
      where a.trackingWebsite_id = b.trackingWebsite_id
    """
    update_TitleBased1_SQL = """
      update TitleBased1 as a, TitleBasedTrackingWebsite as b
      set a.websiteName = b.websiteName, a.websiteType = b.websiteType
      where a.trackingWebsite_id = b.trackingWebsite_id
    """

    update_TitleBasedRemoveNum1_SQL = """
      update TitleBasedRemoveNum as a, TitleBasedTrackingWebsite as b
      set a.websiteName = b.websiteName, a.websiteType = b.websiteType
      where a.trackingWebsite_id = b.trackingWebsite_id
    """

    try:
        target_server_section = "target_server_staging"
        target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
        target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, db_name = target_db, port = target_port, charset = 'utf8')
        target_mysql.queryCMD(update_SiteBased_SQL)
        target_mysql.commit()

        target_mysql.queryCMD(update_TitleBased1_SQL)
        target_mysql.commit()

        target_mysql.queryCMD(update_TitleBasedRemoveNum1_SQL)
        target_mysql.commit()
    except Exception, e:
        sendToMe(subject = "update TrackingWebsite ERROR", body = e)
        logger.DEBUG("update TrackingWebsite data %s" %e)
    finally:
        target_mysql.closeCur()
        target_mysql.closeConn()
        sendToMe(subject = "update TrackingWebsite end", body = "update TrackingWebsite table: SiteBased TitleBased1 TitleBasedRemoveNum1")

    logger.info(" update TrackingWebsite end")

def updateCountry():
    sendToMe(subject = "update Country  start", body = "update Country table: SiteBased")
    logger.info("update Country for table SiteBased start")
    update_SiteBased_SQL = """
      update SiteBased as a, TitleBasedCountry as b
      set a.hostCountry = b.countryName
      where a.country_id = b.country_id
    """
    try:
        target_server_section = "target_server_staging"
        target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
        target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, db_name = target_db, port = target_port, charset = 'utf8')
        target_mysql.queryCMD(update_SiteBased_SQL)
        target_mysql.commit()
    except Exception, e:
        sendToMe(subject = "update TrackingWebsite ERROR", body = e)
        logger.DEBUG("update TrackingWebsite data %s" %e)
    finally:
        target_mysql.closeCur()
        target_mysql.closeConn()
    sendToMe(subject = "update Country  end", body = "update Country table: SiteBased")
    logger.info("update Country for table SiteBased end")
  

def main():
    updateTrackingWebsite()
    updateCountry()

if __name__ == "__main__":
    main()
#############################################################################################################
#sed  "/\/home\/vobile\/cwj\/ViacomProject\/dashboard\/job/\/Job\/HBO\/Dashboard\/TitleBased/g"
