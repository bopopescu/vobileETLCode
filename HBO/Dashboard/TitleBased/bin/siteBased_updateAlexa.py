#!/usr/bin/env python
#coding:utf8
#Date: 2016-03-22
#Author: cwj
#Desc: data from TitleBased1 to SiteBased, TitleBasedRemoveNum to  SiteBasedRemoveNum
#

from mysqlHelp import MySQLHelper
import logging
import time
import datetime
from titleBased import getConfMysqlInfo, getMinDatePara
from sendMail import sendToMe

logger = logging.getLogger("siteBased_updateAlexa")
logger.setLevel(logging.DEBUG)
#log_file = '/Job/HBO/Dashboard/TitleBasedStaging/log/siteBased_updateAlexa.log'
log_file = '/Job/HBO/Dashboard/TitleBased/log/siteBased_updateAlexa.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)


def updateSiteBasedAlexa():
    sendToMe(subject = "update alexa data start", body = "update alexa data start")
    logger.info(" updateSiteBasedAlexa start")
    update_SQL1 = """
      update SiteBased as a, SiteBasedAlexa as b
      set a.alexaGlobalRank = b.alexaGlobalRank, a.alexaTopCountry = b.alexaTopCountry
      where a.trackingWebsite_id = b.trackingWebsite_id
        and b.reportDate = (select max(reportDate) from SiteBasedAlexa)
        and a.reportDate > (select max(reportDate) from SiteBasedAlexa);
    """
    update_SQL2 = """
      update SiteBased as a, SiteBasedAlexa as b
      set a.alexaGlobalRank = b.alexaGlobalRank, a.alexaTopCountry = b.alexaTopCountry
      where a.trackingWebsite_id = b.trackingWebsite_id
       and b.reportDate = (select max(reportDate) from SiteBasedAlexa)
       and a.alexaTopCountry = "unknown"
       and a.alexaGlobalRank = 0;
    """
    try:
        target_server_section = "target_server_staging"
        target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
        target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, db_name = target_db, port = target_port, charset = 'utf8')
        target_mysql.queryCMD(update_SQL1)
        target_mysql.commit()
        target_mysql.queryCMD(update_SQL2)
        target_mysql.commit()
    except Exception, e:
        sendToMe(subject = "update alexa data ERROR", body = e)
        logger.DEBUG("update alexa data %s" %e)
    finally:
        target_mysql.closeCur()
        target_mysql.closeConn()
    
    logger.info(" updateSiteBasedAlexa end")
  

def addAlexaHis():
    beg = datetime.date(2015,3,1)
    end = datetime.date(2016,5,24)
    #end = datetime.date.today() - datetime.timedelta(days=1)
    for i in xrange((end- beg).days+1):
        date = str(end - datetime.timedelta(days=i))
        addAlexa(date)
	print "end '%s'"%date

def addAlexa(d):
    sendToMe(subject = "add alexa data start", body = "add alexa data start")
    logger.info(" addSiteBasedAlexa start")
    get_alexa_sql = "select '%s', trackingWebsite_id, websiteDomain, alexaGlobalRank, alexaTopCountry, current_timestamp() from SiteBasedAlexa where reportDate = (select max(reportDate) from SiteBasedAlexa);" %d
    insert_alexa_sql = "insert into SiteBasedAlexa(reportDate,trackingWebsite_id, websiteDomain, alexaGlobalRank, alexaTopCountry,ETLDate) VALUES(%s,%s,%s,%s,%s,%s);"
    try:
	target_server_section = "target_server_staging"
        target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
        target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, db_name = target_db, port = target_port, charset = 'utf8')
	alexa_result = target_mysql.queryCMD(get_alexa_sql)
	target_mysql.insertUpdateCMD(insert_alexa_sql, alexa_result)
	target_mysql.commit()
    except Exception, e:
        sendToMe(subject = "add alexa data ERROR", body = e)
        logger.DEBUG("add alexa data %s" %e)
    finally:
        target_mysql.closeCur()
        target_mysql.closeConn()
    logger.info(" addSiteBasedAlexa end")

def main():
    today = time.strftime("%Y-%m-%d")
    subject_start = "update and add alexa data start " + today
    sendToMe(subject = subject_start, body = "update and add alexa data (SiteBased table) start")
    updateSiteBasedAlexa()
    addAlexa(today)
    addAlexaHis()
    subject_end = "update and add alexa data end " + today
    sendToMe(subject_end, body = "update and add alexa data (SiteBased table) end")

if __name__ == "__main__":
    main()
#############################################################################################################
#sed  "/\/home\/vobile\/cwj\/ViacomProject\/dashboard\/job/\/Job\/HBO\/Dashboard\/TitleBased/g"

