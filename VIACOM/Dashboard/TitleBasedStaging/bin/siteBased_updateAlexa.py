#!/usr/bin/env python
#coding:utf8
#Date: 2016-03-22
#Author: cwj
#Desc: data from TitleBased1 to SiteBased, TitleBasedRemoveNum to  SiteBasedRemoveNum
#

from mysqlHelp import MySQLHelper
import ConfigParser
from parseConfig import CfgParser
import sys
import os
import logging
import time
from titleBased import getConfMysqlInfo, getMinDatePara

logger = logging.getLogger("siteBased_updateAlexa")
logger.setLevel(logging.DEBUG)
log_file = '/Job/VIACOM/Dashboard/TitleBasedStaging/log/siteBased_updateAlexa.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)


#(select ifnull(max(reportDate), "2015-12-31") from SiteBasedAlexa where reportDate < (select max(reportDate) from SiteBasedAlexa))

def updateSiteBasedAlexa():
    logger.info("start")
    alexa_report_date_para = getMinDatePara("SiteBasedAlexa", "reportDate")
    update_SQL = """
      update SiteBased as a, SiteBasedAlexa as b
      set a.alexaGlobalRank = b.alexaGlobalRank, a.alexaTopCountry = b.alexaTopCountry
      where a.trackingWebsite_id = b.trackingWebsite_id
        and b.reportDate = (select max(reportDate) from SiteBasedAlexa)
        and a.reportDate > (select max(reportDate) from SiteBasedAlexa)
    """
    logger.info("end")
  

def main():
    updateSiteBasedAlexa()

if __name__ == "__main__":
    main()

#################################################################################################################################
#sed  "/\/home\/vobile\/cwj\/ViacomProject\/dashboard\/job/\/Job\/VIACOM\/Dashboard\/TitleBased/g"
