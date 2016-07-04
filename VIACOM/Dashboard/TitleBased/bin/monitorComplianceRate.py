#!/usr/bin/env python
#coding:utf8
#Date: 2016-04-20
#Author: cwj
#Desc: monitor complaince rate
#


from mysqlHelp import MySQLHelper
import ConfigParser
from parseConfig import CfgParser
import sys
import os
import logging
import time
from titleBased import getConfMysqlInfo
from sendMail import sendToMe
import re

logger = logging.getLogger("monitorComplianceRate")
logger.setLevel(logging.DEBUG)
log_file = '/Job/VIACOM/Dashboard/TitleBased/log/monitorComplianceRate.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

cfg_file = "/Job/VIACOM/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
if not os.path.exists(cfg_file):
    logging.debug(": config file not exists") 
    sendToMe(subject = "monitorComplianceRate ERROR", body = "config file not exists")
    sys.exit(0)
#################################################################################################################################
def monitor():
    logger.info("monitor complaince rate start")
    target_server_section = "target_server_staging"
    target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
    try:
        target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, 
            db_name = target_db, port = target_port, charset = 'utf8')
        monitor_SQL = """
            select   a.reportDate,   a.trackingWebsite_id,   a.title, 
              sum(b.removedNum)/sum(a.infringingNum) as removedRate 
            from TitleBased1 a, 
                (select reportDate, trackingWebsite_id, Title, sum(removedNum) as removedNum 
                    from TitleBasedRemoveNum1 group by 1, 2, 3) b 
            where a.reportDate=b.reportDate 
              and a.trackingWebsite_id=b.trackingWebsite_id 
              and a.title = b.title
            group by 1, 2, 3
            having removedRate > 1;
        """
        result = target_mysql.queryCMD(monitor_SQL)
        sj = "compliance rate " + str(time.strftime("%Y-%m-%d"))
        if result:   
            bd = "compliance rate > 1\n" + str(result)
            sendToMe(subject = sj + " ERROR", body = bd)
        else:
            bd = "compliance rate has no data greater than 1"
            sendToMe(subject = sj + " GOOD", body = bd)
    except Exception, e:
        logger.debug(" error, %s" %e)
        sendToMe(subject = "monitor complaince rate ERROR", body = re.sub(r'\'|"|!', "", str(e)))
        sys.exit(0)
    finally:
        target_mysql.closeCur()
        target_mysql.closeConn()
    logger.info("monitor complaince rate end")

def main():
    monitor()

if __name__ == "__main__":
    main()

