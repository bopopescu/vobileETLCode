#!/usr/bin/env python
#coding:utf8

import urllib
import socket
import MySQLdb
import time
from mysqlHelp import MySQLHelper
import ConfigParser
from parseConfig import CfgParser
import sys
import os
import logging
import time
import random
from titleBased import getConfMysqlInfo, getMinDatePara
from sendMail import sendToMe
import re

logger = logging.getLogger("SiteBased_alexa")
logger.setLevel(logging.DEBUG)
log_file = '/Job/HBO/Dashboard/TitleBased/log/siteBased_alexa.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

#get html from alexa
def getHtml(url):
    request_time = 0
    while True:
        request_time += 1
        try:
            page = urllib.urlopen(url)
            html = page.read()
            return html
        except Exception, e:
            logger.info(" get html, %s" %e)

        if request_time == 3:
            break
    return ""

def getGlobalRank(html):
    res = r"""<!-- Alexa web traffic metrics are available via our API at http://aws\.amazon\.com/awis -->\n(.+?)\s+</strong>"""
    rank = re.findall(res, html)
    if rank:
        return rank[0].replace("'", "").replace(",", "")
    else:
        return 0

def getTopOneCountry(html):
    res = r"""ALEXA\.viewsHelpers\.map\.areas=\[\n\{title:"(.+)?\}"""
    country = re.findall(res, html)
    if country:
        return country[0].replace("'", "").split(":")[0].title()
    else:
        return "unknown"

def getAlexaInfo(url):
    html = getHtml(url)
    return getGlobalRank(html), getTopOneCountry(html)

def main():
    cfg_file = "/Job/HBO/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
    if not os.path.exists(cfg_file):
        logging.debug(": config file not exists; file_name %s" %cfg_file) 
        sendToMe(subject = "SiteBased_alexa ERROR", body = "config file not exists")
        sys.exit(0)

    logger.info(": extract data from siteBased start")
    socket.setdefaulttimeout(10.0)

    sendToMe(subject = "SiteBased_alexa start", body = "extract data from siteBased start")
    target_server_section = "target_server_staging"
    target_host, target_user, target_passwd, target_port, target_db= getConfMysqlInfo(target_server_section)
    try:
        target_mysql = MySQLHelper(host=target_host, user=target_user, passwd=target_passwd, db_name = target_db, port = target_port, charset = 'utf8')
        alexa_date_min = getMinDatePara("SiteBasedAlexa", "reportDate")
        alexa_date_min = '2016-04-12'
        if alexa_date_min == None:
            alexa_date_min = time.strftime("%Y-%m-%d", time.localtime(time.time() - 10 * 24 * 60 * 60))
        alexa_date_max = time.strftime("%Y-%m-%d", time.localtime(time.time() - 1 * 24 * 60 * 60))
	delt = time.mktime(time.strptime(str(alexa_date_max), "%Y-%m-%d")) - time.mktime(time.strptime(str(alexa_date_min), "%Y-%m-%d"))
        if delt >= 10*24*60*60:
            alexa_date_dict = {"alexa_date_min": alexa_date_min, "alexa_date_max": alexa_date_max}
            site_SQL = """
                select distinct trackingWebsite_id, websiteDomain from SiteBased 
                where reportDate <= "%(alexa_date_max)s"
                  and reportDate > "%(alexa_date_min)s"
                  and alexaGlobalRank = 0
                  and alexaTopCountry = "unknown"
                """ %alexa_date_dict

            site_info = target_mysql.queryCMD(site_SQL)
            if site_info:
                base_url = "http://www.alexa.com/siteinfo/"
                for site in site_info:
                    domain = site[1]
                    url = base_url + domain
                    run_time = 0
                    alexaGlobalRank, alexaTopCountry = getAlexaInfo(url)
                    while True:         
                        run_time += 1
                        if alexaGlobalRank == 0 or alexaTopCountry == "unknown":
                            alexaGlobalRank, alexaTopCountry = getAlexaInfo(url)                    
                        else:
                            break
                        if run_time == 3:
                            break
                        time.sleep(random.randint(5, 8))

                    time.sleep(random.randint(5, 8))
                    alexa_info_tuple = [(alexa_date_max, site[0], site[1], alexaGlobalRank, alexaTopCountry, time.strftime("%Y-%m-%d %H:%M:%S"))]

                    if not (alexaGlobalRank == 0 and alexaTopCountry == "unknown"):
                        insert_SiteBasedAlexa_SQL = """
                            insert into SiteBasedAlexa
                                (reportDate, trackingWebsite_id, websiteDomain, alexaGlobalRank, alexaTopCountry, ETLDate) 
                            values (%s, %s, %s, %s, %s, %s)  
                            on duplicate  key update 
                                alexaGlobalRank = values(alexaGlobalRank), 
                                alexaTopCountry = values(alexaTopCountry), ETLDate  = values(ETLDate)
                        """
			try:
                        	target_mysql.insertUpdateCMD(insert_SiteBasedAlexa_SQL, alexa_info_tuple)
				print alexa_info_tuple
                        	target_mysql.commit()
			except MySQLdb.Error, e:
				logger.debug(e)
				sendToMe(subject = "update SiteBasedAlexa Error", body = re.sub(r'\'|"|!', "", str(e)))		
				continue
            else:
                logger.info("has no data %s" %alexa_date_max)        
    except Exception, e:
        logger.debug(": load data to SiteBasedAlexa %s" %e)
        sendToMe(subject = "SiteBasedAlexa ERROR", body = re.sub(r'\'|"|!', "", str(e)))
        sys.exit(0)
    finally:
        target_mysql.closeCur()
        target_mysql.closeConn()
        logger.info(":load data to SiteBasedAlexa  end")
    sendToMe(subject = "SiteBasedAlexa End", body = "load data to SiteBasedAlexa  end")

if __name__ == "__main__":
    main()
#################################################################################################################################
#sed  "/\/home\/vobile\/cwj\/ViacomProject\/dashboard\/job/\/Job\/HBO\/Dashboard\/TitleBased/g" 
