#!/usr/bin/env python
#encoding:utf8
#Date: 2016-03-14
#Author: cwj
#Desc: data from vtweb to TitleBasedCountry
#
#

from mysqlHelp import MySQLHelper
import MySQLdb
import ConfigParser
from parseConfig import CfgParser
import sys
import os
import logging
from titleBased import getConfMysqlInfo
#reload(sys)
#sys.setdefaultencoding("utf-8")
cfg_file = "/Job/VIACOM/Dashboard/TitleBased/conf/viacom_dashboard.cfg"
if not os.path.exists(cfg_file):
	logging.debug(": config file not exists; file_name %s" %cfg_file)
	sendToMe(subject = "titleBased_country ERROR", body = "config file not exists")
	sys.exit(0)

vt_TitleBasedTrackingWebsite_SQL = """
	select
	  id as country_id,
	  -- region,
	  country_name as countryName
	  -- country_name as countryName,
	  -- national_flag as nationalFlag,
	  -- CURRENT_TIMESTAMP as ETLDate
	from mddb.country  where id = 2
"""
#vtweb_tracker2_section = "vtweb_tracker2"
vtweb_tracker2_section = "vtweb_staging"
try:
	vt_host, vt_user, vt_passwd, vt_port, vt_db = getConfMysqlInfo(vtweb_tracker2_section)
	vtweb_mysql = MySQLHelper(host=vt_host, user=vt_user,passwd=vt_passwd, port = vt_port, db_name = vt_db)
#	vtweb_mysql.execute("set names utf8")
	result = vtweb_mysql.queryCMD(vt_TitleBasedTrackingWebsite_SQL)
#	print result, repr(result[0][2]), result[0][2]

#        conn = MySQLdb.connect(host="54.67.114.123", user= "kettle", passwd= "kettle", db = "test", port = 3306, charset = "utf8")
        conn = MySQLdb.connect(host="54.67.114.123", user= "kettle", passwd= "kettle", db = "test", port = 3306)
        cur = conn.cursor()
#        cur.execute("insert into test.a (id, name) values(%s, %s)", result[0])
        sql = "insert into test.a (id, name) values(%s, '%s')"  %(result[0][0], result[0][1])
	print result
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

except Exception, e:
        print e
	sys.exit(0)
finally:
	vtweb_mysql.closeCur()
	vtweb_mysql.closeConn()

