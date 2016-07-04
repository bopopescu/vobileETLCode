#!/usr/bin/env python
# coding: utf-8
# author: suncong
# date: 2016-01-13

import re
import sys
import cookielib
import ConfigParser
import MySQLdb
import datetime
import random

conf = ConfigParser.ConfigParser()
conf.read("/Job/executiveDashboard/conf/db.conf")

class DBOperation:
    def __init__(self):
        pass

    def connectMySQL(self):
        try:
            conn = MySQLdb.connect(host = conf.get("db_online_conf", "ip"), user = conf.get("db_online_conf", "user"), \
		passwd = conf.get("db_online_conf", "passwd"), db = "DM_EDASHBOARD")
            cur = conn.cursor()    
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
	    self.connectMySQL()
        return cur, conn

    def closeMySQL(self, cur, conn):
        cur.close()
        conn.commit()
        conn.close()
    
    def insertDate(self, sql):
        cur, conn = self.connectMySQL()
	cur.execute("set names utf8;")
	print sql
        cur.execute(sql)
        self.closeMySQL(cur, conn)


def getQMCInfo(db):
    unResponseStr = "\\xe6\\x9c\\xaa\\xe5\\x93\\x8d\\xe5\\xba\\x94"
    responsedStr = "\\xe5\\xb7\\xb2\\xe5\\x93\\x8d\\xe5\\xba\\x94"

    f = open("/Job/executiveDashboard/download/OPM.html",'r')
    f = str(f.readlines())
    html =  f.split("Active down")[0]
    unDone = html.count(unResponseStr)
    Done = html.count(responsedStr)
    reportDate = str(datetime.date.today())
    totalNum = unDone + Done
    sql = "insert into alert_metric values('%s',%s,%s,current_timestamp());" %(reportDate,totalNum,unDone)
    db.insertDate(sql)

def test(db):
    reportDate = str(datetime.date.today())
    if reportDate == "2016-05-12":
	totalNum = 15
	unDone = 2
    elif reportDate == "2016-05-13":
	totalNum = 20
	unDone = 3
    sql = "insert into alert_metric values('%s',%s,%s,current_timestamp());" %(reportDate,totalNum,unDone)
    db.insertDate(sql)

def main():
    db = DBOperation()
    #getQMCInfo(db)
    test(db)

if __name__=="__main__":
    main()

