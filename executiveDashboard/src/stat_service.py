#!/usr/bin/env python
# coding: utf-8
# author: suncong
# date: 2016-03-01
 
import sys 
import time
import csv
import datetime
import ftplib
import MySQLdb
import ConfigParser

conf = ConfigParser.ConfigParser()
conf.read("/Job/executiveDashboard/conf/db.conf")

class S(object):
    def __init__(self):
   	pass
 
    def connectTargetDB(self):
        try:
            conn = MySQLdb.connect(host = conf.get("db_online_conf", "ip"), user = conf.get("db_online_conf", "user"),  \
			passwd = conf.get("db_online_conf", "passwd"), db = "DM_EDASHBOARD")
            cur = conn.cursor()
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return cur, conn

    def closeMySQL(self, cur, conn):
        cur.close()
        conn.commit()
        conn.close()
	
    def stat_service_uptime(self):
	D = []
	beg = datetime.date(2016,5,1)
	end = datetime.date(2016,5,31)
	cur,conn = self.connectTargetDB()
	for i in xrange((end- beg).days+1):
	    report_date = str(beg + datetime.timedelta(days=i))
	    s1 = "insert into service_metric values('%s','VideoTracker',100,current_timestamp());" %report_date
	    s2 = "insert into service_metric values('%s','mediawise_china',100,current_timestamp());" %report_date
	    s3 = "insert into service_metric values('%s','mediawise_usa',100,current_timestamp());" %report_date
	    s4 = "insert into service_metric values('%s','AdTracker',100,current_timestamp());" %report_date
	    cur.execute(s1)
	    cur.execute(s2)
	    cur.execute(s3)
	    cur.execute(s4)
	self.closeMySQL(cur,conn)

    def test(self):
        beg = datetime.date(2016,5,1)
	end = datetime.date(2016,9,30)
	cur,conn = self.connectTargetDB()
	for i in xrange((end- beg).days+1):
	    report_date = str(beg + datetime.timedelta(days=i))
	    s1 = "insert into system_efficiency_metric values('%s','VT',4105,CURRENT_TIMESTAMP());" %report_date
	    s2 = "insert into system_efficiency_metric values('%s','VTX',279,CURRENT_TIMESTAMP());" %report_date
	    s3 = "insert into system_efficiency_metric values('%s','reclaim',244,CURRENT_TIMESTAMP());" %report_date
	    cur.execute(s1)
	    cur.execute(s2)
	    cur.execute(s3)
	self.closeMySQL(cur,conn)

def main():
    s = S()
    #s.stat_service_uptime()
    s.test()

if __name__=="__main__":
    main()

