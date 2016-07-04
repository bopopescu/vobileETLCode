#!/usr/bin/env python
# coding: utf-8
# author: suncong
# date: 2015-09-17
 
import sys 
import time
import datetime
import random
import MySQLdb
import ConfigParser

conf = ConfigParser.ConfigParser()
conf.read("db.conf")

class Summary(object):
    def __init__(self):
	pass   

 
    def connectDMSUMMIT(self):
        try:
            conn = MySQLdb.connect(host = conf.get("db_conf", "ip"), user = conf.get("db_conf", "user"), passwd = conf.get("db_conf", "passwd"), db = "DM_SUMMIT")
            cur = conn.cursor()
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return cur, conn

    def connectVT(self):
        try:
	    conn = MySQLdb.connect(host = conf.get("vt_conf", "ip"), user = conf.get("vt_conf", "user"), passwd = conf.get("vt_conf", "passwd"), db = "")
	    cur = conn.cursor()
	except MySQLdb.Error,e:
	    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
	return cur,conn

    def closeMySQL(self, cur, conn):
        cur.close()
        conn.commit()
        conn.close()

    def ETL(self, YM, beg, end):
	cur, conn = self.connectVT()
	cur.execute("set time_zone = '-08:00';")
	cur.execute("set names utf8")
        sql = "select DATE_FORMAT(mv.first_send_notice_date, '%%Y-%%m') AS YM, tw.display_name, SUM(case when mv.count_send_notice > 0 then 1 ELSE 0 END) , SUM(CASE WHEN mv.count_send_notice > 0 and mv.matchedFile_id = 0 and mv.takeoff_time >0 THEN 1 ELSE 0 END) , sum(CASE WHEN mv.first_send_notice_date > 0 AND mv.takeoff_time > 0 THEN (Timestampdiff(HOUR, mv.first_send_notice_date, mv.takeoff_time)) end) / (sum(CASE WHEN mv.first_send_notice_date > 0 AND mv.takeoff_time > 0 THEN 1 end))  FROM mddb.trackingWebsite tw,tracker2.matchedVideo mv WHERE tw.id=mv.trackingWebsite_id and mv.company_id=55 and mv.first_send_notice_date >= '%s' and mv.first_send_notice_date < '%s' and tw.website_type = 'ugc' group by display_name;" %(beg, end)
	cur.execute(sql)
	res = cur.fetchall()
	self.closeMySQL(cur, conn)
	
        cur, conn = self.connectDMSUMMIT()
	for t in res:
	    if t[4] == None:
		insert_sql = "insert into SiteCompliance(YM,siteName,numberOfClipsReported,numberOfClipsRemoved) values ('%s', '%s', %s, %s);" %(t[0], t[1], t[2], t[3])
	    else:
		insert_sql = "insert into SiteCompliance(YM,siteName,numberOfClipsReported,numberOfClipsRemoved,responseTime) values ('%s', '%s', %s, %s, %s);" %(t[0], t[1], t[2], t[3], t[4])
	    print insert_sql
	    cur.execute("set names utf8")
	    cur.execute(insert_sql)
        self.closeMySQL(cur, conn)

def main():
    s = Summary()
    ym = ("2015-01","2015-02","2015-03","2015-04","2015-05","2015-06","2015-07","2015-08", "2015-09")
    BEG = ("2015-01-01","2015-02-01","2015-03-01","2015-04-01","2015-05-01","2015-06-01","2015-07-01","2015-08-01","2015-09-01")
    END = ("2015-02-01","2015-03-01","2015-04-01","2015-05-01","2015-06-01","2015-07-01","2015-08-01","2015-09-01","2015-10-01")
    for i in xrange(0, len(ym)):
        s.ETL(ym[i], BEG[i], END[i])

if __name__=="__main__":
    main()

