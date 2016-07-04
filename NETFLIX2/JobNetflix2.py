#!/usr/bin/env python
# coding: utf-8
# author: suncong
# date: 2015-11-18
 
import sys 
import time
import datetime
import random
import MySQLdb
import ConfigParser

conf = ConfigParser.ConfigParser()
conf.read("db.conf")

class NETFLIX2(object):
    def __init__(self):
	pass
    
    def connectGS(self):
        try:
            conn = MySQLdb.connect(host = conf.get("db_conf", "ip"), user = conf.get("db_conf", "user"), \
		 passwd = conf.get("db_conf", "passwd"), db = "DM_NETFLIX")
            cur = conn.cursor()
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return cur, conn

    def connectVT(self):
        try:
	    conn = MySQLdb.connect(host = conf.get("vt_conf", "ip"), user = conf.get("vt_conf", "user"), \
		 passwd = conf.get("vt_conf", "passwd"), db = "tracker2")
	    cur = conn.cursor()
	except MySQLdb.Error,e:
	    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
	return cur,conn

    def closeMySQL(self, cur, conn):
        cur.close()
        conn.commit()
        conn.close()

    def ETL(self):
	cur,conn = self.connectVT()
	sql = """SELECT a.meta_title 'Video Title', c.website_name 'Linking Site', b.link_url 'Linking URL' FROM matchedVideo a, matchedVideo_linkURL b, mddb.trackingWebsite c WHERE a.id = b.matchedVideo_id AND a.company_id = b.company_id AND a.company_id = 1447 AND hide_flag = 2 AND a.takeoff_time = 0 AND a.count_send_notice = 0 AND a.is_in_takedown_queue = 'false' AND b.trackingWebsite_id = c.id AND a.trackingWebsite_id IN (53,54) and b.created_at >= date_format(date_sub(date_sub(now(),INTERVAL WEEKDAY(NOW()) DAY),INTERVAL 1 WEEK),'%Y-%m-%d') and b.created_at < date_format(date_sub(now(),INTERVAL WEEKDAY(NOW()) DAY),'%Y-%m-%d') order by 1,2,3;"""
        cur.execute(sql)
	res = cur.fetchall()
	self.closeMySQL(cur, conn)

	date_id = str(datetime.date.today())
	startat =  str(datetime.date.today() - datetime.timedelta(days=7))
	cur,conn = self.connectGS()
	for e in res:
	    title = e[0].replace('"','\\"')
	    ls = e[1]
	    lu = e[2]
	    insertSQL = """insert into torrnetSiteMatch values("%s","%s","%s","%s","%s",current_timestamp());""" \
		%(date_id,title,ls,lu,startat)
	    print insertSQL
	    try:
		cur.execute(insertSQL)
	    except Exception,e:
		print e
        self.closeMySQL(cur, conn)

    def ETL_Test(self):
	cur,conn = self.connectVT()
        sql = """SELECT a.meta_title 'Video Title', c.website_name 'Linking Site', b.link_url 'Linking URL' FROM matchedVideo a, matchedVideo_linkURL b, mddb.trackingWebsite c WHERE a.id = b.matchedVideo_id AND a.company_id = b.company_id AND a.company_id = 1447 AND hide_flag = 2 AND a.takeoff_time = 0 AND a.count_send_notice = 0 AND a.is_in_takedown_queue = 'false' AND b.trackingWebsite_id = c.id AND a.trackingWebsite_id IN (53,54) and b.created_at >= '2016-03-07' and b.created_at < '2016-03-14' order by 1,2,3;"""
        cur.execute(sql)
        res = cur.fetchall()
        self.closeMySQL(cur, conn)

        date_id = '2016-03-14'
        startat = '2016-03-07'
        cur,conn = self.connectGS()
        for e in res:
            title = e[0].replace('"','\\"')
            ls = e[1]
            lu = e[2]
            insertSQL = """insert into torrnetSiteMatch values("%s","%s","%s","%s","%s",current_timestamp());""" \
                %(date_id,title,ls,lu,startat)
            print insertSQL
            try:
		cur.execute(insertSQL)
	    except Exception,e:
		print e
        self.closeMySQL(cur, conn)

def main():
    s = NETFLIX2()
    s.ETL()
    #s.ETL_Test() 

if __name__=="__main__":
    main()

