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
        sql = "select DATE_FORMAT(mv.first_send_notice_date, '%%Y-%%m') AS YM, mv.vddb_title AS Title, meta_type AS metaType, priority_type AS priorityType, SUM(case when mv.count_send_notice > 0 then 1 ELSE 0 END) AS numberOfClipsReported, SUM(CASE WHEN mv.count_send_notice > 0 and mv.matchedFile_id = 0 and mv.takeoff_time >0 THEN 1 ELSE 0 END) AS numberOfClipsRemoved, current_timestamp AS ETL_DTE FROM mddb.trackingWebsite tw, tracker2.matchedVideo mv, mddb.meta m, tracker2.metaExtraInfo me WHERE mv.trackingWebsite_id = tw.id AND m.id = me.meta_id AND m.id = mv.trackingMeta_id AND tw.website_type = 'ugc' AND mv.company_id=55 AND me.company_id=55 AND mv.first_send_notice_date >= '%s' and mv.first_send_notice_date < '%s' AND (meta_type = 'Movie' OR meta_type = 'TV') AND (priority_type = 'tier 1' OR priority_type = 'tier 2') group by 1, 2, 3, 4;" %(beg, end)
        cur.execute(sql)
	res = cur.fetchall()
	self.closeMySQL(cur, conn)
	
        cur, conn = self.connectDMSUMMIT()
	for t in res:
	    print t[1], t[1].replace("'", "\'")
	    insert_sql = """insert into TierTvMovie(YM, Title, metaType, priorityType, numberOfClipsReported, numberOfClipsRemoved) values ("%s", "%s", "%s", "%s", %s, %s);""" %(t[0], t[1].replace("'", "\'"), t[2], t[3], t[4], t[5])
	    print insert_sql
	    cur.execute(insert_sql)
        self.closeMySQL(cur, conn)

def main():
    s = Summary()
    ym = ("2015-01","2015-02","2015-03","2015-04","2015-05","2015-06","2015-07","2015-08","2015-09")
    BEG = ("2015-01-01","2015-02-01","2015-03-01","2015-04-01","2015-05-01","2015-06-01","2015-07-01","2015-08-01","2015-09-01")
    END = ("2015-02-01","2015-03-01","2015-04-01","2015-05-01","2015-06-01","2015-07-01","2015-08-01","2015-09-01","2015-10-01")
    for i in xrange(0, len(ym)):
        s.ETL(ym[i], BEG[i], END[i])

if __name__=="__main__":
    main()

