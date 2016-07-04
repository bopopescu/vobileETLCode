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

    def ETL(self):
	YM = str(time.strftime('%Y-%m',time.localtime(time.time() - 24*60*60*30)))
	beg = str(datetime.date(datetime.date.today().year,datetime.date.today().month-1,1))
        end = str(datetime.date(datetime.date.today().year,datetime.date.today().month,1))
        cur, conn = self.connectVT()
        cur.execute("set time_zone = '-08:00';")
	sql = """select date_format(date_sub(date_sub(now(),INTERVAL WEEKDAY(NOW()) DAY),INTERVAL 0 MONTH),'%%Y-%%m') AS 'YM', (select meta_title from mddb.meta where id=me.meta_id) 'Title', (select meta_type from mddb.meta where id=me.meta_id) 'metaType', me.priority_type, (CASE WHEN me.trackingSetting_id > 0 AND me.start_at <= NOW() THEN 'active' ELSE 'inactive' END) AS 'title_status', sum(ifnull(a.numberOfClipsReported,0)) 'numberOfClipsReported', sum(ifnull(a.numberOfClipsRemoved,0)) 'numberOfClipsRemoved', CURRENT_TIMESTAMP AS ETL_DTE from (SELECT DATE_FORMAT(mv.first_send_notice_date, '%%Y-%%m') AS YM, mv.trackingMeta_id AS 'trackingMeta_id', meta_type AS metaType, m.meta_title, SUM(CASE WHEN mv.count_send_notice > 0 THEN 1 ELSE 0 END) AS numberOfClipsReported, SUM(CASE WHEN mv.count_send_notice > 0 AND mv.matchedFile_id = 0 AND mv.takeoff_time >0 THEN 1 ELSE 0 END) AS numberOfClipsRemoved FROM mddb.trackingWebsite tw, mddb.meta m, tracker2.matchedVideo mv WHERE mv.trackingWebsite_id = tw.id AND m.id = mv.trackingMeta_id AND tw.website_type = 'ugc' AND mv.company_id=55 AND mv.first_send_notice_date >= '%s' AND mv.first_send_notice_date < '%s' AND (meta_type = 'Movie' OR meta_type = 'TV') GROUP BY 1,2,3,4) a RIGHT JOIN tracker2.metaExtraInfo me ON a.trackingMeta_id = me.meta_id WHERE me.company_id = 55 AND (priority_type = 'tier 1' OR priority_type = 'tier 2') group by 1,2,3,4,5""" %(beg,end)
	cur.execute(sql)
        res = cur.fetchall()
        self.closeMySQL(cur, conn)
    
        cur, conn = self.connectDMSUMMIT()
        for t in res:
            print t[1], t[1].replace("'", "\'")
            insert_sql = """insert into TierTvMovie(YM, Title, metaType, priorityType, titleStatus, numberOfClipsReported, numberOfClipsRemoved) values ("%s", "%s", "%s", "%s", "%s", %s, %s);""" %(YM, t[1].replace("'", "\'"), t[2], t[3], t[4], t[5],t[6])
            print insert_sql
            cur.execute(insert_sql)
        self.closeMySQL(cur, conn)


    def ETL_old(self):
	YM = str(time.strftime('%Y-%m',time.localtime(time.time() - 24*60*60*30)))
	beg = str(datetime.date(datetime.date.today().year,datetime.date.today().month-1,1))
	end = str(datetime.date(datetime.date.today().year,datetime.date.today().month,1))
	cur, conn = self.connectVT()
	cur.execute("set time_zone = '-08:00';")
        sql = "SELECT DATE_FORMAT(mv.first_send_notice_date, '%%Y-%%m') AS YM, mv.vddb_title AS Title, meta_type AS metaType, priority_type AS priorityType, SUM(CASE WHEN mv.count_send_notice > 0 THEN 1 ELSE 0 END) AS numberOfClipsReported, SUM(CASE WHEN mv.count_send_notice > 0 AND mv.matchedFile_id = 0 AND mv.takeoff_time >0 THEN 1 ELSE 0 END) AS numberOfClipsRemoved, CURRENT_TIMESTAMP AS ETL_DTE FROM mddb.trackingWebsite tw, tracker2.matchedVideo mv, mddb.meta m, tracker2.metaExtraInfo me WHERE mv.trackingWebsite_id = tw.id AND m.id = me.meta_id AND m.id = mv.trackingMeta_id AND tw.website_type = 'ugc' AND mv.company_id=55 AND me.company_id=55 AND mv.first_send_notice_date >= '%s' AND mv.first_send_notice_date < '%s' AND (meta_type = 'Movie' OR meta_type = 'TV') AND (priority_type = 'tier 1' OR priority_type = 'tier 2') GROUP BY 1, 2, 3, 4;" %(beg, end)
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
    s.ETL()

if __name__=="__main__":
    main()

