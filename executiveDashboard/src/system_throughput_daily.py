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
conf.read("/Job/executiveDashboard/conf/db.conf")

class SystemThroughPut(object):
    def __init__(self):
	pass
    
    def connectTargetDB(self):
        try:
            conn = MySQLdb.connect(host = conf.get("db_online_conf", "ip"), user = conf.get("db_online_conf", "user"), \
			passwd = conf.get("db_online_conf", "passwd"), db = "DM_EDASHBOARD")
            cur = conn.cursor()
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
	    self.connectTargetDB()
        return cur, conn

    def connectVT(self):
        try:
	    conn = MySQLdb.connect(host = conf.get("vt_conf", "ip"), user = conf.get("vt_conf", "user"), \
			 passwd = conf.get("vt_conf", "passwd"), db = "tracker2")
	    cur = conn.cursor()
	except MySQLdb.Error,e:
	    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
	return cur,conn

    def connectVTX(self):
	try:
	    conn = MySQLdb.connect(host = conf.get("vtx_conf", "ip"), user = conf.get("vtx_conf", "user"), \
			 passwd = conf.get("vtx_conf", "passwd"), db = "xtracker2")
	    cur = conn.cursor()
	except MySQLdb.Error,e:
	    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
	return cur,conn

    def connectReclaim(self):
        try:
            conn = MySQLdb.connect(host = conf.get("reclaim_conf", "ip"), user = conf.get("reclaim_conf", "user"),  \
			passwd = conf.get("reclaim_conf", "passwd"), db = "tracker2")
            cur = conn.cursor()
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return cur,conn

    def connectMediawise(self, mediawise_conf):
	try:
	    conn = MySQLdb.connect(host = conf.get(mediawise_conf,"ip"),user = conf.get(mediawise_conf,"user"),  \
			passwd = conf.get(mediawise_conf,"passwd"), db = "mediawise", port = int(conf.get(mediawise_conf,"port")))
            cur = conn.cursor()
	except MySQLdb.Error,e:
	    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
	return cur,conn

    def closeMySQL(self, cur, conn):
        cur.close()
        conn.commit()
        conn.close()

    def VT_match(self):
	beg = str(datetime.date.today() - datetime.timedelta(days=1))
	end = str(datetime.date.today())
	#sql = """SELECT count(1), company_id, trackingWebsite_id, b.website_type  \
	#	FROM matchedVideo a, mddb.trackingWebsite b WHERE a.created_at > "%s"  \
	#	AND a.created_at < "%s" AND a.trackingWebsite_id = b.id GROUP BY 2,3;""" %(beg,end)
	sql = """SELECT count(1) FROM matchedVideo a, mddb.trackingWebsite b WHERE a.created_at >= "%s"  \
		AND a.created_at < "%s" AND a.trackingWebsite_id = b.id;""" %(beg,end)
	cur,conn = self.connectVT()
        cur.execute(sql)
	res = cur.fetchall()
	self.closeMySQL(cur, conn)
	for e in res:
	    cur, conn = self.connectTargetDB()
	    insertSql = """insert into match_metric values('%s','VT',%s,'10','1','ugc',current_timestamp());""" \
			%(beg,e[0])
	    print insertSql
	    cur.execute(insertSql)
	    self.closeMySQL(cur, conn)

    def VTX_match(self):
	beg = str(datetime.date.today() - datetime.timedelta(days=1))
        end = str(datetime.date.today())
	#beg = '2016-05-15'
	#end = '2016-05-16'
        #sql = """SELECT count(1), company_id, trackingWebsite_id, b.website_type  \
        #        FROM matchedVideo a, mddb.trackingWebsite b WHERE a.created_at > "%s"  \
        #        AND a.created_at < "%s" AND a.trackingWebsite_id = b.id GROUP BY 2,3;""" %(beg,end)
        sql = """SELECT count(1) FROM matchedVideo a, mddb.trackingWebsite b WHERE a.created_at >= "%s"  \
		AND a.created_at < "%s" AND a.trackingWebsite_id = b.id;""" %(beg,end)
	cur,conn = self.connectVTX()
        cur.execute(sql)
        res = cur.fetchall()
        self.closeMySQL(cur, conn)
        for e in res:
            cur, conn = self.connectTargetDB()
            insertSql = """insert into match_metric values('%s','VTX',%s,'10','1','ugc',current_timestamp());""" \
			%(beg,e[0])
            print insertSql
            cur.execute(insertSql)
            self.closeMySQL(cur, conn)

    def reclaim_match(self):
	beg = str(datetime.date.today() - datetime.timedelta(days=1))
        end = str(datetime.date.today())
	#sql = """SELECT count(1), company_id, trackingWebsite_id, b.website_type  \
        #        FROM matchedVideo a, mddb.trackingWebsite b WHERE a.created_at > "%s"  \
        #        AND a.created_at < "%s" AND a.trackingWebsite_id = b.id GROUP BY 2,3;""" %(beg,end)
	sql = """SELECT count(1) FROM matchedVideo a, mddb.trackingWebsite b WHERE a.created_at >= "%s"  \
		AND a.created_at < "%s" AND a.trackingWebsite_id = b.id;""" %(beg,end)
	cur,conn = self.connectReclaim()
        cur.execute(sql)
        res = cur.fetchall()
        self.closeMySQL(cur, conn)
	for e in res:
            cur, conn = self.connectTargetDB()
            insertSql = """insert into match_metric values('%s','Reclaim',%s,'10','1','ugc',current_timestamp());""" \
			%(beg,e[0])
            print insertSql
            cur.execute(insertSql)
            self.closeMySQL(cur, conn)
	
    def mediawise_task(self):
	beg = str(datetime.date.today() - datetime.timedelta(days=1))
	end = str(datetime.date.today())
	sql_china = "select company_id, sum(clip_duration),sum(timestampdiff(second,created_at,end_query_time))  \
			from task where end_query_time >= '%s' and end_query_time < '%s' group by 1;" %(beg,end)
	sql_usa = "select company_id, sum(clip_duration),sum(timestampdiff(second,created_at,end_query_time))  \
			from task where end_query_time >= '%s' and end_query_time < '%s' group by 1;" %(beg,end)
	cur,conn = self.connectMediawise("mediawise_china_conf")
	cur.execute(sql_china)
	res_china = cur.fetchall()
	cur,conn = self.connectMediawise("mediawise_usa_conf")
	cur.execute(sql_usa)
	res_usa = cur.fetchall()
	self.closeMySQL(cur, conn)
	
	cur, conn = self.connectTargetDB()
	cur.execute("delete from mediawise_task_metric where report_date = '%s';"%beg)
	for e in res_china:
	    insertSql = """insert into mediawise_task_metric values('%s',%s,%s,%s,'finished','china',current_timestamp());"""  \
			%(beg,e[1],e[2],e[0])
	    print insertSql
	    cur.execute(insertSql)
	for e in res_usa:
	    insertSql = """insert into mediawise_task_metric values('%s',%s,%s,%s,'finished','usa',current_timestamp());"""  \
			%(beg,e[1],e[2],e[0])
	    print insertSql
	    cur.execute(insertSql)
	self.closeMySQL(cur, conn)

def main():
    s = SystemThroughPut()
    s.VT_match()
    s.VTX_match()
    s.reclaim_match()
    s.mediawise_task()

if __name__=="__main__":
    main()

