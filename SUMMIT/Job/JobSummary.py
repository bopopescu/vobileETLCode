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
archTotal = 1377099617

class Summary(object):
    def __init__(self, host, user, pwd, sender, receivers):
	self.host = host
	self.user = user
	self.pwd = pwd
	self.sender = sender
	self.receivers = receivers

    def send(self, subject, content):
	message = MIMEText(content, "html")
        message["Subject"] = subject
	message["From"] = self.user
	message["To"] = ";".join(self.receivers)
        try:
	    server = smtplib.SMTP()
	    server.connect(self.host)
	    server.ehlo()
	    server.starttls()
            server.login(self.user, self.pwd)
	    for r in self.receivers:
		server.sendmail(self.sender, r, message.as_string()) 
	    server.close()
        except Exception,e:
	    print e
    
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
        sql = "SELECT SUM(CASE WHEN mv.count_send_notice > 0 THEN 1 ELSE 0 END) '#Takedown Sent', SUM(CASE WHEN mv.count_send_notice > 0 AND mv.matchedFile_id = 0 AND mv.takeoff_time >0 THEN 1 ELSE 0 END) removedNum, sum(view_count) FROM mddb.trackingWebsite tw, tracker2.matchedVideo mv WHERE mv.trackingWebsite_id = tw.id AND tw.website_type = 'ugc' AND mv.company_id=55 AND mv.first_send_notice_date >= '%s' AND mv.first_send_notice_date < '%s';" %(beg, end)
        cur.execute(sql)
	res = cur.fetchall()
	sqlTotal = "select sum(view_count) FROM mddb.trackingWebsite tw, tracker2.matchedVideo mv WHERE mv.trackingWebsite_id = tw.id AND tw.website_type = 'ugc' and mv.company_id=55 and mv.first_send_notice_date < '%s';" %end
	cur.execute(sqlTotal)
	total = cur.fetchall()[0][0] + archTotal
	self.closeMySQL(cur, conn)
	
        cur, conn = self.connectDMSUMMIT()
	insert_sql = "insert into Summary(YM, NumberOfClipsReported, NumberOfClipsRemoved, InfringingViews, TotalViews) values ('%s', %s, %s, %s, %s);" %(YM, int(res[0][0]), int(res[0][1]), int(res[0][2]), total)
	print insert_sql
	cur.execute(insert_sql)
        self.closeMySQL(cur, conn)

def main():
    s = Summary(conf.get("sender_conf", "host"), conf.get("sender_conf", "user"), conf.get("sender_conf", "pwd"), conf.get("sender_conf", "sender"), conf.get("sender_conf", "receivers"))
    s.ETL()

if __name__=="__main__":
    main()

