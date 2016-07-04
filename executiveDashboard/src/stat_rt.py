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
import smtplib
import ConfigParser
from email.mime.text import MIMEText

conf = ConfigParser.ConfigParser()
conf.read("/Job/executiveDashboard/conf/db.conf")

class RT(object):
    def __init__(self):
   	self.host = "mail.vobile.cn"
        self.user = "alert_sender@vobile.cn"
        self.pwd = "HeOd}okiz|"
        self.sender = "alert_sender@vobile.cn"
        self.sender = "verify@vobile.cn"
	self.receivers = ["sun_cong@vobile.cn","chen_weijie@vobile.cn"]
        self.receivers = ["sun_cong@vobile.cn","hong_wubing@vobile.cn","chen_weijie@vobile.cn"]
 
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

    def connectTargetDB(self):
        try:
            conn = MySQLdb.connect(host = conf.get("db_online_conf", "ip"), user = conf.get("db_online_conf", "user"),  \
			passwd = conf.get("db_online_conf", "passwd"), db = "DM_EDASHBOARD")
            cur = conn.cursor()
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
	    self.connectTargetDB()
        return cur, conn

    def closeMySQL(self, cur, conn):
        cur.close()
        conn.commit()
        conn.close()
	
    def stat_rt(self):
	L = []
	report_date = str(datetime.date.today() - datetime.timedelta(days=1))
	date = report_date.replace('-','.') 
	reader = csv.reader(file("/Job/executiveDashboard/Tmp_file/%s.csv"%date, "rb"))
 	if 0 == len(open("/Job/executiveDashboard/Tmp_file/%s.csv"%date, "rb").readlines()):
	    print "no csv"
	    self.send("csv of RT didn't upload to FTP server", "%s.csv of RT didn't upload to FTP server" %date)
	    sys.exit()	
	
	for l in reader:
	    if len(l) == 0:
		continue
	    L.append(l[0])
	insertSql = "insert into rt_metric values('%s',%s,%s,%s,'%s',%s,%s,%s,%s,'%s',%s,%s,%s,current_timestamp());" \
		%(report_date,L[1],L[2],L[3],L[4],L[5],L[6],L[7],L[8],L[9],L[10],L[11],L[12])
	print insertSql
	cur,conn = self.connectTargetDB()
	cur.execute(insertSql)
	self.closeMySQL(cur,conn)

def main():
    r = RT()
    r.stat_rt()

if __name__=="__main__":
    main()

