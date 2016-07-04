#!/usr/bin/env python
# coding: utf-8
# author: suncong
# date: 2016-03-01
 
import sys 
import time
import datetime
import traceback
import logging
import MySQLdb
import ConfigParser
import smtplib
from email.mime.text import MIMEText

conf = ConfigParser.ConfigParser()
conf.read("/Job/FOX/Dashboard/TitleBased/conf/viacom_dashboard.cfg")

class CountryBased(object):
    def __init__(self):
   	self.host = "mail.vobile.cn"
        self.user = "alert_sender@vobile.cn"
        self.pwd = "HeOd}okiz|"
        self.sender = "alert_sender@vobile.cn"
        self.sender = "verify@vobile.cn"
        self.receivers = ["sun_cong@vobile.cn"]
 
    def connectTargetDB(self):
        try:
            conn = MySQLdb.connect(host = conf.get("target_server_staging", "host"), user = conf.get("target_server_staging", "user"),  \
			passwd = conf.get("target_server_staging", "passwd"), db = "FOX_DASHBOARD")
            cur = conn.cursor()
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return cur, conn

    def closeMySQL(self, cur, conn):
        cur.close()
        conn.commit()
        conn.close()

    def ETL(self, report_date):
	try:
	    cur,conn = self.connectTargetDB()
	    cur.execute("delete from CountryBased where report_date = '%s';"%report_date)
	    sql = """select "%s", hostCountry, title, ifnull(b.nationalFlag, 'unknown'), sum(allMatch), sum(infringing), 0 as allIPs, 0 as infringingIPs, current_timestamp() from (select hostCountry, title, matchedNum+infringingNumCMS as allMatch, infringingNum+infringingNumCMS as infringing, 0 as allIPs, 0 as infringingIPs from SiteBased where reportDate = "%s") a left join (select nationalFlag, countryName from TitleBasedCountry where countryName <> 'unknown') b on a.hostCountry = b.countryName group by 2,3 order by 2 desc;"""%(report_date, report_date)
	    cur.execute(sql)
	    res = cur.fetchall()
	    insertSql =  """INSERT INTO CountryBased values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
	    cur.executemany(insertSql, res)
	    self.closeMySQL(cur,conn)
	except Exception,e:
	    print traceback.format_exc()
            logging.error(traceback.format_exc())
	print report_date, " finished ", len(res)

    def ETL_history(self):
	beg = datetime.date(2015,3,1)
	end = datetime.date.today() - datetime.timedelta(days=1)
	for i in xrange((end- beg).days+1):
	    date = str(end - datetime.timedelta(days=i))
	    self.ETL(date)
 
    def send(self):
	sql = "select report_date, max(gmt_create), count(1) from CountryBased group by 1 order by 1 desc;"
	cur,conn = self.connectTargetDB()
	cur.execute(sql)
	res = cur.fetchall()
	self.closeMySQL(cur,conn)
	content = "<br>"
	for e in res:
	    content += str(e[0]) + " , "
	    content += str(e[1]) + " , "
	    content += str(e[2]) + "</br>"
        message = MIMEText(content, "html")
        message["Subject"] = "Fox Dashboard CountryBased Daily Job"
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

def main():
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='/Job/FOX/Dashboard/TitleBased/log/countryBased.log',
                filemode='a')
    c = CountryBased()
    c.ETL_history()
    c.send()

if __name__=="__main__":
    main()

