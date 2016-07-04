#!/usr/bin/env python
# coding: utf-8
# author: suncong
# date: 2015-12-30
# monitor big file more than 1G, output path and file size 

import os
import time
import xlwt
import datetime
import smtplib
import MySQLdb
from email.mime.text import MIMEText

yesterday = str(datetime.date.today() - datetime.timedelta(days=1))
tables = ("rt_metric","alert_metric","cpu_metric","match_metric","mediawise_task_metric","system_efficiency_metric")

def judgeETL():
    conn = MySQLdb.connect(host="54.67.114.123",user="kettle",passwd="kettle",db="DM_EDASHBOARD")
    cur = conn.cursor()

    failList = []
    for t in tables:
	cur.execute("select max(report_date) from %s;"%t)
        dbMaxDate = str(cur.fetchall()[0][0])
	if yesterday <> dbMaxDate:
	    failList.append(t)

    cur.close()
    conn.commit()
    conn.close()
    return failList


def match():
    conn = MySQLdb.connect(host="54.67.114.123",user="kettle",passwd="kettle",db="DM_EDASHBOARD")
    cur = conn.cursor()
    #yesterday = '2016-05-20'
    cur.execute("select report_date,product,match_num from match_metric where report_date = '%s'"%yesterday)
    res1 = cur.fetchall()
    cur.execute("select area,sum(video_time)/sum(query_time) from mediawise_task_metric where report_date = '%s' group by 1;"%yesterday)   
    res2 = cur.fetchall()
    content = "<br>match: </br>"
    for e in res1:
        content += str(e[0]) + " , "
        content += str(e[1]) + " , "
        content += str(e[2]) + "</br>"
    content += "mediawise: </br>"
    for e in res2:
	content += str(e[0]) + " , "
	content += str(e[1]) + "</br>"

    cur.close()
    conn.commit()
    conn.close()
    return content

class SendMail(object):
    def __init__(self):
        self.host = "mail.vobile.cn"
        self.user = "alert_sender@vobile.cn"
        self.pwd = "HeOd}okiz|"
        #self.sender = "alert_sender@vobile.cn"
        self.sender = "verify@vobile.cn"
	self.receivers = ["sun_cong@vobile.cn"]

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

def main():
    failList = judgeETL()
    client = SendMail()
    info = match()
    if len(failList) == 0:
        client.send("Exe Dashboard", str(yesterday) + " complete well!" + "</br>" + info)
    else:
        client.send("WARNNING Exe Dashboard", str(yesterday) + str(failList).replace('[','').replace(']','') + " failed" + "</br>" + info)

if __name__=="__main__":
    main()

