#!/usr/bin/env python
# coding: utf-8
# author: suncong
# date: 2015-12-30
# monitor big file more than 1G, output path and file size 
import time
import os
import smtplib
from email.mime.text import MIMEText
from email.MIMEMultipart import MIMEMultipart

curTime = time.strftime("%Y%m%d", time.localtime(time.time()-24*60*60))

class SendMail(object):
    def __init__(self):
        self.host = "mail.vobile.cn"
        self.user = "alert_sender@vobile.cn"
        self.pwd = "HeOd}okiz|"
        self.sender = "alert_sender@vobile.cn"
        #self.receivers = ["du_li@vobile.cn"]
	#self.cc = ["chen_weijie@vobile.cn"]
	self.receivers = ["manni.nagi@vobileinc.com", "support@vobileinc.com"]
	self.cc = ["wang_geng@vobile.cn"]
	self.Bcc = ["sun_cong@vobile.cn"]

    def send(self, subject, content):
        #message = MIMEText(content, "html")
        message = MIMEMultipart() 
	message["Subject"] = subject
        message["From"] = self.user
        message["To"] = ";".join(self.receivers)
	message["Cc"] = ";".join(self.cc)
	att = MIMEText(open(r'/Job/datawarehouse/opertaion/mailsender/data/VTOperationReport_AllAccount' + curTime + '.xlsx','rb').read(),'base64','gb2312')
	att["Content-Type"] = 'application/octet-stream'
	att["Content-Disposition"] = 'attachment;filename="VTOperationReport_AllAccount.xlsx"'
	message.attach(att)
        try:
            server = smtplib.SMTP()
            server.connect(self.host)
            server.ehlo()
            server.starttls()
            server.login(self.user, self.pwd)
	    sendList = self.receivers + self.cc + self.Bcc
            server.sendmail(self.sender, sendList, message.as_string())
	    server.close()
        except Exception,e:
            print e
	
def main():
    client = SendMail()
    client.send("VT Operation Mertics Reports","")

if __name__=="__main__":
    main()

