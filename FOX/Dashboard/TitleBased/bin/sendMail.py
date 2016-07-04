#!/usr/bin/env python
#coding:utf8
#Date: 2016-03-22
#Author: cwj
#Desc: send mail
#

import os
import logging
import logging.handlers

logging.basicConfig(format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
logger = logging.getLogger("sendMail")
logger.setLevel(logging.DEBUG)
log_file = '/Job/FOX/Dashboard/TitleBased/log/sendMail.log'
filehandler = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=5*1024*1024, backupCount=10, mode='a')
logger.addHandler(filehandler)

def sendToMe(subject = "fox dashboard", body = "hello, please input email body."):
    logger.info("send mail start")	
    cmd = """echo "%s"| mail -s "%s" sun_cong@vobile.cn -a From:sun_cong@vobile.cn""" %(body, subject)
    os.system(cmd)
    logger.info("send mail end")

def main():
	sendToMe()

if __name__ == "__main__":
	main()
