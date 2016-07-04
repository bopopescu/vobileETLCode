#!/usr/bin/env python
# coding: utf-8
# author: suncong
# date: 2015-07-07
 
import sys
import re
import time
import random
import calendar
import logging
import socket
import urllib2
import MySQLdb
from bs4 import BeautifulSoup

socket.setdefaulttimeout(10.0)

RETRYTIME = 3
PROXYS = ['183.203.14.88:3129','218.60.101.39:55336','218.89.170.114:8888','61.234.249.127:8118','218.204.140.213:8118','117.185.13.87:8080','106.44.176.251:80','123.193.70.213:80','124.254.57.150:8118','112.114.63.27:55336']
baseUrl = "http://www.alexa.com/siteinfo/"
curDate = time.strftime("%Y-%m-%d")
Y, M, D = str(curDate).split("-")
if calendar.isleap(int(Y)) == True and int(D) == 29:
    isEndOfMonth = 1
elif int(M) in [1,3,5,7,8,10,12] and int(D) == 31:
    isEndOfMonth = 1
elif int(M) in [4,6,9,11] and int(D) == 30:
    isEndOfMonth = 1
else:
    isEndOfMonth = 0 

class DBOperation:
    def __init__(self):
        pass

    def connectMySQL(self):
        try:
            #conn = MySQLdb.connect(host = "54.67.114.123", user = "kettle", passwd = "kettle", db = "DM_VIACOM")
	    conn = MySQLdb.connect(host = "192.168.110.114", user = "kettle", passwd = "k3UTLe", db = "DM_VIACOM")
            cur = conn.cursor()    
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return cur, conn

    def closeMySQL(self, cur, conn):
        cur.close()
        conn.commit()
        conn.close()

    def InsertDate(self, id, display_name, rank, topOneCountry):
        cur, conn = self.connectMySQL()
        sql = "insert into Website_Alexa_Info(WebsiteId, CreateDate, DisplayName, IsEndOfMonth, Rank, TopOneCountry) values('%s','%s','%s','%s','%s','%s');" %(id, curDate, display_name, isEndOfMonth, rank, topOneCountry)
        print sql 
        cur.execute(sql)
        self.closeMySQL(cur, conn)

    def searchRankZero(self):
        cur, conn = self.connectMySQL()
        sql = "select WebsiteId from Website_Alexa_Info where Rank = 0 and CreateDate = '%s';" %str(curDate)
        print sql
	cur.execute(sql)
	ids = cur.fetchall() 
        self.closeMySQL(cur, conn)
        return ids

    def searchDomain(self, id):
        cur, conn = self.connectMySQL()
        sql = "select Domain from Alexa_WebsiteId_Domain where WebsiteId = %s;" %id
	cur.execute(sql)
	domain = cur.fetchone()
        self.closeMySQL(cur, conn)
        return domain

    def UpdateDate(self, id, rank, topOneCountry):
        cur, conn = self.connectMySQL()
        sql = "UPDATE Website_Alexa_Info SET Rank = '%s', topOneCountry = '%s' WHERE WebsiteId = '%s' and CreateDate = '%s';" %(rank, topOneCountry, id, str(curDate))
	print sql
	cur.execute(sql)
	self.closeMySQL(cur, conn)

def dealWithStr(s):
    rank = ""
    for i in xrange(0, len(s)):
        if s[i].isdigit() == True:
            rank += s[i]
    return rank

def alexaSpider(db):
    f = open("id_dis_domain", "r")
    count = 0
    for line in f.readlines():
        count += 1
        line = line.strip("\n")
        id, display_name, domain = line.split(",")[1], line.split(",")[2], line.split(",")[3]
        url = baseUrl + domain
        rank, topOneCountry = parserUrl(url)
	db.InsertDate(id, display_name, rank, topOneCountry)
        print "finished num = ",count 
    f.close()
        
def parserUrl(url):
    retryTime = 0
    while retryTime < RETRYTIME:
        retryTime += 1 
        proxy = random.choice(PROXYS)
        try:
            req = urllib2.Request(url)
            req.set_proxy(proxy, "http")
            html = urllib2.urlopen(req).read()
            soup = BeautifulSoup(html)
        except Exception,e:
            if retryTime == 3:
                return "", ""
    	    else:
		pass
    try:
        rank = dealWithStr(soup.find("strong", {"class":"metrics-data align-vmiddle"}).get_text())
    except Exception,e:
        rank = ""
        print "rank: ",e
    try:
        topOneCountry = dict(soup.find("img", {"class":"dynamic-icon"}).attrs)['alt']
    except Exception,e:
        topOneCountry = ""
        print "topOneCountry: ", e
    return rank, topOneCountry[:-5]

def runAgain(db):
    againIds = db.searchRankZero()
    for i in xrange(0, len(againIds)):
   	id = int(againIds[i][0])
        domain = str(db.searchDomain(id)[0])
	print domain
        url = baseUrl + domain
        rank, topOneCountry = parserUrl(url)
        db.UpdateDate(id, rank, topOneCountry)

def main():
    print parserUrl("http://www.alexa.com/siteinfo/dl7.torrentreactor.net")
    #db = DBOperation()
    #alexaSpider(db)
    #for i in xrange(0, 10):
        #runAgain(db)

if __name__=="__main__":
    main()
