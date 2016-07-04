#!/usr/bin/env python
# coding: utf-8
# author: suncong
# date: 2015-07-07
 
import sys
import re
import time
import datetime
import random
import calendar
import logging
import socket
import urllib2
import MySQLdb
from bs4 import BeautifulSoup

socket.setdefaulttimeout(10.0)

PROXYS = ['183.203.14.88:3129','218.60.101.39:55336','218.89.170.114:8888','61.234.249.127:8118','218.204.140.213:8118','117.185.13.87:8080','106.44.176.251:80','123.193.70.213:80','124.254.57.150:8118','112.114.63.27:55336']
baseUrl = "http://www.alexa.com/siteinfo/"
curDate = time.strftime("%Y-%m-%d")
RETRYTIME = 0

class DBOperation:
    def __init__(self):
        pass

    def connectMySQL(self):
        try:
            #conn = MySQLdb.connect(host = "54.184.177.217", user = "kettle", passwd = "kettle", db = "DM_VIACOM")
            conn = MySQLdb.connect(host = "192.168.110.114", user = "kettle", passwd = "k3UTLe", db = "DM_VIACOM")
            cur = conn.cursor()    
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return cur, conn

    def closeMySQL(self, cur, conn):
        cur.close()
        conn.commit()
        conn.close()

    def InsertDate(self, site, YM, display_name, rank, country, topOneCountry):
        createdAt = datetime.datetime.now() 
        cur, conn = self.connectMySQL()
        sql = "insert into Website_Alexa_Country(site, YM, ETL_DTE, AlexaRank, AlexaCountry, PrimaryHostCountry, DisplayName) values('%s', '%s','%s','%s','%s','%s','%s');" %(site, YM, createdAt, rank, topOneCountry, country, display_name)
        print sql 
        cur.execute(sql)
        self.closeMySQL(cur, conn)

def getInfo(db):
    cur, conn = db.connectMySQL()
    sql = "select YM, websiteName from Torrent_Summary_Monthly_Tmp";
    cur.execute(sql)
    result = cur.fetchall()
    db.closeMySQL(cur, conn)
    return result

def dealWithStr(s):
    rank = ""
    for i in xrange(0, len(s)):
        if s[i].isdigit() == True:
            rank += s[i]
    return rank

def getDisplayName(site):
    if len(site.split(".")) == 2:
	return site
    elif len(site.split(".")) >= 3:
	return site.split(".")[1] + "." + site.split(".")[2]
    elif len(site.split(".")) == 1:
	return site.split(".")[0]

def alexaSpider(db):
    result = getInfo(db)
    count = 0
    for t in result:
        count += 1
        YM = str(t[0])
	site = str(t[1])
	display_name = getDisplayName(site)
	if display_name == 'Total' or display_name == 'Other':
	    continue
        url = baseUrl + site
        rank, country, topOneCountry = parserUrl(url)
	db.InsertDate(site, YM, display_name, rank, country, topOneCountry)
        time.sleep(10)
        print "finished num = ",count 
        
def parserUrl(url):
    try:
        response = urllib2.urlopen(url)
        html = response.read()
        soup = BeautifulSoup(html)
    except Exception,e:
        return 0, "", ""

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
    try:
        country = soup.find("span", {"class":"text-inline"}).get_text()
    except Exception,e:
        country = ""
        print "country: ",e
    return rank, country, topOneCountry[:-5]

def runAgain(db):
    cur, conn = db.connectMySQL()
    sql = "select site from Website_Alexa_Country where AlexaRank = 0 and YM = (select distinct YM from Torrent_Summary_Monthly_Tmp);"
    cur.execute(sql)
    d_names = cur.fetchall()
    print d_names
    for i in xrange(0, len(d_names)):
        site = d_names[i][0]
        url = baseUrl + site
        rank, country, topOneCountry = parserUrl(url)
	sql = "update Website_Alexa_Country set AlexaRank = '%s', AlexaCountry = '%s', PrimaryHostCountry = '%s' where site = '%s';" %(rank, topOneCountry, country, site)
        print sql
        cur.execute(sql)
        time.sleep(10)
    db.closeMySQL(cur, conn)

def main():
    #print parserUrl("http://www.alexa.com/siteinfo/extratorrent.cc")
    db = DBOperation()
    alexaSpider(db)
    for i in xrange(0, 5):
        runAgain(db)

if __name__=="__main__":
    main()
