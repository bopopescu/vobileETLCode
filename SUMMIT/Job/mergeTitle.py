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

mList = ("Boss", "Deadbeat", "Mad Men Mad Men", "Nashville", "Saint George", "Dead Zone")


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
    
    def closeMySQL(self, cur, conn):
        cur.close()
        conn.commit()
        conn.close()


    def merge(self,YM, beg, end):
        cur, conn = self.connectDMSUMMIT()
	for l in mList:
	    sql = """select sum(numberOfClipsReported), sum(numberOfClipsRemoved) from TierTvMovie where priorityType ='tier 2' and YM = '%s' and Title like '%%%l%%';""" %(YM, l)
    	    print sql
	    cur.execute(sql)
	    res = cur.fetchall()
	    print res
	self.closeMySQL(cur, conn)

def main():
    s = Summary()
    ym = ("2016-01")
    BEG = ("2016-01-01")
    END = ("2016-02-01")
    for i in xrange(0, len(ym)):
        s.merge(ym[i], BEG[i], END[i])

if __name__=="__main__":
    main()

