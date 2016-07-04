#!/usr/bin/env python
# coding: utf-8
# author: suncong
# date: 2015-12-08

import sys
from datetime import datetime
import pymongo
import MySQLdb

class DBOperation:
    def __init__(self):
        pass

    def connectMySQL(self):
        try:
            conn = MySQLdb.connect(host="192.168.110.114", user="kettle", passwd="k3UTLe", db="DM_VIACOM")
            conn.ping(True)
            cur = conn.cursor()
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return cur, conn

    def commitMySQL(self, conn):
        conn.commit()

    def closeMySQL(self, cur, conn):
        cur.close()
        conn.close()

    def insertMySQL(self,sql):
	cur, conn = self.connectMySQL()
	cur.execute(sql)
	self.closeMySQL(cur, conn)

    def connectMongo(self):
	client = pymongo.MongoClient("192.168.110.50", 27017)
	db = client.insight
	db.authenticate('insight','123456')
	#print db.collection_names(include_system_collections=False)
	return db 

    def closeMongo(self):
	pass

    def extractMatchedVideoEstimated(self, start, end):
	cur, conn = self.connectMySQL()
	db = self.connectMongo()
	c = 0
	for e in db.matchedVideoEstimated.find({"report_at":{"$gte":start, "$lt":end}}):    
	    trackingMeta_id = e["trackingMeta_id"]
	    matchedVideo_id = e["matchedVideo_id"]
	    trackingWebsite_id = e["trackingWebsite_id"]
	    website_type = e["website_type"] 
	    estimated_viewcount = round(e["estimated_viewcount"],8)
	    report_at = str(e["report_at"])
	    sql = "insert into matchedVideoEstimated values(%s,%s,%s,'%s','%s',%s);" \
			%(matchedVideo_id,trackingMeta_id,trackingWebsite_id,website_type,\
			report_at,estimated_viewcount)
	    c += 1
	    cur.execute(sql)
	    if c%50000 == 0:
		self.commitMySQL(conn)
		#cur, conn = self.connectMySQL()
		print c
	self.commitMySQL(conn)
	self.closeMySQL(cur, conn)

    def extractMatchedVideoViewCompletion(self, start, end):
	cur, conn = self.connectMySQL()
        db = self.connectMongo()
	c = 0
	err = 0
        for e in db.matchedVideoViewCompletion.find({"report_at":{"$gte":start, "$lt":end}}):
            trackingMeta_id = e["trackingMeta_id"]
            matchedVideo_id = e["matchedVideo_id"]
            trackingWebsite_id = e["trackingWebsite_id"]
            website_type = "UGC"
	    report_at = str(e["report_at"])
            view_count = e["view_count"]
	    sql = "insert into matchedVideoViewCompletion values(%s,%s,%s,'%s','%s',%s);" \
			%(matchedVideo_id,trackingMeta_id,trackingWebsite_id,website_type,report_at,view_count)
	    c += 1
	    try:
	        cur.execute(sql)
	    except Exception,e:
	        err += 1
		continue
	    if c%50000 == 0:
		self.commitMySQL(conn)
                #cur, conn = self.connectMySQL()
                print "count, err = ", c, err
	print err
	self.commitMySQL(conn)
	self.closeMySQL(cur, conn)

def main():
    db = DBOperation()
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end =  datetime.strptime(end_date, '%Y-%m-%d')
    db.extractMatchedVideoEstimated(start, end)
    db.extractMatchedVideoViewCompletion(start, end)

if __name__=="__main__":
    main()
