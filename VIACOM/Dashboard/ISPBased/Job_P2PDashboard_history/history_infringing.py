#!/usr/bin/env python
# coding: utf-8
# author: suncong
# date: 2015-08-10

import os
import MySQLdb
import datetime
import time
from MySQLHelper import MySQLHelper

tracker2_host = "eqx-vtweb-subordinate-db"
tracker2_user = "kettle"
tracker2_pass = "k3UTLe"
tracker2_db = "tracker2"

p2pwarehouse_host = "dna-610"
p2pwarehouse_user = "report"
p2pwarehouse_pass = "report"
p2pwarehouse_db = "p2pArchViacom"

DASHBOARD_host = "54.67.114.123"
DASHBOARD_user = "kettle"
DASHBOARD_pass = "kettle"
DASHBOARD_db = "VIACOM_DASHBOARD"

def loadingDaily(ETL_DTE):

    print datetime.datetime.now()
    retryTime = 0
    while retryTime < 10:
	try:
	    print "retryTime:"
            print retryTime
            pre_DASHBOARD = MySQLdb.connect(DASHBOARD_host, DASHBOARD_user, DASHBOARD_pass, DASHBOARD_db)
    	    cursor = pre_DASHBOARD.cursor()
	    break
	except Exception,e:
	    print e
	    retryTime += 1
    cursor.execute("truncate table infringingP2P_Tmp")
    #cursor.execute("truncate table allP2P_Tmp")
    cursor.execute("drop index meta_isp on infringingP2P_Tmp")
    #cursor.execute("drop index meta_isp on allP2P_Tmp")
    pre_DASHBOARD.close()
    print "truncate finished!"

    
    retrytracker2 = 0
    while retrytracker2 < 10:
        try:
            print "retrytracker2:"
            print retrytracker2
            tracker2 = MySQLHelper(tracker2_host,tracker2_user,tracker2_pass,tracker2_db)
            break
        except Exception,e:
            print e
            retrytracker2 += 1
    #tracker2 = MySQLHelper(tracker2_host,tracker2_user,tracker2_pass,tracker2_db)
    print datetime.datetime.now()
    infP2P_Tmp_select = """SELECT a.meta_id, a.isp_id, c.key_id AS protocol_hash, count(a.IP) AS infringingIPs FROM tracker2.takedownNoticeItemP2PDetail AS a, tracker2.matchedVideoP2PItem AS b, tracker2.matchedVideo AS c WHERE a.company_id = 14 AND b.company_id = 14 AND c.company_id = 14 AND a.matchedVideoP2PItem_id = b.id AND b.matchedVideo_id = c.id AND a.first_notice_send_time >= DATE_SUB('%s 08:00:00', INTERVAL +1 DAY) AND a.first_notice_send_time < DATE_SUB('%s 08:00:00', INTERVAL 0 DAY) GROUP BY a.meta_id, a.isp_id, c.key_id""" %(ETL_DTE,ETL_DTE)
    print infP2P_Tmp_select
    infP2P_result = tracker2.query_sql_cmd(infP2P_Tmp_select)
    print "inf fetched"
    retryVIADASHBOARD = 0
    while retryVIADASHBOARD < 10:
        try:
            print "retryVIADASHBOARD:"
	    print retryVIADASHBOARD
    	    VIACOM_DASHBOARD = MySQLHelper(DASHBOARD_host, DASHBOARD_user, DASHBOARD_pass, DASHBOARD_db)
            break
        except Exception,e:
	    print e
	    retryVIADASHBOARD += 1
    infP2P_Tmp_insert = "insert into infringingP2P_Tmp " + " values " + str(infP2P_result)[1:-1].replace("L", "").replace("u","")
    VIACOM_DASHBOARD.insert_sql_cmd(infP2P_Tmp_insert)   
    print datetime.datetime.now()



    retry114 = 0
    while retry114 < 10:
        try:
            print "retry114:"
            print retry114
            backup114 = MySQLHelper("192.168.110.114","kettle","k3UTLe","DM_VIACOM_TEST")
            break
        except Exception,e:
            print e
            retry114 += 1
    #tracker2 = MySQLHelper(tracker2_host,tracker2_user,tracker2_pass,tracker2_db)
    print datetime.datetime.now()
    infP2Pbackup_Tmp_select = """SELECT a.meta_id, a.isp_id, b.key_id AS protocol_hash, count(a.IP) AS infringingIPs FROM backup_takedownNoticeItemP2PDetail_20160324 AS a, infback_Tmp AS b WHERE a.matchedVideoP2PItem_id = b.id AND a.first_notice_send_time >= DATE_SUB('%s 08:00:00',INTERVAL + 1 DAY) AND a.first_notice_send_time < DATE_SUB('%s 08:00:00', INTERVAL 0 DAY) GROUP BY a.meta_id , a.isp_id , b.key_id""" %(ETL_DTE,ETL_DTE)
    print infP2Pbackup_Tmp_select
    infP2Pbackup_result = backup114.query_sql_cmd(infP2Pbackup_Tmp_select)
    print "infbackup fetched"
    print datetime.datetime.now()
    retryVIADASHBOARD1 = 0
    while retryVIADASHBOARD1 < 10:
        try:
            print "retryVIADASHBOARD1:"
            print retryVIADASHBOARD1
            VIACOM_DASHBOARD1 = MySQLHelper(DASHBOARD_host, DASHBOARD_user, DASHBOARD_pass, DASHBOARD_db)
            break
        except Exception,e:
            print e
            retryVIADASHBOARD1 += 1
    infP2Pbackup_Tmp_insert = "insert into infringingP2P_Tmp " + " values " + str(infP2Pbackup_result)[1:-1].replace("L", "").replace("u","")
    VIACOM_DASHBOARD1.insert_sql_cmd(infP2Pbackup_Tmp_insert)
    print datetime.datetime.now()

    '''retryTime_ware = 0
    while retryTime_ware < 10:
        try:
            print "retryTime_ware:"
	    print retryTime_ware
	    p2pwarehouse = MySQLHelper(p2pwarehouse_host, p2pwarehouse_user, p2pwarehouse_pass, p2pwarehouse_db)
	    break
        except Exception,e:
            print e
   	    retryTime_ware += 1
   # sql_set_time_zone = """set time_zone = '-8:00';"""
   # db1.query_sql_cmd(sql_set_time_zone)
   # print sql_set_time_zone 
    allP2P_Tmp_select = """SELECT a.trackingMeta_id AS meta_id, a.isp_id, a.protocol_hash, count(a.peer_ip_address) AS allIPs FROM p2pArchViacom.infringmentSummary20150910 AS a WHERE a.created_at >= DATE_SUB('%s', INTERVAL +1 DAY) AND a.created_at < DATE_SUB('%s', INTERVAL 0 DAY) GROUP BY a.trackingMeta_id, a.isp_id, a.protocol_hash;""" %(ETL_DTE,ETL_DTE)
    print allP2P_Tmp_select    
    allP2P_result = p2pwarehouse.query_sql_cmd(allP2P_Tmp_select)
    print "all fetched"
    #print len(Res)
    allP2P_Tmp_insert = "insert into allP2P_Tmp " + " values " + str(allP2P_result)[1:-1].replace("L", "").replace("u","")
    VIACOM_DASHBOARD.insert_sql_cmd(allP2P_Tmp_insert)
    print datetime.datetime.now()'''

    
    retryTime_afterdashboard = 0
    while retryTime_afterdashboard < 10:
	try:
	    print "retryTime_afterdashboard:"
            print retryTime_afterdashboard
            after_DASHBOARD = MySQLdb.connect(DASHBOARD_host, DASHBOARD_user, DASHBOARD_pass, DASHBOARD_db)
            cursor = after_DASHBOARD.cursor()
	    break
        except Exception,e:
	    print e
            retryTime_afterdashboard +=1   
    cursor.execute("create index meta_isp on infringingP2P_Tmp(meta_id, isp_id)")
    #cursor.execute("create index meta_isp on allP2P_Tmp(meta_id, isp_id)")
    infinsert = """INSERT INTO ISPBased SELECT DATE_SUB('%s', INTERVAL +1 DAY) AS dateID, b.meta_title AS title, max(b.priority_type) AS tier, c.isp_name AS isp, d.country_name AS country, CASE WHEN c.isp_name LIKE '%%Comcast%%' OR c.isp_name LIKE '%%Verizon%%' OR c.isp_name LIKE '%%AT&T%%' OR c.isp_name LIKE '%%SBC%%' OR c.isp_name LIKE '%%Cablevision%%' OR c.isp_name LIKE '%%Road Runner%%' OR c.isp_name LIKE '%%Time Warner%%' THEN 1 ELSE 0 END AS CASFlag, CASE WHEN d.id = 233 THEN 1 ELSE 0 END AS USFlag, 1 AS infringingFlag, sum(a.infringingIPs) AS IPs, count(DISTINCT a.protocol_hash) AS hashes, CURRENT_TIMESTAMP AS ETL_DTE FROM VIACOM_DASHBOARD.infringingP2P_Tmp AS a, VIACOM_DASHBOARD.meta_Tmp AS b, VIACOM_DASHBOARD.isp_Tmp AS c, VIACOM_DASHBOARD.country_Tmp AS d WHERE a.meta_id = b.id AND a.isp_id = c.id AND c.country_id = d.id GROUP BY b.meta_title, c.isp_name, d.country_name, CASE WHEN c.isp_name LIKE '%%Comcast%%' OR c.isp_name LIKE '%%Verizon%%' OR c.isp_name LIKE '%%AT&T%%' OR c.isp_name LIKE '%%SBC%%' OR c.isp_name LIKE '%%Cablevision%%' OR c.isp_name LIKE '%%Road Runner%%' OR c.isp_name LIKE '%%Time Warner%%' THEN 1 ELSE 0 END, CASE WHEN d.id = 233 THEN 1 ELSE 0 END""" %ETL_DTE 
    print infinsert
    cursor.execute(infinsert)
    after_DASHBOARD.commit()
    print "infringing finished"
    print datetime.datetime.now()
    '''allinsert = """INSERT INTO ISPBased SELECT DATE_SUB('%s', INTERVAL +1 DAY) AS dateID, b.meta_title AS title, max(b.priority_type) AS tier, c.isp_name AS isp, d.country_name AS country, CASE WHEN c.isp_name LIKE '%%Comcast%%' OR c.isp_name LIKE '%%Verizon%%' OR c.isp_name LIKE '%%AT&T%%' OR c.isp_name LIKE '%%SBC%%' OR c.isp_name LIKE '%%Cablevision%%' OR c.isp_name LIKE '%%Road Runner%%' OR c.isp_name LIKE '%%Time Warner%%' THEN 1 ELSE 0 END AS CASFlag, CASE WHEN d.id = 233 THEN 1 ELSE 0 END AS USFlag, 0 AS infringingFlag, sum(a.allIPs) AS IPs, count(DISTINCT a.protocol_hash) AS hashes, CURRENT_TIMESTAMP AS ETL_DTE FROM VIACOM_DASHBOARD.allP2P_Tmp AS a, VIACOM_DASHBOARD.meta_Tmp AS b, VIACOM_DASHBOARD.isp_Tmp AS c, VIACOM_DASHBOARD.country_Tmp AS d WHERE a.meta_id = b.id AND a.isp_id = c.id AND c.country_id = d.id GROUP BY b.meta_title, c.isp_name, d.country_name, CASE WHEN c.isp_name LIKE '%%Comcast%%' OR c.isp_name LIKE '%%Verizon%%' OR c.isp_name LIKE '%%AT&T%%' OR c.isp_name LIKE '%%SBC%%' OR c.isp_name LIKE '%%Cablevision%%' OR c.isp_name LIKE '%%Road Runner%%' OR c.isp_name LIKE '%%Time Warner%%' THEN 1 ELSE 0 END, CASE WHEN d.id = 233 THEN 1 ELSE 0 END""" %ETL_DTE   
    print allinsert
    cursor.execute(allinsert)
    after_DASHBOARD.commit()
    print "all finished"
    print datetime.datetime.now()'''
    after_DASHBOARD.close()


def main():
    step = 0
    while True:
    	dat=time.strftime('%Y-%m-%d', time.localtime(1438934378.917212 - step * 24 * 60 * 60))     
   	print dat
	step += 1
	print step
	#d = time.strftime('%Y-%m-%d',datetime.datetime(2016, 2, 1))
	#print d
    	if '2015-03-01' == dat:
	    break
	try:
	    loadingDaily(dat)
	except Exception,e:
	    print e   


if __name__=="__main__":
    main()
