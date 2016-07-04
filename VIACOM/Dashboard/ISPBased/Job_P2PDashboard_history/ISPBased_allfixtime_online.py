#!/usr/bin/env python
# coding: utf-8
# author: duli
# date: 2016-03-26

import os
import MySQLdb
import datetime
import time
from MySQLHelper import MySQLHelper

tracker2_host = "eqx-vtweb-slave-db"
tracker2_user = "kettle"
tracker2_pass = "k3UTLe"
tracker2_db = "tracker2"

p2pwarehouse_host = "p2p-3-replica-02.c85gtgxi0qgc.us-west-1.rds.amazonaws.com"
p2pwarehouse_user = "report"
p2pwarehouse_pass = "report"
p2pwarehouse_db = "p2pwarehouse"

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
            DASHBOARD = MySQLdb.connect(DASHBOARD_host, DASHBOARD_user, DASHBOARD_pass, DASHBOARD_db)
            cursor = DASHBOARD.cursor()
            break
        except Exception,e:
            print e
            retryTime += 1
    cursor.execute("truncate table allP2P_Tmp;drop index meta_isp on allP2P_Tmp")
    cursor.close()
    #DASHBOARD.commit()
    print "truncate finished!"


    retryTime_ware = 0
    while retryTime_ware < 10:
        try:
            print "retryTime_ware:"
            print retryTime_ware
            p2pwarehouse = MySQLHelper(p2pwarehouse_host, p2pwarehouse_user, p2pwarehouse_pass, p2pwarehouse_db)
            break
        except Exception,e:
            print e
            retryTime_ware += 1
    #sql_set_time_zone = """set time_zone = '-8:00';"""
    #db1.query_sql_cmd(sql_set_time_zone)
    #print sql_set_time_zone 
    allP2P_Tmp_select = """SELECT a.trackingMeta_id AS meta_id, a.isp_id, a.protocol_hash, count(a.peer_ip_address) AS allIPs FROM infringmentSummary AS a WHERE a.created_at >= DATE_SUB('%s 08:00:00', INTERVAL +1 DAY) AND a.created_at < DATE_SUB('%s 08:00:00', INTERVAL 0 DAY) GROUP BY a.trackingMeta_id, a.isp_id, a.protocol_hash;""" %(ETL_DTE,ETL_DTE)
    print allP2P_Tmp_select    
    allP2P_result = p2pwarehouse.query_sql_cmd(allP2P_Tmp_select)
    print "all fetched"
    #print len(Res)
    allP2P_Tmp_insert = "insert into allP2P_Tmp " + " values " + str(allP2P_result)[1:-1].replace("L", "").replace("u","")

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

    VIACOM_DASHBOARD.insert_sql_cmd(allP2P_Tmp_insert)
    print datetime.datetime.now()
    
    
    cursor = DASHBOARD.cursor()
    cursor.execute("create index meta_isp on allP2P_Tmp(meta_id, isp_id)")
    cursor.close()
    print "index created"

    allinsert = """INSERT INTO ISPBased_allfixtime_online SELECT DATE_SUB('%s', INTERVAL + 1 DAY) AS dateID, ifnull(e.mapTitle, b.meta_title) AS title, max(b.priority_type) AS tier, c.isp_name AS isp, d.countryName AS country, CASE WHEN c.isp_name LIKE '%%Comcast%%' OR c.isp_name LIKE '%%Verizon%%' OR c.isp_name LIKE '%%AT&T%%' OR c.isp_name LIKE '%%SBC%%' OR c.isp_name LIKE '%%Cablevision%%' OR c.isp_name LIKE '%%Road Runner%%' OR c.isp_name LIKE '%%Time Warner%%' THEN 1 ELSE 0 END AS CASFlag, CASE WHEN d.country_id = 233 THEN 1 ELSE 0 END AS USFlag, 0 AS infringingFlag, sum(a.allIPs) AS IPs, count(DISTINCT a.protocol_hash) AS hashes, CURRENT_TIMESTAMP AS ETL_DTE FROM VIACOM_DASHBOARD.allP2P_Tmp AS a, VIACOM_DASHBOARD.meta_Tmp AS b left join VIACOM_DASHBOARD.MetaTitleMapTitle as e on b.meta_title = e.metaTitle, VIACOM_DASHBOARD.isp_Tmp AS c, VIACOM_DASHBOARD.TitleBasedCountry AS d WHERE a.meta_id = b.id AND a.isp_id = c.id AND c.country_id = d.country_id GROUP BY ifnull(e.mapTitle, b.meta_title), c.isp_name, d.countryName, CASE WHEN c.isp_name LIKE '%%Comcast%%' OR c.isp_name LIKE '%%Verizon%%' OR c.isp_name LIKE '%%AT&T%%' OR c.isp_name LIKE '%%SBC%%' OR c.isp_name LIKE '%%Cablevision%%' OR c.isp_name LIKE '%%Road Runner%%' OR c.isp_name LIKE '%%Time Warner%%' THEN 1 ELSE 0 END , CASE WHEN d.country_id = 233 THEN 1 ELSE 0 END""" %ETL_DTE
    print allinsert
    cursorall = DASHBOARD.cursor()
    cursorall.execute(allinsert)
    DASHBOARD.commit()
    cursorall.close()
    print "all finished"
    print datetime.datetime.now()
    DASHBOARD.close()


def main():
    step = 0
    while True:
        dat = time.strftime('%Y-%m-%d', time.localtime(1463395880.478011 - step * 24 * 60 * 60))
        print dat
        step += 1
        print step
        if '2015-03-17' == dat:
            break
        try:
            loadingDaily(dat)
        except Exception,e:
            print e


if __name__=="__main__":
    main()
