#!/usr/bin/env python
# coding: utf-8
# author: duli
# date: 2016-03-26

import os
import MySQLdb
import datetime
import time
from MySQLHelper import MySQLHelper

tracker2_host = "eqx-vtweb-subordinate-db"
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
    cursor.execute("truncate table infringingP2P_Tmp;truncate table allP2P_Tmp;drop index meta_isp on infringingP2P_Tmp;drop index meta_isp on allP2P_Tmp")
    #truncate table meta_Tmp;truncate table isp_Tmp;drop index meta_isp on infringingP2P_Tmp;drop index meta_isp on allP2P_Tmp)
    cursor.close()
    #DASHBOARD.commit()
    print "truncate finished!"


    #fetch the VTWeb infringing IPs and hashes begin
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
    set_NamesUTF8 = """SET NAMES UTF8"""
    print set_NamesUTF8
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
    #fetch the VTWeb infringing IPs and hashes end
    
    """meta_Tmp_select = SELECT a.id, a.meta_title, b.priority_type FROM mddb.meta AS a, tracker2.metaExtraInfo AS b WHERE a.company_id = 14 AND b.company_id = 14 AND a.id = b.meta_id
    #cursorMeta = Dimension.cursor()
    #cursorMeta.execute(set_NamesUTF8)
    #cursorMeta.execute(meta_Tmp_select)
    #meta = cursorMeta.fetchall()
    meta = tracker2.query_sql_cmd(meta_Tmp_select)
    print "meta fetched"
    meta_insert = "insert into meta_Tmp " + " values " + str(meta)[1:-1].replace("0L", "0").replace("1L", "1").replace("2L", "2").replace("3L", "3").replace("4L", "4").replace("5L", "5").replace("6L", "6").replace("7L", "7").replace("8L", "8").replace("9L", "9").replace("u'","'").replace('u"','"')
    VIACOM_DASHBOARD.insert_sql_cmd(meta_insert)
    print "meta finished"
    #cursorMeta.close()

    isp_Tmp_select = "select id, country_id, isp_name from tracker2.isp"
    #cursorIsp = Dimension.cursor()
    #cursorIsp.execute(set_NamesUTF8)
    #cursorIsp.execute(isp_Tmp_select)
    #isp = cursorIsp.fetchall()
    isp = tracker2.query_sql_cmd(isp_Tmp_select)
    print "isp fetched"
    isp_insert = "insert into isp_Tmp " + " values " + str(isp)[1:-1].replace("0L", "0").replace("1L", "1").replace("2L", "2").replace("3L", "3").replace("4L", "4").replace("5L", "5").replace("6L", "6").replace("7L", "7").replace("8L", "8").replace("9L", "9").replace("u'","'").replace('u"','"')
    VIACOM_DASHBOARD.insert_sql_cmd(isp_insert)
    print "isp finished"
    #cursorIsp.close()

    country_Tmp_select = "select id, country_name from tracker2.country"
    #cursorCountry = Dimension.cursor()
    #cursorCountry.execute(set_NamesUTF8)
    #cursorCountry.execute(country_Tmp_select)
    #country = cursorCountry.fetchall()
    country = tracker2.query_sql_cmd(country_Tmp_select)
    #print country
    print "country fetched"
    country_insert = "insert into country_Tmp " + " values " + str(country)[1:-1].replace("0L", "0").replace("1L", "1").replace("2L", "2").replace("3L", "3").replace("4L", "4").replace("5L", "5").replace("6L", "6").replace("7L", "7").replace("8L", "8").replace("9L", "9").replace("u'","'").replace('u"','"')
    VIACOM_DASHBOARD.insert_sql_cmd(country_insert)
    print "country finished"
    #cursorCountry.close()
    #Dimension.close()"""


    #fetch the p2pwarehouse all IPs and hashes begin
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
    allP2P_Tmp_select = """SELECT a.trackingMeta_id AS meta_id, a.isp_id, a.protocol_hash, count(a.peer_ip_address) AS allIPs FROM p2pwarehouse.infringmentSummary AS a WHERE a.created_at >= DATE_SUB('%s 08:00:00', INTERVAL +1 DAY) AND a.created_at < DATE_SUB('%s 08:00:00', INTERVAL 0 DAY) GROUP BY a.trackingMeta_id, a.isp_id, a.protocol_hash;""" %(ETL_DTE,ETL_DTE)
    print allP2P_Tmp_select    
    allP2P_result = p2pwarehouse.query_sql_cmd(allP2P_Tmp_select)
    print "all fetched"
    #print len(Res)
    allP2P_Tmp_insert = "insert into allP2P_Tmp " + " values " + str(allP2P_result)[1:-1].replace("L", "").replace("u","")
    VIACOM_DASHBOARD.insert_sql_cmd(allP2P_Tmp_insert)
    print datetime.datetime.now()
    #fetch the p2pwarehouse all IPs and hashes end

    
    cursor = DASHBOARD.cursor()
    cursor.execute("create index meta_isp on infringingP2P_Tmp(meta_id, isp_id);create index meta_isp on allP2P_Tmp(meta_id, isp_id)")
    cursor.close()
    print "index created"
    #insert the infringing IPs and hashes to the target table
    infinsert = """INSERT INTO ISPBased SELECT DATE_SUB('%s', INTERVAL + 1 DAY) AS dateID, ifnull(e.mapTitle, b.meta_title) AS title, max(b.priority_type) AS tier, c.isp_name AS isp, d.countryName AS country, CASE WHEN c.isp_name LIKE '%%Comcast%%' OR c.isp_name LIKE '%%Verizon%%' OR c.isp_name LIKE '%%AT&T%%' OR c.isp_name LIKE '%%SBC%%' OR c.isp_name LIKE '%%Cablevision%%' OR c.isp_name LIKE '%%Road Runner%%' OR c.isp_name LIKE '%%Time Warner%%' THEN 1 ELSE 0 END AS CASFlag, CASE WHEN d.country_id = 233 THEN 1 ELSE 0 END AS USFlag, 1 AS infringingFlag, sum(a.infringingIPs) AS IPs, count(DISTINCT a.protocol_hash) AS hashes, CURRENT_TIMESTAMP AS ETL_DTE FROM VIACOM_DASHBOARD.infringingP2P_Tmp AS a, VIACOM_DASHBOARD.meta_Tmp AS b LEFT JOIN VIACOM_DASHBOARD.MetaTitleMapTitle AS e ON b.meta_title = e.metaTitle, VIACOM_DASHBOARD.isp_Tmp AS c, VIACOM_DASHBOARD.TitleBasedCountry AS d WHERE a.meta_id = b.id AND a.isp_id = c.id AND c.country_id = d.country_id GROUP BY ifnull(e.mapTitle, b.meta_title) , c.isp_name , d.countryName , CASE WHEN c.isp_name LIKE '%%Comcast%%' OR c.isp_name LIKE '%%Verizon%%' OR c.isp_name LIKE '%%AT&T%%' OR c.isp_name LIKE '%%SBC%%' OR c.isp_name LIKE '%%Cablevision%%' OR c.isp_name LIKE '%%Road Runner%%' OR c.isp_name LIKE '%%Time Warner%%' THEN 1 ELSE 0 END , CASE WHEN d.country_id = 233 THEN 1 ELSE 0 END""" %ETL_DTE
    print infinsert
    cursorinf = DASHBOARD.cursor()
    cursorinf.execute(infinsert)
    DASHBOARD.commit()
    cursorinf.close()
    print "infringing finished"
    print datetime.datetime.now()

    #insert the all IPs and hashes to the target table
    allinsert = """INSERT INTO ISPBased SELECT DATE_SUB('%s', INTERVAL + 1 DAY) AS dateID, ifnull(e.mapTitle, b.meta_title) AS title, max(b.priority_type) AS tier, c.isp_name AS isp, d.countryName AS country, CASE WHEN c.isp_name LIKE '%%Comcast%%' OR c.isp_name LIKE '%%Verizon%%' OR c.isp_name LIKE '%%AT&T%%' OR c.isp_name LIKE '%%SBC%%' OR c.isp_name LIKE '%%Cablevision%%' OR c.isp_name LIKE '%%Road Runner%%' OR c.isp_name LIKE '%%Time Warner%%' THEN 1 ELSE 0 END AS CASFlag, CASE WHEN d.country_id = 233 THEN 1 ELSE 0 END AS USFlag, 0 AS infringingFlag, sum(a.allIPs) AS IPs, count(DISTINCT a.protocol_hash) AS hashes, CURRENT_TIMESTAMP AS ETL_DTE FROM VIACOM_DASHBOARD.allP2P_Tmp AS a, VIACOM_DASHBOARD.meta_Tmp AS b left join VIACOM_DASHBOARD.MetaTitleMapTitle as e on b.meta_title = e.metaTitle, VIACOM_DASHBOARD.isp_Tmp AS c, VIACOM_DASHBOARD.TitleBasedCountry AS d WHERE a.meta_id = b.id AND a.isp_id = c.id AND c.country_id = d.country_id GROUP BY ifnull(e.mapTitle, b.meta_title), c.isp_name, d.countryName, CASE WHEN c.isp_name LIKE '%%Comcast%%' OR c.isp_name LIKE '%%Verizon%%' OR c.isp_name LIKE '%%AT&T%%' OR c.isp_name LIKE '%%SBC%%' OR c.isp_name LIKE '%%Cablevision%%' OR c.isp_name LIKE '%%Road Runner%%' OR c.isp_name LIKE '%%Time Warner%%' THEN 1 ELSE 0 END , CASE WHEN d.country_id = 233 THEN 1 ELSE 0 END""" %ETL_DTE
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
        dat = time.strftime('%Y-%m-%d', time.localtime(time.time() - step * 24 * 60 * 60))
        print dat
        step += 1
        print step
        if time.strftime('%Y-%m-%d', time.localtime(time.time() - 1 * 24 * 60 * 60)) == dat:
            break
        try:
            loadingDaily(dat)
        except Exception,e:
            print e


if __name__=="__main__":
    main()
