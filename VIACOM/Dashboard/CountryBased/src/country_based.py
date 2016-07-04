#!/usr/bin/env python
# coding: utf-8
# author: suncong
# date: 2016-03-01
 
import sys 
import time
import datetime
import traceback
import logging
import MySQLdb
import ConfigParser

conf = ConfigParser.ConfigParser()
conf.read("/Job/VIACOM/Dashboard/CountryBased/conf/db.conf")

class CountryBased(object):
    def __init__(self):
   	pass
 
    def connectTargetDB(self):
        try:
            conn = MySQLdb.connect(host = conf.get("db_online_conf", "ip"), user = conf.get("db_online_conf", "user"),  \
			passwd = conf.get("db_online_conf", "passwd"), db = "VIACOM_DASHBOARD")
            cur = conn.cursor()
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return cur, conn

    def closeMySQL(self, cur, conn):
        cur.close()
        conn.commit()
        conn.close()
	
    def ETL_old(self, report_date):
	try:
            cur,conn = self.connectTargetDB()
	    print "delete from CountryBased where report_date = '%s';"%report_date
            cur.execute("delete from CountryBased where report_date = '%s';"%report_date)
            sql = """select hostCountry, title, b.nationalFlag, sum(allMatch), sum(infringing), \
                     sum(allIPs), sum(infringingIPs) \
                     from (select hostCountry, title, matchedNum+infringingNumCMS as allMatch, \
                     infringingNum+infringingNumCMS as infringing, 0 as allIPs, 0 as infringingIPs from \
                     SiteBased where reportDate = '%s' union all select country, title, 0 as allMatch, 0 as infringing, \
                     sum(case when infringingFlag = 0 then IPs else 0 end) as allIPs, sum(case when infringingFlag = 1 \
                     then IPs else 0 end) as infringingIPs from ISPBased where dateID = '%s' group by 1,2) a  left join \
                     (select nationalFlag,countryName from TitleBasedCountry where countryName <> 'unknown') b \
                     on a.hostCountry = b.countryName group by 1,2 order by 2 desc;"""%(report_date, report_date)
            cur.execute(sql)
            res = cur.fetchall()
	    print len(res)
            for e in res:
                insertSql = """insert into CountryBased values("%s","%s","%s","%s",%s,%s,%s,%s,current_timestamp());"""  \
                    %(report_date, e[0], e[1], e[2],e[3],e[4],e[5],e[6])
                #print insertSql
                cur.execute(insertSql) 
            self.closeMySQL(cur,conn)
        except Exception,e:
            print traceback.format_exc()
            logging.error(traceback.format_exc())
        print report_date, " finished ", len(res)

    def ETL(self, report_date):
	try:
	    cur,conn = self.connectTargetDB()
	    cur.execute("delete from CountryBased where report_date = '%s';"%report_date)
	    sql = """select "%s", hostCountry, title, b.nationalFlag, sum(allMatch), sum(infringing), \
		     sum(allIPs), sum(infringingIPs),current_timestamp() \
		     from (select hostCountry, title, matchedNum+infringingNumCMS as allMatch, \
		     infringingNum+infringingNumCMS as infringing, 0 as allIPs, 0 as infringingIPs from \
		     SiteBased where reportDate = '%s' union all select country, title, 0 as allMatch, 0 as infringing, \
		     sum(case when infringingFlag = 0 then IPs else 0 end) as allIPs, sum(case when infringingFlag = 1 \
		     then IPs else 0 end) as infringingIPs from ISPBased where dateID = '%s' group by 1,2) a  left join \
		     (select nationalFlag,countryName from TitleBasedCountry where countryName <> 'unknown') b \
		     on a.hostCountry = b.countryName group by 2,3 order by 2 desc;"""%(report_date, report_date, report_date)
	    cur.execute(sql)
	    res = cur.fetchall()
	    insertSql =  """INSERT INTO CountryBased values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
	    cur.executemany(insertSql, res)
	    self.closeMySQL(cur,conn)
	except Exception,e:
	    print traceback.format_exc()
            logging.error(traceback.format_exc())
	print report_date, " finished ", len(res)

    def ETL_history(self):
	beg = datetime.date(2015,3,1)
	end = datetime.date(2016,4,13)
	for i in xrange((end- beg).days+1):
	    date = str(end - datetime.timedelta(days=i))
	    self.ETL(date)

def main():
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='/Job/VIACOM/Dashboard/CountryBased/log/countryBased.log',
                filemode='a')
    report_date = str(datetime.date.today() - datetime.timedelta(days=1))
    c = CountryBased()
    c.ETL(report_date)
    #c.ETL_history()

if __name__=="__main__":
    main()

