#!/usr/bin/env python
# coding: utf-8
# author: suncong
# date: 2016-03-01
 
import sys 
import time
import datetime
import random
import MySQLdb
import ConfigParser

conf = ConfigParser.ConfigParser()
conf.read("/Job/executiveDashboard/conf/db.conf")

class CPU(object):
    def __init__(self, coreNum):
	self.coreNum = coreNum
    
    def connectTargetDB(self):
        try:
            conn = MySQLdb.connect(host = conf.get("db_online_conf", "ip"), user = conf.get("db_online_conf", "user"),  \
			passwd = conf.get("db_online_conf", "passwd"), db = "DM_EDASHBOARD")
            cur = conn.cursor()
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
	    self.connectTargetDB()
        return cur, conn

    def closeMySQL(self, cur, conn):
            cur.close()
            conn.commit()
            conn.close()
	
    def ec2_mechine_info(self):
	report_date = str(datetime.date.today() - datetime.timedelta(days=1))
	cur,conn = self.connectTargetDB()
	cur.execute("delete from ec2_mechine_info_Tmp where report_date = '%s';" %report_date)
	f = open("/Job/executiveDashboard/Tmp_file/ec2.list","r")
	for line in f.readlines():
	    line = line.strip("\n").split("\t")
	    line[5] ="ip-" + line[5].replace(".","-")
	    if "VDDB" in line[4]:
		line[4] = "VDDB"
	    elif "Taisan" in line[4] and "Crawler" in line[4]:
		line[4] = "Taisan"
	    insertSql = "insert ec2_mechine_info_Tmp values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',current_timestamp())" \
			%(report_date,line[0],line[1],line[2],line[3],line[4],line[5],line[6],'0000-00-00','0000-00-00')
	    print insertSql
	    cur.execute(insertSql)
	self.closeMySQL(cur,conn)

    def stat_core_num(self):
	core_num_vt, core_num_vtx, core_num_reclaim = (0, 0, 0)
	report_date = str(datetime.date.today() - datetime.timedelta(days=1))
	f = open("/Job/executiveDashboard/Tmp_file/ec2.list","r")
	for line in f.readlines():
	    line = line.strip("\n").split("\t")
	    if "VTX" in line[4]:
		core_num_vtx += self.coreNum[line[3].split(".")[1]]
	    elif "VT" in line[4] and "VTX" not in line[4]:
		core_num_vt += self.coreNum[line[3].split(".")[1]]
	    elif "reclaim" in line[4]:
		core_num_reclaim += self.coreNum[line[3].split(".")[1]]
	print "vt ", core_num_vt
	print "vtx ", core_num_vtx
	print "reclaim ", core_num_reclaim

    def stat_cpu_rate(self):
	report_date = str(datetime.date.today() - datetime.timedelta(days=1))
	cur,conn = self.connectTargetDB()
	conn = MySQLdb.connect(host = conf.get("db_conf", "ip"), user = conf.get("db_conf", "user"),  \
                        passwd = conf.get("db_conf", "passwd"), db = "DM_EDASHBOARD")
        cur = conn.cursor()
	sql_vddb = "select sum(a.data_value)/count(1) from d_monitor_metric a, ec2_mechine_info_Tmp b \
		where b.service = 'Taisan' and a.endpoint = b.ip_inner and a.gmt_create = '%s' and a.data_value <= 100;" %report_date
	sql_taisan = "select sum(a.data_value)/count(1) from d_monitor_metric a, ec2_mechine_info_Tmp b \
		where b.service = 'VDDB' and a.endpoint = b.ip_inner and a.gmt_create = '%s' and a.data_value <= 100;" %report_date
	cur.execute(sql_vddb)
	res_vddb = cur.fetchall()
	cur.execute(sql_taisan)
	res_taisan = cur.fetchall()
	print "vddb: ", res_vddb
	print "taisan: ", res_taisan

def main():
    coreNumDict = {
	"micro" : 1,
	"small" : 1,
	"nano" : 1,
	"medium" : 1,
	"large" : 2,
	"xlarge" : 4,
	"2xlarge" : 8,
	"4xlarge" : 16,
	"8xlarge" : 32,
	"10xlarge" : 40
    }
    c = CPU(coreNumDict)
    #c.ec2_mechine_info()
    c.stat_cpu_rate()
    c.stat_core_num()

if __name__=="__main__":
    main()

