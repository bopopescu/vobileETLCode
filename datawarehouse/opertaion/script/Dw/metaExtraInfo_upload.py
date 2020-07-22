#!/usr/bin/env python
# coding: utf-8

import os
import sys 
import datetime
import bigquery
import ConfigParser
sys.path.append("/Job/Test/service")
from connectService import ConnectionService, MailService
reload(sys) 
sys.setdefaultencoding('utf8')

def upload_to_gs(db):
    beg = datetime.date(2015,01,01)
    end = datetime.date(2016,04,27)
    for i in xrange((end- beg).days+1):
	date = str(end - datetime.timedelta(days=i))
	date1 = str(end - datetime.timedelta(days=i-1)) 
	f = "metaExtraInfo%s.csv" %date.replace("-","")
        cmd_csv = """mysql -heqx-vtweb-subordinate-db -ukettle -pk3UTLe tracker2 -e"select * from metaExtraInfo where created_at >= '%s' and created_at < '%s';" >> %s""" %(date, date1, f)
        print cmd_csv
	cmd_upload = "gsutil cp %s gs://vobile-data-analysis/OperationalMetrics/metaExtraInfo" %f
	os.system(cmd_csv)
	os.system(cmd_upload)
	os.system("rm %s"%f)

def main():
     conf = ConfigParser.ConfigParser()
     conf.read("/Job/datawarehouse/upload_data/conf/conf")
     db=ConnectionService(conf.get("db_conf","ip"), conf.get("db_conf","user"), conf.get("db_conf","passwd"), "tracker2")
     upload_to_gs(db)

if __name__=="__main__":
     main()




