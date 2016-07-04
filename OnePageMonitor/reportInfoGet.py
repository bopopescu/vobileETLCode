#!/usr/bin/python
import sys
import MySQLdb
import logging
import os
import commands
import re
import time

def def_report_script_status(script_run_status,script_name,run_time,err):
	try:
		script_status = script_run_status
		errInfo = err
		DATA_CENTER='"eqx"'
		hostname_output = os.popen("cat /etc/hostname").readlines()
		hostname=hostname_output[0].rstrip('\n')
		start_time = run_time
		DB_HOST_report = '206.99.94.72'
		DB_USER_report = 'ops'
		DB_PASS_report = 'pass'
		DB_NAME_report = 'DBManage'
		conn_report = MySQLdb.connect(host=DB_HOST_report, user=DB_USER_report, passwd=DB_PASS_report, db=DB_NAME_report)
		cur_report = conn_report.cursor()
		if script_status == 'running':
			sql_script_report = "replace into scriptRunTask(hostname,script_name,status,start_at) values('%s','%s','%s','%s')"%(hostname,script_name,script_status,start_time)
		else:
			sql_script_report = "update scriptRunTask set status='%s',stop_at=now() where script_name='%s' and hostname='%s' and start_at='%s' "%(script_status,script_name,hostname,start_time)	

		sql_script_reportHis = "insert into scriptRunTaskHis (hostname,script_name,status,errInfo,created_at) values(%s,%s,%s,%s,now())"
		cur_report.execute(sql_script_report)
		cur_report.execute(sql_script_reportHis,(hostname,script_name,script_status,errInfo))
		conn_report.commit()
		cur_report.close
	except Exception,err:
		print "reportInfoGet failed Exception",err

