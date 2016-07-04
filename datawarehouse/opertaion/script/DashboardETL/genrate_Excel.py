#!/usr/bin/python
#coding=utf-8

import MySQLdb
import sys
import time
import datetime
import os
from openpyxl.workbook import Workbook
from openpyxl.writer.excel import ExcelWriter
#import mysql.connector 

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('DW_VTMetrics')
cur=conn.cursor()

stop = 31
nameColumn = ''
allColumn = ''

curTime = time.strftime("%Y%m%d", time.localtime(time.time()-24*60*60))

for i in range(1,32):
  nameColumn += time.strftime("%Y%m%d", time.localtime(time.time()-stop*24*60*60)) + ','
  allColumn += "`" + time.strftime("%Y%m%d", time.localtime(time.time()-stop*24*60*60)) + "`," 
  stop = stop - 1

print stop 

allColumn = allColumn + 'Benchmark'
get_name = "select 'MetricsTitle'," + nameColumn + "'Benchmark' from ArgMetricsReportAll union "
#print get_name
get_data = "select metric_Title, %s from ArgMetricsReportAll where metric_Title not in ('%s','%s','%s','%s','%s','%s')" %(allColumn,'title','site','Compliance_Time_Linking_Site(Min)','Compliance_Time_Search_Engine(Min)','Compliance_Percent_Linking_Site','Compliance_Percent_Search_Engine')
get_all = get_name +get_data
print get_all
data = cur.execute(get_all)
data_list = cur.fetchall()

cur.close()
conn.close()

wb = Workbook()
ws = wb.worksheets[0]
ws.title = 'VTOperationMetricsReports'
ws.column_dimensions["A"].width = 35.0
ew = ExcelWriter(workbook = wb)

file_name = r'/Job/datawarehouse/opertaion/mailsender/data/VTOperationReport_AllAccount' + curTime + '.xlsx'

rows = len(data_list)
cols = len(data_list[0])

for rx in range(rows):
   for cx in range(cols):
     ws.cell(row = rx + 1, column = cx + 1).value = data_list[rx][cx]

ew.save(filename = file_name)



