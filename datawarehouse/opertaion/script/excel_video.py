#!/usr/bin/python
#encoding=utf8

import MySQLdb
import sys
import time
import datetime
import os
from openpyxl.workbook import Workbook
from openpyxl.writer.excel import ExcelWriter
#import mysql.connector 
reload(sys)
sys.setdefaultencoding('utf8')
conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('DW_VTMetrics')
cur=conn.cursor()
cur.execute("SET NAMES UTF8")
get_all = "select * from video"
print get_all

data = cur.execute(get_all)
data_list = cur.fetchall()

cur.close()
conn.close()

wb = Workbook()
ws = wb.worksheets[0]
ws.title = 'video'
#ws.column_dimensions["A"].width = 35.0
ew = ExcelWriter(workbook = wb)

file_name = r'/Job/datawarehouse/opertaion/mailsender/data/video.xlsx'

rows = len(data_list)
cols = len(data_list[0])

for rx in range(rows):
   for cx in range(cols):
     ws.cell(row = rx + 1, column = cx + 1).value = data_list[rx][cx]

ew.save(filename = file_name)


