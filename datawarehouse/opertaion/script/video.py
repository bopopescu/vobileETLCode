#!/usr/bin/python
#coding=utf-8:

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

#stop = 30
nameColumn = ''


column_name="SELECT column_name FROM information_schema.columns WHERE table_schema = 'DW_VTMetrics' AND table_name = 'video'"
get_column = cur.execute(column_name)
column_list = cur.fetchall()

for get_name = "select " + nameColumn + " from video union "

#print get_name
get_data = "select * from video"
get_all = get_name + get_data
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



