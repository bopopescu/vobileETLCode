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

get_all = "select '网站名', '网站domain', '网站类型', 'trackingWebsiteId', 'URL', 'key_id', '来源', '分数', 'videoId', 'trackingMetaId', '匹配情况' from video union select c.website_name, c.website_domain, c.website_type, a.trackingWebsite_id, a.clip_url, a.key_id, a.source_type, a.rating_score, b.video_id, b.trackingMeta_id, d.related_status from video a, video_trackingMeta b, trackingWebsite c, matchedVideo d where a.id = b.video_id and a.trackingWebsite_id = c.id and a.id = d.video_id "
print get_all

data = cur.execute(get_all)
data_list = cur.fetchall()

cur.close()
conn.close()

wb = Workbook()
ws = wb.worksheets[0]
ws.title = 'VT Operation'
#ws.column_dimensions["A"].width = 35.0
ew = ExcelWriter(workbook = wb)

file_name = r'/Job/datawarehouse/opertaion/mailsender/data/DW测试.xlsx'

rows = len(data_list)
cols = len(data_list[0])

for rx in range(rows):
   for cx in range(cols):
     ws.cell(row = rx + 1, column = cx + 1).value = data_list[rx][cx]

ew.save(filename = file_name)


