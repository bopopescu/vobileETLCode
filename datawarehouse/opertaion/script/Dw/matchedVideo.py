#!/usr/bin/python

import MySQLdb
import os
import sys
import time
import datetime
import csv
import boto
import gcs_oauth2_boto_plugin
import shutil
import StringIO


#taisan Video
conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2')
cur=conn.cursor()

start = 0
stop = 1
upload = ''
#intodir = ''
delete = ''
info='/Job/datawarehouse/opertaion/script/hist/archive/'


for i in range(1,10):
  get_data = "select count(*) from matchedVideo where updated_at >= date_sub(curdate(),interval '%s' day) and updated_at < date_sub(curdate(),interval '%s' day)" %(stop,start)
  print get_data
  cur.execute(get_data)
  curTime = time.strftime("%Y%m%d", time.localtime(time.time()-stop*24*60*60))
  print curTime
  csvfile = file('/Job/datawarehouse/opertaion/script/hist/archive/matchedVideo' + curTime + '.csv','wb')
  writers = csv.writer(csvfile)
  writers.writerow(['id'])

  while 1:
    lines = cur.fetchall()
    if len(lines) ==0:
      break
    for i in lines:
      writers.writerows([i])

  csvfile.close()
  stop = stop + 1
  start = start + 1
  print stop
  print start
  listfile = os.listdir(info)
  for line in listfile:
    lines = info + line 
    upload = 'gsutil cp ' + lines + ' gs://vobile-data-analysis/matchedVideo/'  
    os.system(upload)

  os.remove(lines)

cur.close()
conn.close()





