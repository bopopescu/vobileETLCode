#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime

#taisan Video
conn=MySQLdb.connect(host='eqx-taisan-slave-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('taisan') 

cur=conn.cursor()

stop = 4170000000
colname = ''
for i in range(0,11049):

  colname += "'" + str(stop) + "',"
  stop = stop + 1
colname = colname[0:-1]
get_data = "select * from video_trackingMeta where video_id in (%s)" %(colname)
cur.execute(get_data)
rows = cur.fetchall()  

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

cur=conn.cursor()
conn.select_db('DW_VTMetrics')

for e in rows:
   if not e is None:
      insert = 'insert into video_trackingMeta(video_id,trackingMeta_id) values("%s","%s")' %(e[0],e[1])

      print insert
      cur.execute(insert)
      conn.commit()

cur.close()
conn.close()



