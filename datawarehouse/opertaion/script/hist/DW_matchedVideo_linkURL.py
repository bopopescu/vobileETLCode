#!/usr/bin/python
#-*-coding:utf-8-*-
import MySQLdb
import sys
import time
import datetime
import logging

#def setLogger():
   #创建一个logger,可以考虑如何将它封装  
logger = logging.getLogger('mylogger')
logger.setLevel(logging.DEBUG)

   #创建一个handler，用于写入日志文件
fh = logging.FileHandler(os.path.join(os.getcwd(),'log.txt'))
fh.setLevel(logging.DEBUG)

   #再创建一个handler，用于输出到控制台
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

   #定义handler的输出格式
formatter = logging.Formatter('%(asctime)s - %(module)s.%(funcName)s.%(lineno)d - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
 
   #给logger添加handler
logger.addHandler(fh)
logger.addHandler(ch)

   #记录一条日志
   logger.info('hello world, i\'m log helper in python, may i help you')
   return logger

#log_format = '%(filename)s [%(asctime)s] [%(levelname)s] %(message)s'
#logging.basicConfig(format=log_format,datefmt='%Y-%m-%d %H:%M:%S %p',level=logging.DEBUG)
#logging.debug('this message should be logged')
#matchedVideo_linkURL

conn=MySQLdb.connect(host='eqx-vtweb-slave-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2') 
cur=conn.cursor()
stop = 0
start = 1

for i in range(1,420):
  get_data = "select id,matchedVideo_id,company_id,trackingWebsite_id,link_keyid,confirm_status,replace(link_url,'" + "\\'" + "','" + "\"" + "'),is_in_takedown_queue,in_takedown_queue_date,count_send_notice,first_send_notice_date,last_send_notice_date,notice_send_by,takeoff_time,created_at,updated_at from matchedVideo_linkURL where updated_at >= date_sub(curdate(),interval '%s' day) and updated_at < date_sub(curdate(),interval '%s' day)" %(start,stop)

  print get_data
  cur.execute(get_data)
  rows = cur.fetchall()  


  dwconn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306) 

  dwcur=dwconn.cursor()
  dwconn.select_db('DW_VTMetrics')

  for e in rows:
    insert = "insert into DW_matchedVideo_linkURL(id,matchedVideo_id,company_id,trackingWebsite_id,link_keyid,confirm_status,link_url,is_in_takedown_queue,in_takedown_queue_date,count_send_notice,first_send_notice_date,last_send_notice_date,notice_send_by,takeoff_time,created_at,updated_at) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" %(e[0],e[1],e[2],e[3],e[4],e[5],e[6],e[7],e[8],e[9],e[10],e[11],e[12],e[13],e[14],e[15])

    print insert
    dwcur.execute(insert)
    dwconn.commit()
  
    excepti:
      logger.exception("Exceptioin Logged")   

  start = start + 1
  stop = stop + 1


cur.close()
conn.close()



