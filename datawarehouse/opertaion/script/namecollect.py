#!/usr/bin/python

import requests
import MySQLdb
import time,os,glob
import json
from socket import socket,AF_INET,SOCK_DGRAM,SOCK_STREAM

end = 1451577600
start = 1448899200

thefile = open("/Job/datawarehouse/opertaion/script/phy_hostname.txt","r")
lines = thefile.readlines()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('DW_VTMetrics')
cur = conn.cursor()

for line in lines:  
    line = line.replace('\n','')
    insert = "insert into VTCPUMetricsReport(hostname) values('%s')" %(line)
    cur.execute(insert)
    conn.commit()

cur.close()
conn.close()
