#!/usr/bin/python

import requests
import MySQLdb
import time,os,glob
import json
from socket import socket,AF_INET,SOCK_DGRAM,SOCK_STREAM
ISOTIMEFORMAT='%Y-%m-%d %X'

class Graphite(object):
    def __init__(self, addr='206.99.94.252', port=2003):
        self.sock = socket()
        self.sock.connect( (addr, port) )

    def send(self, key, value, collect_time):
        message = "%s %s %s\n" % (key, value, collect_time)
        self.sock.send(message)
    def close(self):
        self.sock.close()


end = 1451577600
start = 1448899200

thefile = open("/Job/datawarehouse/opertaion/script/phy_hostname.txt","r")
lines = thefile.readlines()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('DW_VTMetrics')
cur = conn.cursor()

#######################cpu_busy

for line in lines:  
  line = line.replace('\n','')
  d = {
        "start": start,
        "end": end,
        "cf": "AVERAGE",
        "endpoint_counters": [
            {
                "endpoint": line,
                "counter": "cpu.busy"            
            },
            
                              ],
      }
  
  url = "http://216.151.23.144:9966/graph/history"
  r = requests.post(url, data=json.dumps(d))
 
  pools = r.json()
  for pool in pools:
      key = "storage.nas-watong-1.%s.%s.used" % (pool["endpoint"],pool["counter"].split("/")[-1]) 
      for record in pool["Values"]:
        value = record["value"]
        print pool["endpoint"],pool["counter"], value
        if not value is None:
          insert = "insert into VTCPUMetricsReport(check_date, host_name, cpu_busy) values ('%s','%s','%s')" %(time.strftime(ISOTIMEFORMAT,time.localtime()),pool["endpoint"],value)

          cur.execute(insert)
          conn.commit()

#######################cpu.core/idc

for line in lines:  
  line = line.replace('\n','')
  d = {
        "start": start,
        "end": end,
        "cf": "AVERAGE",
        "endpoint_counters": [
            {
                "endpoint": line,
                "counter": "cpu.core/idc=watong.vobile,type=SA"            
            },
            
                              ],
      }
  
  url = "http://216.151.23.144:9966/graph/history"
  r = requests.post(url, data=json.dumps(d))
 
  pools = r.json()
  for pool in pools:
      key = "storage.nas-watong-1.%s.%s.used" % (pool["endpoint"],pool["counter"].split("/")[-1]) 
      for record in pool["Values"]:
        value = record["value"]
        print pool["endpoint"],pool["counter"], value
        if not value is None:
          core_idc_insert = "update VTCPUMetricsReport set cpu_core_idc = '%s' where host_name in ('%s')" %(value,pool["endpoint"])

          cur.execute(core_idc_udpate)
          conn.commit()



cur.close()
conn.close()
