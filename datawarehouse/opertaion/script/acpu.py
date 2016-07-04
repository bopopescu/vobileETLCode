#!/usr/bin/python

import requests
import MySQLdb
import time,os,glob
import json
from socket import socket,AF_INET,SOCK_DGRAM,SOCK_STREAM

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

for line in lines:  
  line = line.replace('\n','')
  d = {
        "start": start,
        "end": end,
        "cf": "AVERAGE",
        "endpoint_counters": [
            {
                "endpoint": line,
                "acounter": "cpu.busy",
                "bcounter": "cpu.core/idc=watong.vobile,type=SA",
                "ccounter": "cpu.guest",
                "dcounter": "cpu.idle",
                "ecounter": "cpu.iowait",
                "fcounter": "cpu.irq",
                "gcounter": "cpu.nice",
                "hcounter": "cpu.socket/idc=watong.vobile,type=SA",
                "icounter": "cpu.softirq",
                "jcounter": "cpu.steal",
                "kcounter": "cpu.switches",
                "lcounter": "cpu.system",
                "mcounter": "cpu.thread/idc=watong,type=SA",
                "ncounter": "cpu.user"
            },
            
                              ],
      }
  
  url = "http://216.151.23.144:9966/graph/history"
  r = requests.post(url, data=json.dumps(d))
#  print r  
  pools = r.json()
  for pool in pools:
      key = "storage.nas-watong-1.%s.%s.used" % (pool["endpoint"],pool["counter"].split("/")[-1]) 
      insert = "insert into VTCPUMetricsReport(hostname, cpu_busy, cpu_core_idc,cpu_guest, cpu_idle, cpu_iowait,cpu_irq, cpu_nice, cpu_socket_idc,cpu_softirq, cpu_steal, cpu_switches, cpu_system, cpu_thread_idc, cpu_user) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" %(pool["endpoint"],pool["acounter"],pool["bcounter"],pool["ccounter"],pool["dcounter"],pool["ecounter"],pool["fcounter"],pool["gcounter"],pool["hcounter"],pool["icounter"],pool["jcounter"],pool["kcounter"],pool["lcounter"],pool["mcounter"],pool["ncounter"])
      print insert
#      for record in pool["Values"]:
#        value = record["value"]
#        insert = "insert into VTCPUMetricsReport(hostname, cpu_busy, cpu_core_idc,cpu_guest, cpu_idle, cpu_iowait,cpu_irq, cpu_nice, cpu_socket_idc, cpu_softirq, cpu_steal, cpu_switches, cpu_system, cpu_thread_idc, cpu_user) values('%s')" %(pool["endpoint"],value[0],value[1],value[2],value[3],value[4],value[5],value[6],value[7],value[8],value[9],value[10],value[11],value[12],value[13],value[14])
      cur.execute(insert)
      conn.commit()

cur.close()
conn.close()
