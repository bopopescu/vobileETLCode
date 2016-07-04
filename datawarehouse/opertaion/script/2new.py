#!/usr/bin/python

import requests
import MySQLdb
import time,os,glob
import json
from socket import socket,AF_INET,SOCK_DGRAM,SOCK_STREAM


file_list = open("/Job/datawarehouse/opertaion/script/hostname.txt")

end = 1451577600
start = 1448899200

while True:

    line = file_list.readline()
    
    if line:
       line = line.replace('\n','')
       d = {
            "start": start,
            "end": end,
            "cf": "AVERAGE",
            "endpoint_counters": [
              {
                "endpoint": "line",
                "counter01": "cpu.busy",
                "counter02": "cpu.core/idc=watong.vobile,type=SA",
                "counter03": "cpu.guest",
                "counter04": "cpu.idle",
                "counter05": "cpu.iowait",
                "counter06": "cpu.irq",
                "counter07": "cpu.nice",
                "counter08": "cpu.socket/idc=watong.vobile,type=SA",
                "counter09": "cpu.softirq",
                "counter10": "cpu.steal",
                "counter11": "cpu.switches",
                "counter12": "cpu.system",
                "counter13": "cpu.thread/idc=watong,type=SA",
                "counter14": "cpu.user"
              },
                
                                 ],
            }    
    else:
       break

#url = "http://216.151.23.144:9966/graph/history"
#r = requests.post(url, data = json.dumps(d))
pools = json.dumps(d)

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('DW_VTMetrics')
cur = conn.cursor()

#pools = r.json()

print pools
for pool in pools:
    insert = "insert into VTCPUMetricsReport(hostname,cpu_busy,cpu_core_idc,cpu_guest,cpu_idle,cpu_iowait,cpu_irq,cpu_nice,cpu_socket_idc,cpu_softirq,cpu_steal,cpu_switches,cpu_system,cpu_thread_idc,cpu_user) values()"  %(pool["endpoint"],pool["counter01"],pool["counter02"],pool["counter03"],pool["counter04"],pool["counter05"],pool["counter06"],pool["counter07"],pool["counter08"],pool["counter09"],pool["counter10"],pool["counter11"],pool["counter12"],pool["counter13"],pool["counter14"])
    cur.execute(insert)
    conn.commit

file_list.close()

cur.close()
conn.close()
