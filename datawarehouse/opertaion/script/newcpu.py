#!/usr/bin/python

import requests
import MySQLdb
import time,os,glob
import json
from socket import socket,AF_INET,SOCK_DGRAM,SOCK_STREAM
import sys
import re
import scribe
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol

def read_cpu_usage():
    """Read the current system cpu usage from /proc/stat."""
    lines = open("/proc/stat").readlines()
    for line in lines:
        #print "l = %s" % line
        l = line.split()
        if len(l) < 5:
            continue
        if l[0].startswith('cpu'):
            return l;
    return {}
  
def sendlog(host,port,messa):
    #"""send log to scribe
    socket = TSocket.TSocket(host=por-falcon-graph-1, port=port)
    transport = TTransport.TFramedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False, strictWrite=False)
    client = scribe.Client(iprot=protocol, oprot=protocol)
    transport.open()
    log_entry = scribe.LogEntry(dict(category='SYSD', message=messa))
    result = client.Log(messages=[log_entry])
    transport.close()
    return result
 
if len(sys.argv) >= 2:
  host_port = sys.argv[1].split(':')
  host = host_port[0]
  if len(host_port) > 1:
    port = int(host_port[1])
  else:
    port = 1463
else:
  sys.exit('usage : py.test  host[:port]] ')
 
cpustr=read_cpu_usage()
down=True
#cpu usage=[(user_2 +sys_2+nice_2) - (user_1 + sys_1+nice_1)]/(total_2 - total_1)*100
usni1=long(cpustr[1])+long(cpustr[2])+long(cpustr[3])+long(cpustr[5])+long(cpustr[6])+long(cpustr[7])+long(cpustr[4])
usn1=long(cpustr[1])+long(cpustr[2])+long(cpustr[3])
#usni1=long(cpustr[1])+long(cpustr[2])+long(cpustr[3])+long(cpustr[4])
while(down):
       time.sleep(2)
       cpustr=read_cpu_usage()
       usni2=long(cpustr[1])+long(cpustr[2])+float(cpustr[3])+long(cpustr[5])+long(cpustr[6])+long(cpustr[7])+long(cpustr[4])
       usn2=long(cpustr[1])+long(cpustr[2])+long(cpustr[3])
       #usni2=long(cpustr[1])+long(cpustr[2])+float(cpustr[3])+long(cpustr[4])
       print usn2
       print usni2
       cpuper=(usn2-usn1)/(usni2-usni1)
       s="CPUTotal used percent =%.4f \r\n" % cpuper
       print s
       sendlog(host,port,s)
       usn1=usn2
       usni1=usni2 



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
                "counter": "cpu.busy",
                "counter": "cpu.core/idc=watong.vobile,type=SA",
                "counter": "cpu.guest",
                "counter": "cpu.idle",
                "counter": "cpu.iowait",
                "counter": "cpu.irq",
                "counter": "cpu.nice",
                "counter": "cpu.socket/idc=watong.vobile,type=SA",
                "counter": "cpu.softirq",
                "counter": "cpu.steal",
                "counter": "cpu.switches",
                "counter": "cpu.system",
                "counter": "cpu.thread/idc=watong,type=SA",
                "counter": "cpu.user"
              },
                
                                 ],
            }    
    else:
       break

url = "http://216.151.23.144:9966/graph/history"
r = requests.post(url, data = json.dumps(d))
#       print r.endpoint,r.counter

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('DW_VTMetrics')
cur = conn.cursor()

pools = r.json()

print pools
for pool in pools:
    insert = "insert into VTCPUMetricsReport(hostname,cpu_busy,cpu_core_idc,cpu_guest,cpu_idle,cpu_iowait,cpu_irq,cpu_nice,cpu_socket_idc,cpu_softirq,cpu_steal,cpu_switches,cpu_system,cpu_thread_idc,cpu_user) values()"  %(pool["endpoint"],pool["counter"].split("/")[-1])
    cur.execute(insert)
    conn.commit

file_list.close()
cur.close()
conn.close()
