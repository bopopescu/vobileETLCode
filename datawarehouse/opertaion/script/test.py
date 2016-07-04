#!/usr/bin/python

import sys
import re
import time
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
