#!/usr/bin/python2

import requests
import time
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

d = {
        "start": start,
        "end": end,
        "cf": "AVERAGE",
        "endpoint_counters": [
            {
                "endpoint": "hz-401",
                "counter": "df.bytes.total/fstype=ext4,mount=/datapool",
            },
            {
                "endpoint": "hz-420",
                "counter": "df.bytes.total/fstype=ext4,mount=/data",
            },
            {
                "endpoint": "hz-430",
                "counter": "df.bytes.total/fstype=ext4,mount=/mnt",
            },
        ],
}


url = "http://216.151.23.144:9966/graph/history"
r = requests.post(url, data=json.dumps(d))


pools = r.json()
for pool in pools:
    graphite = Graphite(addr="206.99.94.252")
    key = "storage.nas-watong-1.%s.%s.used" % (pool["endpoint"],pool["counter"].split("/")[-1])
    print pool["endpoint"]
    print pool["counter"]
    for record in pool["Values"]:
        value = record["value"]
        print value
        
        #if not value is None:
        #    collect_time = record["timestamp"]
        #    graphite.send(key, value, collect_time)
    graphite.close()
