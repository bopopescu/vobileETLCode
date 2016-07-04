#!/usr/bin/python
import sys
import socket

host = sys.argv[1]
port = sys.argv[2]
service = sys.argv[3]
component = sys.argv[4]
address = sys.argv[5] if len(sys.argv) == 6 else ""

def send_message(host, port, service, component, address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, int(port)))
    json = '{"status":[{"service":"' + service + '","component":"' + component + '"},{"address":"' + address + '"}]}'
    sock.send(json)
    sock.close() 

if __name__  == "__main__":
    send_message(host, port, service, component, address)
    # {"status":[{"service":"service name","component":"application name"},{"address":"127.0.0.1"}]}

