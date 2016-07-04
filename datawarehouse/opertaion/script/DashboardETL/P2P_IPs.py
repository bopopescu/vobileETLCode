#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime


IPs = 0
conn=MySQLdb.connect(host='p2p-3-replica-01.c85gtgxi0qgc.us-west-1.rds.amazonaws.com',user='kettle',passwd='k3UTLe',port=3306)
# Disney
cur=conn.cursor()
conn.select_db('p2pwarehouseDisney')
get_data = "select count(*) from infringmentSummary where created_at >= date_sub(curdate(),interval 1 day) and created_at < date_sub(curdate(),interval 0 day)"

cur.execute(get_data)
rows = cur.fetchall()

for e in rows:
    IPs = IPs + e[0]  

print IPs
  
#p2pwarehousemattel
conn.select_db('p2pwarehousemattel')
get_data = "select count(*) from infringmentSummary where created_at >= date_sub(curdate(),interval 1 day) and created_at < date_sub(curdate(),interval 0 day)" 

cur.execute(get_data)
rows = cur.fetchall()

for e in rows:
    IPs = IPs + e[0]
print IPs

#viacom
conn.select_db('p2pwarehouse')
get_data = "select count(*) from infringmentSummary where created_at >= date_sub(curdate(),interval 1 day) and created_at < date_sub(curdate(),interval 0 day)" 

cur.execute(get_data)
rows = cur.fetchall()

for e in rows:
    IPs = IPs + e[0]

print IPs

#Netfilx2
conn.select_db('p2pwarehouseNetflix2')
get_data = "select count(*) from infringmentSummary where created_at >= date_sub(curdate(),interval 1 day) and created_at < date_sub(curdate(),interval 0 day)"

cur.execute(get_data)
rows = cur.fetchall()

for e in rows:
    IPs = IPs + e[0]
print IPs

#linkedin
conn.select_db('p2pwarehouselinkedin')
get_data = "select count(*) from infringmentSummary where created_at >= date_sub(curdate(),interval 1 day) and created_at < date_sub(curdate(),interval 0 day)"

cur.execute(get_data)
rows = cur.fetchall()

for e in rows:
    IPs = IPs + e[0]
print IPs

#miramax
conn.select_db('p2pwarehousemiramax')
get_data = "select count(*) from infringmentSummary where created_at >= date_sub(curdate(),interval 1 day) and created_at < date_sub(curdate(),interval 0 day)"

cur.execute(get_data)
rows = cur.fetchall()

for e in rows:
    IPs = IPs + e[0]

print IPs

#scripps
conn.select_db('p2pwarehousemiramax')
get_data = "select count(*) from infringmentSummary where created_at >= date_sub(curdate(),interval 1 day) and created_at < date_sub(curdate(),interval 0 day)"

cur.execute(get_data)
rows = cur.fetchall()
 
for e in rows:
    IPs = IPs + e[0]
 
print IPs
 
dwconn=MySQLdb.connect(host='p2p-1-replica.c85gtgxi0qgc.us-west-1.rds.amazonaws.com',user='kettle',passwd='k3UTLe',port=3306)
dwcur=dwconn.cursor()
#TVB
dwconn.select_db('p2pwarehouseTVB')
get_data = "select count(*) from infringmentSummary where created_at >= date_sub(curdate(),interval 1 day) and created_at < date_sub(curdate(),interval 0 day)"

dwcur.execute(get_data)
rows = dwcur.fetchall()

for e in rows:
    IPs = IPs + e[0]

print IPs

#yingyin_test
dwconn.select_db('p2pwarehouseyingyin_test')
get_data = "select count(*) from infringmentSummary where created_at >= date_sub(curdate(),interval 1 day) and created_at < date_sub(curdate(),interval 0 day)"

dwcur.execute(get_data)
rows = dwcur.fetchall()
  
for e in rows:
    IPs = IPs + e[0]

print IPs

#stx 
dwconn.select_db('p2pwarehousestx')
get_data = "select count(*) from infringmentSummary where created_at >= date_sub(curdate(),interval 1 day) and created_at < date_sub(curdate(),interval 0 day)"

dwcur.execute(get_data)
rows = dwcur.fetchall()

for e in rows:
    IPs = IPs + e[0]

print IPs

#legendary
dwconn.select_db('p2pwarehouselegendary')
get_data = "select count(*) from infringmentSummary where created_at >= date_sub(curdate(),interval 1 day) and created_at < date_sub(curdate(),interval 0 day)"

dwcur.execute(get_data)
rows = dwcur.fetchall()

for e in rows:
    IPs = IPs + e[0]

print IPs

#Starz
dwconn.select_db('p2pwarehouseStarz')
get_data = "select count(*) from infringmentSummary where created_at >= date_sub(curdate(),interval 1 day) and created_at < date_sub(curdate(),interval 0 day)"

dwcur.execute(get_data)
rows = dwcur.fetchall()

for e in rows:
    IPs = IPs + e[0]

print IPs

#Project007
dwconn.select_db('p2pwarehouse007')
get_data = "select count(*) from infringmentSummary where created_at >= date_sub(curdate(),interval 1 day) and created_at < date_sub(curdate(),interval 0 day)"

dwcur.execute(get_data)
rows = dwcur.fetchall()

for e in rows:
    IPs = IPs + e[0]
print IPs

#Discovery_One
dwconn.select_db('p2pwarehouseDiscovery_One')
get_data = "select count(*) from infringmentSummary where created_at >= date_sub(curdate(),interval 1 day) and created_at < date_sub(curdate(),interval 0 day)"

dwcur.execute(get_data)
rows = dwcur.fetchall()

for e in rows:
    IPs = IPs + e[0]
print IPs

dmconn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)

dmcur=dmconn.cursor()
dmconn.select_db('DW_VTMetrics')

update = "update TempMetricsReportAll set IPs_P2P = '%s' where reportedDate = date_sub(curdate(),interval 1 day)" %(IPs)
dmcur.execute(update)
dmconn.commit()

IPs = 0

cur.close()
conn.close()



