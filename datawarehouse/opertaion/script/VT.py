#!/usr/bin/python

import MySQLdb
import sys
import time
import datetime


conn=MySQLdb.connect(host='eqx-vtweb-subordinate-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2')
#Compliance% - UGC;Compliance Time - UGC

Cucur=conn.cursor()
Cucur.execute('set @baseTime = now(); Select b.website_type, sum(if(a.takeoff_time  >= date(@baseTime) - interval 80 day  and a.takeoff_time  < date(@baseTime),1,0))/count(*) Compliance_pert, avg(timestampdiff(second,a.first_send_notice_date,if(a.takeoff_time  >= date(@baseTime) - interval 80 day  and a.takeoff_time  < date(@baseTime),a.takeoff_time,a.first_send_notice_date))) avg_Compliance_Time from matchedVideo a, mddb.trackingWebsite b where a.trackingWebsite_id = b.id and b.website_type in ("ugc") and a.first_send_notice_date >= date(@baseTime) - interval 7 day  and a.first_send_notice_date < date(@baseTime)  group by 1')

Curows = Cucur.fetchall()

#Compliance% - Hybrid;Compliance% - Cyberlocker

Chcur=conn.cursor()
Chcur.execute('set @baseTime = now(); Select b.website_type, sum(if(a.takeoff_time  >= date(@baseTime) - interval 80 day  and a.takeoff_time  < date(@baseTime),1,0))/count(*) Compliance_pert, avg(timestampdiff(second,a.first_send_notice_date,if(a.takeoff_time  >= date(@baseTime) - interval 80 day  and a.takeoff_time  < date(@baseTime),a.takeoff_time,a.first_send_notice_date))) avg_Compliance_Time from matchedVideo a, mddb.trackingWebsite b where a.trackingWebsite_id = b.id and b.website_type in ("hybrid") and a.first_send_notice_date >= date(@baseTime) - interval 7 day  and a.first_send_notice_date < date(@baseTime)  group by 1')

Chrows = Chcur.fetchall()

#Compliance% - Cyberlocker;Compliance Time - Cyberlocker

Cccur=conn.cursor()
Cccur.execute('set @baseTime = now(); Select sum(if( b.takeoff_time  >= date(@baseTime) - interval 7 day and b.takeoff_time  < date(@baseTime),1,0))/count(*) Compliance_perc, avg(timestampdiff(second,a.first_send_notice_date,if(b.takeoff_time  >= date(@baseTime) - interval 7 day and b.takeoff_time  < date(@baseTime),a.takeoff_time,a.first_send_notice_date))) avg_Compliance_Time from matchedVideo a, matchedFileItem b where  a.matchedFile_id = b.matchedFile_id and a.first_send_notice_date >= date(@baseTime) - interval 7 day and a.first_send_notice_date < date(@baseTime)')

Cccur=conn.cursor()

conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)    
Cucur=conn.cursor()
Chcur=conn.cursor()
Cccur=conn.cursor()
conn.select_db('DW_VTMetrics')

for e in Curows:

    Cu_update = "update VTMetricsReport set Compliance_Percent_UGC = '%s', Compliance_Time_UGC = '%s' where reportedDate in ('%s')" %(e[0],e[1],now())
    Cucur.execute(Cu_update)
    conn.commit()

Cucur.close()

for e in Chrows:
    Ch_update = "update VTMetricsReport set Compliance_Percent_Hybrid = '%s', Compliance_Time_Hybrid = '%s' where reportedDate in ('%s')" %(e[0],e[1],now())
    Chcur.execute(Ch_update)
    conn.commit()

Chcur.close()
for e in Ccrows:
    Cc_update = "update VTMetricsReport set Compliance_Percent_Cyberlocker = '%s', Compliance_Time_Cyberlocker = '%s' where reportedDate in ('%s')" %(e[0],e[1],now())
     Cccur.execute(Cc_update)
     conn.commit()

Cccur.close()
conn.close()
