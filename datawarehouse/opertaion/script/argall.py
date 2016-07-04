#!/usr/bin/python
#coding=utf-8

import MySQLdb
import sys
import time
import datetime

stop = 1
for i in range(1,20):
 curTime = time.strftime("%Y%m%d", time.localtime(time.time()-stop*24*60*60))
 print curTime

 conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)
 conn.select_db('DW_VTMetrics')
 cur=conn.cursor()
 get_data = "select * from TempMetricsReportAll where reportedDate = date_sub(curdate(),interval '%s' day)" %(stop)
# print get_data
 cur.execute(get_data)
 rows = cur.fetchall()

 for e in rows:
   if e is not None:
#    title = "update TempMetricsReportAll set '%s' = '%s' where metric_Title in ('%s')" %(curTime,e[1],'title')    
#    print title
 #   cur.execute(title)

#    site = "update TempMetricsReportAll set '%s' = '%s' where metric_Title in ('%s')" %(curTime,e[2],'site')
#    cur.execute(site)

    Low_compliant_Sites_Num = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[4],'Low_compliant_Sites_Num')
    print Low_compliant_Sites_Num
    cur.execute(Low_compliant_Sites_Num)
 
    Low_compliant_Sites = "update ArgMetricsReportAll set `%s`= %s where metric_Title in ('%s')" %(curTime,e[5],'Low_compliant_Sites_Percent') 
    print Low_compliant_Sites
    cur.execute(Low_compliant_Sites)
    
    Reported_Matches_All_Hosting = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[6],'Reported_Matches_All_Hosting')    
    print Reported_Matches_All_Hosting
    cur.execute(Reported_Matches_All_Hosting)

    Reported_Matches_UGC = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[7],'Reported_Matches_UGC')   
    print Reported_Matches_UGC
    cur.execute(Reported_Matches_UGC)
              
    Reported_Matches_Hybrid = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[8],'Reported_Matches_Hybrid') 
    print Reported_Matches_Hybrid 
    cur.execute(Reported_Matches_Hybrid)

    Reported_Matches_Cyberlocker = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[9],'Reported_Matches_Cyberlocker') 
    print Reported_Matches_Cyberlocker
    cur.execute(Reported_Matches_Cyberlocker)

    Reported_Linking_URLs_Linking_Sites= "update ArgMetricsReportAll set `%s`= %s where metric_Title in ('%s')" %(curTime,e[10],'Reported_Linking_URLs_Linking_Sites') 
    print Reported_Linking_URLs_Linking_Sites
    cur.execute(Reported_Linking_URLs_Linking_Sites)    

    Reported_Linking_URLs_Search_Engine = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[11],'Reported_Linking_URLs_Search_Engine')
    print Reported_Linking_URLs_Search_Engine
    cur.execute(Reported_Linking_URLs_Search_Engine)

    Matches_Notices_Sent_All_Hosting = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[12],'Matches_Notices_Sent_All_Hosting')
    print Matches_Notices_Sent_All_Hosting
    cur.execute(Matches_Notices_Sent_All_Hosting)

    Matches_Notices_Sent_UGC = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[13],'Matches_Notices_Sent_UGC')
    print Matches_Notices_Sent_UGC
    cur.execute(Matches_Notices_Sent_UGC)

    Matches_Notices_Sent_Hybrid = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[14],'Matches_Notices_Sent_Hybrid')
    
    cur.execute(Matches_Notices_Sent_Hybrid)

    Matches_Notices_Sent_Cyberlocker = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[15],'Matches_Notices_Sent_Cyberlocker') 
    cur.execute(Matches_Notices_Sent_Cyberlocker)

    URLs_Notices_Sent_Linking_Sites = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[16],'URLs_Notices_Sent_Linking_Sites') 
    cur.execute(URLs_Notices_Sent_Linking_Sites)

    URLs_Notices_Sent_Search_Engine = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[17],'URLs_Notices_Sent_Search_Engine') 
    cur.execute(URLs_Notices_Sent_Search_Engine)

    Compliance_Percent_UGC = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[18],'Compliance_Percent_UGC')
    cur.execute(Compliance_Percent_UGC)

    Compliance_Percent_Hybrid = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[19],'Compliance_Percent_Hybrid')
    cur.execute(Compliance_Percent_Hybrid)

    Compliance_Percent_Cyberlocker = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[20],'Compliance_Percent_Cyberlocker')
    cur.execute(Compliance_Percent_Cyberlocker)

    Compliance_Percent_Linking_Site = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[21],'Compliance_Percent_Linking_Site') 
    cur.execute(Compliance_Percent_Linking_Site)

    Compliance_Percent_Search_Engine = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[22],'Compliance_Percent_Search_Engine') 
    cur.execute(Compliance_Percent_Search_Engine)    

    Compliance_Time_UGC = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[23],'Compliance_Time_UGC') 
    cur.execute(Compliance_Time_UGC)

    Compliance_Time_Hybrid = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[24],'Compliance_Time_Hybrid')
    cur.execute(Compliance_Time_Hybrid)    
    
    Compliance_Time_Cyberlocker = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[25],'Compliance_Time_Cyberlocker')
    cur.execute(Compliance_Time_Cyberlocker)

    Compliance_Time_Linking_Site = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[26],'Compliance_Time_Linking_Site') 
    cur.execute(Compliance_Time_Linking_Site)

    Compliance_Time_Search_Engine = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[27],'Compliance_Time_Search_Engine') 
    cur.execute(Compliance_Time_Search_Engine)

    IPs_P2P = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[28],'IPs_P2P')
    print IPs_P2P
    cur.execute(IPs_P2P)

    Notices_Sent_P2P = "update ArgMetricsReportAll set `%s` = %s where metric_Title in ('%s')" %(curTime,e[29],'Notices_Sent_P2P') 
    cur.execute(Notices_Sent_P2P)
    conn.commit()

 cur.close()
 conn.close()

# start = start + 1
 stop = stop + 1

