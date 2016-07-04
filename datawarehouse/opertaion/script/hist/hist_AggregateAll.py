#!/usr/bin/python
#coding=utf-8

import MySQLdb
import sys
import time
import datetime

stop = 1
start = 0

curTime = ''
print curTime
print stop
print start
conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('DW_VTMetrics')
cur=conn.cursor()

for i in range(1,100):
  get_data = "select * from TempMetricsReportAll where reportedDate >= date_sub(curdate(),interval '%s' day) and reportedDate < date_sub(curdate(),interval '%s' day)" %(stop,start)
#  print get_data
  cur.execute(get_data)
  rows = cur.fetchall()
  curTime = time.strftime("%Y%m%d", time.localtime(time.time()-stop*24*60*60))
  stop = stop + 1
  start = start + 1
  for e in rows:
    if e is not None:
#    title = "update TempMetricsReportAll set '%s' = '%s' where metric_Title in ('%s')" %(curTime,e[1],'title')    
#    print title
 #   cur.execute(title)

#    site = "update TempMetricsReportAll set '%s' = '%s' where metric_Title in ('%s')" %(curTime,e[2],'site')
#    cur.execute(site)

      Low_compliant_Sites_Num = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[4])),'Low_compliant_Sites_Num')
      print Low_compliant_Sites_Num
      cur.execute(Low_compliant_Sites_Num)
 
      Low_compliant_Sites = "update ArgMetricsReportAll set `%s`= '%s' where metric_Title in ('%s')" %(curTime,str(round(e[5],2))+'%','Low_compliant_Sites_Percent') 
      print Low_compliant_Sites
      cur.execute(Low_compliant_Sites)
    
      Reported_Matches_All_Hosting = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[6])),'Reported_Matches_All_Hosting')    
      print Reported_Matches_All_Hosting
      cur.execute(Reported_Matches_All_Hosting)

      Reported_Matches_UGC = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[7])),'Reported_Matches_UGC')   
      print Reported_Matches_UGC
      cur.execute(Reported_Matches_UGC)
              
      Reported_Matches_Hybrid = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[8])),'Reported_Matches_Hybrid') 
      print Reported_Matches_Hybrid 
      cur.execute(Reported_Matches_Hybrid)

      Reported_Matches_Cyberlocker = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[9])),'Reported_Matches_Cyberlocker') 
      print Reported_Matches_Cyberlocker
      cur.execute(Reported_Matches_Cyberlocker)

      Reported_Linking_URLs_Linking_Sites= "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[10])),'Reported_Linking_URLs_Linking_Sites') 
      print Reported_Linking_URLs_Linking_Sites
      cur.execute(Reported_Linking_URLs_Linking_Sites)    

      Reported_Linking_URLs_Search_Engine = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[11])),'Reported_Linking_URLs_Search_Engine')
      print Reported_Linking_URLs_Search_Engine
      cur.execute(Reported_Linking_URLs_Search_Engine)

      Matches_Notices_Sent_All_Hosting = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[12])),'Notices_Sent_All_Hosting')
      print Matches_Notices_Sent_All_Hosting
      cur.execute(Matches_Notices_Sent_All_Hosting)

      Matches_Notices_Sent_UGC = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[13])),'Notices_Sent_UGC')
      print Matches_Notices_Sent_UGC
      cur.execute(Matches_Notices_Sent_UGC)

      Matches_Notices_Sent_Hybrid = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[14])),'Notices_Sent_Hybrid')
      print Matches_Notices_Sent_Hybrid
      cur.execute(Matches_Notices_Sent_Hybrid)

      Matches_Notices_Sent_Cyberlocker = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[15])),'Notices_Sent_Cyberlocker') 
      print Matches_Notices_Sent_Cyberlocker
      cur.execute(Matches_Notices_Sent_Cyberlocker)


      URLs_Notices_Sent_Linking_Sites = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[16])),'URLs_Notices_Sent_Linking_Sites') 
      print URLs_Notices_Sent_Linking_Sites
      cur.execute(URLs_Notices_Sent_Linking_Sites)


      URLs_Notices_Sent_Search_Engine = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[17])),'URLs_Notices_Sent_Search_Engine') 
      print URLs_Notices_Sent_Search_Engine
      cur.execute(URLs_Notices_Sent_Search_Engine)


      Compliance_Percent_UGC = "update ArgMetricsReportAll set `%s` = '%s' where metric_Title in ('%s')" %(curTime,str(round(e[18]*100,2)) + '%' ,'Compliance_Percent_UGC')
      print Compliance_Percent_UGC
      cur.execute(Compliance_Percent_UGC)

      Compliance_Percent_Hybrid = "update ArgMetricsReportAll set `%s` = '%s' where metric_Title in ('%s')" %(curTime,str(round(e[19]*100,2)) + '%' ,'Compliance_Percent_Hybrid')
      print Compliance_Percent_Hybrid
      cur.execute(Compliance_Percent_Hybrid)


      Compliance_Percent_Cyberlocker = "update ArgMetricsReportAll set `%s` = '%s' where metric_Title in ('%s')" %(curTime,str(round(e[20]*100,2)) + '%' ,'Compliance_Percent_Cyberlocker')
      print Compliance_Percent_Cyberlocker
      cur.execute(Compliance_Percent_Cyberlocker)


      Compliance_Percent_Linking_Site = "update ArgMetricsReportAll set `%s` = '%s' where metric_Title in ('%s')" %(curTime,str(round(e[21]*100,2)) + '%' ,'Compliance_Percent_Linking_Site') 
      print Compliance_Percent_Linking_Site
      cur.execute(Compliance_Percent_Linking_Site)


      Compliance_Percent_Search_Engine = "update ArgMetricsReportAll set `%s` = '%s' where metric_Title in ('%s')" %(curTime,str(round(e[22]*100,2)) + '%' , 'Compliance_Percent_Search_Engine') 
      print Compliance_Percent_Search_Engine
      cur.execute(Compliance_Percent_Search_Engine)    


      Compliance_Time_UGC = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[23]/60)),'Compliance_Time_UGC(Min)') 
      print Compliance_Time_UGC
      cur.execute(Compliance_Time_UGC)


      Compliance_Time_Hybrid = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[24]/60)),'Compliance_Time_Hybrid(Min)')
      print Compliance_Time_UGC
      cur.execute(Compliance_Time_Hybrid)    
    

      Compliance_Time_Cyberlocker = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[25]/60)),'Compliance_Time_Cyberlocker(Min)')
      print Compliance_Time_Cyberlocker
      cur.execute(Compliance_Time_Cyberlocker)


      Compliance_Time_Linking_Site = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[26]/60)),'Compliance_Time_Linking_Site(Min)') 
      print Compliance_Time_Linking_Site
      cur.execute(Compliance_Time_Linking_Site)


      Compliance_Time_Search_Engine = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[27]/60)),'Compliance_Time_Search_Engine(Min)') 
      print Compliance_Time_Search_Engine
      cur.execute(Compliance_Time_Search_Engine)


      IPs_P2P = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[28])),'IPs_P2P')
      print IPs_P2P
      cur.execute(IPs_P2P)

      Notices_Sent_P2P = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[29])),'Notices_Sent_P2P') 
      print Notices_Sent_P2P
      cur.execute(Notices_Sent_P2P)

      VK_Infringing_Matches = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[31])),'VK_Infringing_Matches')
      print VK_Infringing_Matches
      cur.execute(VK_Infringing_Matches)

      Baidu_Pan_Infringing_Matches = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[33])),'Baidu_Pan_Infringing_Matches')
      print Baidu_Pan_Infringing_Matches
      cur.execute(Baidu_Pan_Infringing_Matches)

      Facebook_Infringing_Matches = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[32])),'Facebook_Infringing_Matches')
      print Facebook_Infringing_Matches
      cur.execute(Facebook_Infringing_Matches)

      YouTube_Infringing_Matches = "update ArgMetricsReportAll set `%s` = format(%s,',') where metric_Title in ('%s')" %(curTime,str(round(e[30])),'YouTube_Infringing_Matches')
      print YouTube_Infringing_Matches
      cur.execute(YouTube_Infringing_Matches)

      conn.commit()


cur.close()
conn.close()

# start = start + 1
