import MySQLdb
import sys
import time
import datetime

#lowCompliantSitesNum /lowCompliantSitesPercent
conn=MySQLdb.connect(host='eqx-vtweb-slave-db',user='kettle',passwd='k3UTLe',port=3306)
conn.select_db('tracker2')
cur=conn.cursor()

cur.execute('set @baseTime = "2016-03-01"; select @baseTime, sum(if(compliant <= 80,1,0)) Low_compliant_Sites_Num, sum(if(compliant <= 80,1,0)) / count(*) * 100 Low_compliant_Sites_percent from (select trackingWebsite_id, sum(if (takeoff_time > 0,1,0)) / count(*) * 100 compliant from matchedVideo where company_id = 14 and hide_flag = 2 and count_send_notice > 0 and created_at >= @baseTime - interval 6 day and  created_at < @baseTime + interval 1 day group by 1) tmp_a')


rows = cur.fetchall()
print rows
conn=MySQLdb.connect(host='192.168.110.114',user='kettle',passwd='k3UTLe',port=3306)    
cur=conn.cursor()
conn.select_db('DW_VTMetrics')


for e in rows:
    insert = "insert into VTLowCompliantSiteSummaryReport(company_id,reportedDate,lowCompliantSitesNum,lowCompliantSitesPercent) values('%s','%s','%s','%s')" %(14,e[0],e[1],e[2])
    print insert
    cur.execute(insert)
              
conn.commit()
cur.close()
conn.close()
