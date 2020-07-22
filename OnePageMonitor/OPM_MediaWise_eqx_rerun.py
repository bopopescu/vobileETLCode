import sys,os,time
import MySQLdb
import logging
import reportInfoGet

script_start_time = time.strftime('%Y-%m-%d %X',time.localtime(time.time()))


LOG_FILENAME = "/Job/OnePageMonitor/log/OnePageMonitor.log"
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,format='%(asctime)s %(name)-4s %(levelname)-4s %(message)s')
logger = logging.getLogger("OnePageMonitor")

IDC="EQX"
STARTDATE = '2016-04-18'
ENDDATE= '2016-04-19'
#pentaho db
reporting_host = '54.67.114.123'
reporting_user = 'kettle'
reporting_password = 'kettle'
reporting_usedb = 'ONEPAGE_MONITOR'

#eqx mediawise db
db_wmw_host = 'eqx-mediawise-subordinate-db'
db_wmw_user = 'kettle'
db_wmw_password = 'k3UTLe'
db_wmw_port = 3306
db_wmw_usedb = 'mediawise'

wasu_sql1="""
select c.company_name, if(submit.submit_num is null,0,submit.submit_num), if(finished.finished_num is null,0,finished.finished_num), if(failed.failed_num is null,0,failed.failed_num), if(matched.matched_num is null,0,matched.matched_num), if(noresult.noresult_num is null,0,noresult.noresult_num), if(duration.avg_duration is null,0,duration.avg_duration), if(response.avg_response is null,0,response.avg_response)
from company c
left join 
(
select a.company_id, sum(count) submit_num from (select company_id,count(*) count from task where created_at >='%s' and created_at <'%s' group by 1)a group by 1
)submit on c.id=submit.company_id
left join 
(
select a.company_id, sum(count) finished_num from (select company_id,count(*) count from task where created_at >='%s' and created_at <'%s' and (status ='query_success' or status='query_failed') group by 1)a group by 1
)finished on c.id=finished.company_id
left join 
(
select a.company_id, sum(count) failed_num from (select company_id,count(*) count from task where created_at >='%s' and created_at <'%s' and status='query_failed' group by 1)a group by 1
)failed on c.id=failed.company_id
left join 
(
select m.company_id, count(distinct task_identification) matched_num from matchVideo m where m.created_at >='%s' and m.created_at <'%s' group by 1
)matched on c.id=matched.company_id
left join 
(
select a.company_id, count(*) noresult_num from task a where a.created_at >='%s' and a.created_at <'%s' and fetch_time = 0 group by 1
)noresult on c.id=noresult.company_id
left join 
(
select a.company_id, sum(duration)/sum(count) avg_duration from (select company_id,count(*) count, sum(clip_duration) duration from task where created_at >='%s' and created_at <'%s' group by 1)a group by 1
)duration on duration.company_id=c.id
left join
(
select a.company_id, sum(query_time)/sum(count) avg_response from (select company_id,count(*) count, sum(unix_timestamp(end_query_time) - unix_timestamp(created_at)) query_time from task where created_at >='%s' and created_at <'%s' and query_count =1 group by 1)a group by 1
)response on response.company_id=c.id
where c.is_delete='false';
"""

wasu_sql2="""
select c.company_name, sum(count) from (select company_id,count(*) count from task where created_at >='%s' and created_at <'%s' and (unix_timestamp(now())-unix_timestamp(created_at))>10800 and (status='new' or status='query') group by 1 union all select company_id,count(*) count from task where created_at >='%s' and created_at <'%s' and (unix_timestamp(end_query_time)-unix_timestamp(created_at))>10800 group by 1)a, company c where c.id=a.company_id  group by 1;
"""
wasu_update_sql="""update OPM_MediaWise set task_pending_num=task_submitted_num-task_finished_num where report_at='%s' and data_center='EQX';"""




try:
    script_name = __file__
    reportInfoGet.def_report_script_status("running",script_name,script_start_time,"ok")
    conn_pentaho=MySQLdb.Connection(reporting_host,reporting_user,reporting_password,reporting_usedb)
    cur_pentaho=conn_pentaho.cursor()
    cur_pentaho.execute("set autocommit=1")

    conn_wmw=MySQLdb.Connection(db_wmw_host,db_wmw_user,db_wmw_password,db_wmw_usedb, port=db_wmw_port)
    cur_wmw=conn_wmw.cursor()
    
    tmp_wsql1=wasu_sql1%(STARTDATE, ENDDATE, STARTDATE, ENDDATE, STARTDATE, ENDDATE, STARTDATE, ENDDATE, STARTDATE, ENDDATE, STARTDATE, ENDDATE, STARTDATE, ENDDATE)
    
    tmp_wsql2=wasu_sql2%(STARTDATE, ENDDATE, STARTDATE, ENDDATE)

    cur_wmw.execute(tmp_wsql1)
    print tmp_wsql1
    str_wresult1 = cur_wmw.fetchall()
    for row in str_wresult1:
	str_wsql1="""insert ignore into OPM_MediaWise(data_center,company_name, task_submitted_num, task_finished_num, task_failed_num, task_matched_num, task_noresult_num, task_avg_duration, task_avg_response, report_at) values('%s', '%s', %s, %s, %s, %s, %s, %s, %s, '%s')""" %('EQX',row[0] ,row[1],row[2],row[3],row[4],row[5],row[6],row[7],STARTDATE)
        cur_pentaho.execute(str_wsql1)
        print str_wsql1
        logger.info(str_wsql1)

    cur_wmw.execute(tmp_wsql2)
    print tmp_wsql2
    str_wresult2 = cur_wmw.fetchall()
    for row in str_wresult2:
        str_wsql2="""insert ignore into OPM_MediaWise(data_center, company_name, task_abnormal_num, report_at) values ('%s', '%s', %s, '%s') ON DUPLICATE KEY UPDATE task_abnormal_num=%s""" %('EQX',row[0] ,row[1], STARTDATE, row[1])
        cur_pentaho.execute(str_wsql2)
        print str_wsql2
        logger.info(str_wsql2)


    tmp1_wsql=wasu_update_sql%(STARTDATE)
    cur_pentaho.execute(tmp1_wsql)
    print tmp1_wsql

    reportInfoGet.def_report_script_status("finished",script_name,script_start_time,"ok")


except Exception,err:
    reportInfoGet.def_report_script_status("failed",script_name,script_start_time,err)
    print "refresh bandwidth Exception",err
    logger.info(err)
    sys.exit()

sys.exit()


