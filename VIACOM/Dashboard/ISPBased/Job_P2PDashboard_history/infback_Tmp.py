import os
import MySQLdb
import datetime
import time
from MySQLHelper import MySQLHelper


tracker2_host = "eqx-vtweb-slave-db"
tracker2_user = "kettle"
tracker2_pass = "k3UTLe"
tracker2_db = "tracker2"


t_host = "192.168.110.114"
t_user = "kettle"
t_pass = "k3UTLe"
t_db = "DM_VIACOM_TEST"


def main():
    tracker2 = MySQLHelper(tracker2_host,tracker2_user,tracker2_pass,tracker2_db)
    
    countid = 0
    countid1 = 1000000
    while countid < 123000000:
        select = """SELECT b.id, c.key_id FROM tracker2.matchedVideoP2PItem AS b, tracker2.matchedVideo AS c WHERE b.company_id = 14 AND c.company_id = 14 AND b.matchedVideo_id = c.id and b.id > %s and b.id <= %s""" %(countid,countid1)
        countid += 1000000
        countid1 += 1000000
        print select
        select_result = tracker2.query_sql_cmd(select)
        print "fetched!"
        retrytracker2 = 0
        while retrytracker2 < 10:
            try:
                print "retrytracker2:"
                print retrytracker2
                target = MySQLHelper(t_host,t_user,t_pass,t_db)
	        print "start insert!"
	        print datetime.datetime.now()
	        break
            except Exception,e:
                print e
                retrytracker2 += 1

        #target = MySQLHelper(t_host,t_user,t_pass,t_db)
        insert_result = "insert into infback_Tmp " + " values " + str(select_result)[1:-1].replace("L", "").replace("u","")
        target.insert_sql_cmd(insert_result)
        print "finished!"
        print datetime.datetime.now()
    

if __name__=="__main__":
    main()

