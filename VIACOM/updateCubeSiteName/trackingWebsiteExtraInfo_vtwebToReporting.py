#!/usr/bin/env python

import MySQLdb
import ConfigParser
from parseConfig import CfgParser
import sys
import os

cfg_file = "/Job/VIACOM/updateCubeSiteName/conf/trackingWebsiteExtraInfo_vtwebToReporting.cfg"
if not os.path.exists(cfg_file):
	print "file not exists"
	sys.exit(0)

conf = CfgParser(cfg_file).parse()

vtweb_tracker2_section = "vtweb_tracker2"
vt_host = conf[vtweb_tracker2_section]["host"]
vt_user = conf[vtweb_tracker2_section]["user"]
vt_passwd = conf[vtweb_tracker2_section]["passwd"]
vt_port = conf[vtweb_tracker2_section]["port"]
vt_db = conf[vtweb_tracker2_section]["db"]

vt_conn = MySQLdb.connect(host=vt_host, user=vt_user,passwd=vt_passwd, port = vt_port, charset = 'utf8')
vt_conn.select_db(vt_db)

vt_SQL = """
select a.id as trackingWebsite_id, a.country_id, a.website_type as WebsiteType, a.display_name as WebsiteName, 
ifnull(b.country_name, "unknown") as Country, ifnull(b.region, "unknown") as Region, current_timestamp as ETL_DTE 
from (select b.id, b.country_id, b.website_type, a.display_name from trackingWebsiteExtraInfo as a, mddb.trackingWebsite as b where a.trackingWebsite_id = b.id) as a 
left join mddb.country as b on a.country_id = b.id
"""

vt_cur = vt_conn.cursor()
vt_cur.execute(vt_SQL)

result = vt_cur.fetchall()

vt_conn.close()
vt_cur.close()
#################################################################################################################################
target_server_section = "target_server"
target_host = conf[target_server_section]["host"]
target_user = conf[target_server_section]["user"]
target_passwd = conf[target_server_section]["passwd"]
target_port = conf[target_server_section]["port"]
target_db = conf[target_server_section]["db"]

target_conn = MySQLdb.connect(host=target_host, user=target_user, passwd=target_passwd, port = target_port, charset = 'utf8')
target_conn.autocommit(1)

target_conn.select_db(target_db)
target_cur = target_conn.cursor()
target_cur.execute("delete from trackingWebsite")
target_cur.executemany("""insert into trackingWebsite values(%s, %s, %s, %s, %s, %s, %s)""", result)

update_table = """
update SelfService_Aggregate_ByNoticedDate as a, trackingWebsite as b 
set a.WebsiteName = b.WebsiteName, a.Country = b.Country, a.Region = b.Region 
where a.trackingWebsite_id = b.trackingWebsite_id;
"""
target_cur.execute(update_table)

target_cur.close()
target_conn.close()


