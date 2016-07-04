#!/bin/bash

#generate data
python /Job/datawarehouse/opertaion/script/DashboardETL/Low_compliant_sites.py & 
python /Job/datawarehouse/opertaion/script/DashboardETL/matches_UGC.py &
python /Job/datawarehouse/opertaion/script/DashboardETL/matches_hybrid.py &
python /Job/datawarehouse/opertaion/script/DashboardETL/matches_cyberlocker.py &
python /Job/datawarehouse/opertaion/script/DashboardETL/matches_linkingsites.py &
python /Job/datawarehouse/opertaion/script/DashboardETL/matches_SearchEngine.py &
python /Job/datawarehouse/opertaion/script/DashboardETL/matches_P2P.py &
python /Job/datawarehouse/opertaion/script/DashboardETL/matches_All.py &
python /Job/datawarehouse/opertaion/script/DashboardETL/complianceUGC.py &
python /Job/datawarehouse/opertaion/script/DashboardETL/complianceHybrid.py &
python /Job/datawarehouse/opertaion/script/DashboardETL/complianceCyberlocker.py &
python /Job/datawarehouse/opertaion/script/DashboardETL/AggregateAll.py &


#send generated data 
python /opt/viewer/script/report/GlobalVDDB_RT31391/mail_sender/mail_sender.py --subject="VT Operation Report for All Account_`date -d -1day +%Y%m%d`" --attachment="/Job/datawarehouse/opertaion/script/DashboardETL/`date -d -1day +%Y%m%d`.csv"

