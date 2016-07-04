#!/bin/bash

python /Job/datawarehouse/opertaion/script/DashboardETL/Low_compliant_sites.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/matches_UGC.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/notice_UGC.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/matches_hybrid.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/notice_hybrid.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/matches_cyberlocker.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/notice_cyberlocker.py
python /Job/datawarehouse/opertaion/script/DashboardETL/matches_linkingsites.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/notice_linkingsites.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/matches_SearchEngine.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/notice_SearchEngine.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/infringing_facebook.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/infringing_baidupan.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/infringing_VK.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/infringing_youtube.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/P2P_IPs.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/P2P_Notices_Sent.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/matches_All.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/complianceUGC.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/complianceHybrid.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/complianceCyberlocker.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/AggregateAll.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/genrate_Excel.py 
python /Job/datawarehouse/opertaion/script/DashboardETL/sendMail.py 
