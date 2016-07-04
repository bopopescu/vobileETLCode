#!/bin/bash


source /etc/profile

bash /root/Job/SelfService_Aggregate_ByNoticedDate_Test/SelfService_Aggregate_ByNoticedDate_Test.sh
bash /Job/VIACOM/cwj_viacom_job/viacom_monthly_mongo_v1.sh
