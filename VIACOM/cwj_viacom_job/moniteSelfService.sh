#!/bin/bash

#Date: 20150901
#Author: cwj



while :
 do
	if [ -z "$(ps aux | grep SelfService_Aggregate_ByNoticedDate_Test.sh|grep -v grep)" ]
        then
		bash /Job/VIACOM/cwj_viacom_job/viacom_monthly.sh	
		break
	fi
   	sleep 20
  done





