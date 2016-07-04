#!/bin/bash

#date: 20150901
#author: cwj



# SiteDetail_Monthly
bash /Job/VIACOM/SiteDetail_Monthly/SiteDetail_Monthly_alter_linking.sh
bash /Job/VIACOM/Torrent_Summary_Monthly/Torrent_Summary_Monthly.sh

# P2PISPSUM_Monthly
bash /Job/VIACOM/P2PISPSUM_Monthly/P2PISPSUM_Monthly.sh

# TitleDetail_Monthly
bash /Job/VIACOM/TitleDetail_Monthly/TitleDetail_Monthly.sh


# Top5TitleMonthly
bash  /Job/VIACOM/Top5TitleMonthly/Top5TitleMonthly.sh

#SiteDetail_Yearly
bash /Job/VIACOM/SiteDetail_Yearly/SiteDetail_Yearly_alter_linking.sh
bash /Job/VIACOM/Torrent_Summary_Yearly/Torrent_Summary_Yearly.sh

#TitleDetail_Yearly
bash /Job/VIACOM/TitleDetail_Yearly/TitleDetail_Yearly.sh

#P2PISPSUM_Yearly
bash /Job/VIACOM/P2PISPSUM_Yearly/P2PISPSUM_Yearly.sh


python /root/script/alexaSpider/linkingsite.py

bash  /Job/VIACOM/cwj_viacom_job/updateHostCountry.sh

bash /Job/VIACOM/cwj_viacom_job/updateYoutubeTakedown.sh

#bash /Job/VIACOM/cwj_viacom_job/mvViacomMonthData114To123.sh

# shell end
echo "viacom monthly report ETL is end"| mail -s "viacom monthly report ETL is end" chen_weijie@vobile.cn




