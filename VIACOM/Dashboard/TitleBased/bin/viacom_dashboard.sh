#!/bin/bash
#

source /etc/profile

python /Job/VIACOM/Dashboard/TitleBased/bin/extractDataFromVTWeb.py
python /Job/VIACOM/Dashboard/TitleBased/bin/titleBased_trackingWebsite.py
python /Job/VIACOM/Dashboard/TitleBased/bin/titleBased_meta.py
python /Job/VIACOM/Dashboard/TitleBased/bin/titleBased_remove.py
python /Job/VIACOM/Dashboard/TitleBased/bin/titleBased.py
python /Job/VIACOM/Dashboard/TitleBased/bin/titleBased_titlebased1.py
python /Job/VIACOM/Dashboard/TitleBased/bin/titleBased_CMS.py
python  /Job/VIACOM/Dashboard/TitleBased/bin/titleBased_matchedVideoViewCountCompletion.py
#python  /Job/VIACOM/Dashboard/TitleBased/bin/titleBased_matchedVideo.py
python  /Job/VIACOM/Dashboard/TitleBased/bin/titleBased_matchedVideoViewCountCompletionAll.py
#python  /Job/VIACOM/Dashboard/TitleBased/bin/titleBased_infringViews.py
python  /Job/VIACOM/Dashboard/TitleBased/bin/titleBased_infringAllViews.py

python /Job/VIACOM/Dashboard/TitleBased/bin/siteBased.py

#python /Job/VIACOM/Dashboard/TitleBased/bin/siteBased_alexa.py

python /Job/VIACOM/Dashboard/TitleBased/bin/siteBased_updateAlexa.py
python /Job/VIACOM/Dashboard/TitleBased/bin/titleBased_siteBased_updateAllDim.py
python /Job/VIACOM/Dashboard/TitleBased/bin/monitorComplianceRate.py
