#!/bin/bash
#

source /etc/profile

python /Job/FOX/Dashboard/TitleBased/bin/extractDataFromVTWeb.py
python /Job/FOX/Dashboard/TitleBased/bin/titleBased_trackingWebsite.py
python /Job/FOX/Dashboard/TitleBased/bin/titleBased_meta.py
python /Job/FOX/Dashboard/TitleBased/bin/titleBased_remove.py
python /Job/FOX/Dashboard/TitleBased/bin/titleBased.py
python /Job/FOX/Dashboard/TitleBased/bin/titleBased_titlebased1.py
python  /Job/FOX/Dashboard/TitleBased/bin/titleBased_matchedVideoViewCountCompletionAll.py
python  /Job/FOX/Dashboard/TitleBased/bin/titleBased_infringAllViews.py
python /Job/FOX/Dashboard/TitleBased/bin/siteBased.py
python /Job/FOX/Dashboard/TitleBased/bin/siteBased_alexa.py
python /Job/FOX/Dashboard/TitleBased/bin/siteBased_updateAlexa.py
python /Job/FOX/Dashboard/TitleBased/bin/monitorComplianceRate.py
python /Job/FOX/Dashboard/TitleBased/bin/country_based.py
python /Job/FOX/Dashboard/TitleBased/bin/titleBased_siteBased_updateAllDim.py
