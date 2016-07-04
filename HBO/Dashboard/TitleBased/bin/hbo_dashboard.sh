#!/bin/bash
#

source /etc/profile

python /Job/HBO/Dashboard/TitleBased/bin/extractDataFromVTWeb.py
python /Job/HBO/Dashboard/TitleBased/bin/titleBased_trackingWebsite.py
python /Job/HBO/Dashboard/TitleBased/bin/titleBased_meta.py
python /Job/HBO/Dashboard/TitleBased/bin/titleBased_remove.py
python /Job/HBO/Dashboard/TitleBased/bin/titleBased.py
python /Job/HBO/Dashboard/TitleBased/bin/titleBased_titlebased1.py
python  /Job/HBO/Dashboard/TitleBased/bin/titleBased_matchedVideoViewCountCompletionAll.py
python  /Job/HBO/Dashboard/TitleBased/bin/titleBased_infringAllViews.py
python /Job/HBO/Dashboard/TitleBased/bin/siteBased.py
python /Job/HBO/Dashboard/TitleBased/bin/siteBased_alexa.py
python /Job/HBO/Dashboard/TitleBased/bin/siteBased_updateAlexa.py
python /Job/HBO/Dashboard/TitleBased/bin/monitorComplianceRate.py
python /Job/HBO/Dashboard/TitleBased/bin/country_based.py
python /Job/HBO/Dashboard/TitleBased/bin/titleBased_siteBased_updateAllDim.py
