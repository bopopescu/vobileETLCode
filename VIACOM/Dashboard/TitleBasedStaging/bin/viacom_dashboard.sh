#!/bin/bash
#

source /etc/profile

python /Job/VIACOM/Dashboard/TitleBasedStaging/bin/titleBased_remove.py
python /Job/VIACOM/Dashboard/TitleBasedStaging/bin/titleBased.py
python /Job/VIACOM/Dashboard/TitleBasedStaging/bin/titleBased_meta.py
python /Job/VIACOM/Dashboard/TitleBasedStaging/bin/titleBased_trackingWebsite.py
python /Job/VIACOM/Dashboard/TitleBasedStaging/bin/titleBased_titlebased1.py
python /Job/VIACOM/Dashboard/TitleBasedStaging/bin/titleBased_views.py
python /Job/VIACOM/Dashboard/TitleBasedStaging/bin/titleBased_CMS.py
python /Job/VIACOM/Dashboard/TitleBasedStaging/bin/siteBased.py
#bash 
#python /Job/VIACOM/Dashboard/TitleBasedStaging/bin/siteBased_updateAlexa.py

