#!/bin/bash

#Date: 2016-03-02
#author: duli

yesterday=`date -d "1 days ago" +%Y%m%d`
title="YouTube Daily Match Summary - Viacom(${yesterday})"
content="Dear Viacom team,

Please find attached the daily YouTube export for ${yesterday}. If you have any questions please contact the Vobile support team at support@vobileinc.com

Thank you.
 
Copyright(c) 2016 Vobile, Inc. All rights reserved"
from="reports@vobileinc.com"
to="support@vobileinc.com,Michael.Housley@viacom.com,deborah.robinson@viacom.com,paul.jackson@viacom.com"
bcc="du_li@vobile.cn,gs_po@vobile.cn"
#to="du_li@vobile.cn"
#bcc="du_li@vobile.cn"

file="YouTube_Daily_Match_Summary_${yesterday}.xls"
attachment="YouTube_Daily_Match_Summary_${yesterday}.zip"

cd /YouTube
zip -r ${attachment} ${file}

(echo "${content}";uuencode "${attachment}" "${attachment}")|mail -s "${title}" -t "${to}" -a From:"${from}" -a bcc:"${bcc}"
