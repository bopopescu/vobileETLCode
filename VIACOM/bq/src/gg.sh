#!/bin/bash 

#get data from vtweb

conf_vt="mysql -heqx-vtweb-master-db -ukettle -pk3UTLe "

today=`date -d now +"%Y%m%d"`
${conf_vt} tracker2 -e "set names utf8;select * from matchedVideo where company_id = 14 and hide_flag!=2 " > /Job/VIACOM/bq/data/noinfring

