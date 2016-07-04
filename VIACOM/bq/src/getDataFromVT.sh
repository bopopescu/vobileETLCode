#!/bin/bash 

#get data from vtweb

conf_vt="mysql -heqx-vtweb-master-db -ukettle -pk3UTLe "

today=`date -d now +"%Y%m%d"`
${conf_vt} tracker2 -e "set names utf8;select * from matchedVideo where company_id = 10 and hide_flag=2 " > /Job/VIACOM/bq/data/matchedVideo${today}

#sed -i "s/$/v/g"  /Job/VIACOM/bq/data/matchedVideo${today}
#sed -i "s/^/v/g" /Job/VIACOM/bq/data/matchedVideo${today}

bq load --quote=v --skip_leading_rows=1 --field_delimiter='\t' test.c /Job/VIACOM/bq/data/matchedVideo${today}
