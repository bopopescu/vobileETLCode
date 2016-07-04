#!/bin/bash
#
mysql_conf_114="mysql -h192.168.110.114 -ukettle -pk3UTLe  "

${mysql_conf_114} DM_VIACOM -e "update SiteDetail_Monthly set TakedownNum = InfringingNum where  TakedownNum/InfringingNum *100 > 100;
update SiteDetail_Yearly set TakedownNum = InfringingNum where  TakedownNum/InfringingNum *100 > 100;"
