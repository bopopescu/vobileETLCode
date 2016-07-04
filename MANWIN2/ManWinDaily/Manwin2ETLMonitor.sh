#!/bin/bash
#date: 2016-02-17
#author: cwj
#desc: monitor manwin2 daily report ETL, send mail to me about ETL runing status

function monitorETLFunc()
{
  local mysql_conf_114="-h192.168.110.114 -ukettle -pk3UTLe DM_MANWIN2"
  mysql ${mysql_conf_114} -e "select * from KLog where JOBNAME = '${1}' and date_format(ENDDATE, '%Y-%m-%d') = date_format(now(), '%Y-%m-%d')  order by ENDDATE desc limit 1\G" > /Job/MANWIN2/ManWinDaily/log/${1}.log

  if [ -n "$(grep -i error /Job/MANWIN2/ManWinDaily/log/${1}.log| grep -v 'ERRORS: 0')" ];then
         cat /Job/MANWIN2/ManWinDaily/log/${1}.log|mail -s "${1} ETL failed"$dateNow chen_weijie@vobile.cn
	 exit
      else
         echo "Good day, commander. ${1} OK. What a beautiful day."|mail -s "${1} ETL OK "$dateNow chen_weijie@vobile.cn
      fi
}

function monitorRFunc()
{
  if [ -n "$(grep -i error /Job/R/MANWIN2/log/genExcelManWin2${1}.log)" ];then
         cat /Job/R/MANWIN2/log/genExcelManWin2${1}.log|mail -s "R generate manwin2 daily report excel file failed ${dateNow}" chen_weijie@vobile.cn
         exit
  else
         echo "Good day, commander. R generate manwin2 daily report excel file OK. What a beautiful day."|mail -s "R generate excel file OK ${dateNow}" chen_weijie@vobile.cn
  fi 
}

