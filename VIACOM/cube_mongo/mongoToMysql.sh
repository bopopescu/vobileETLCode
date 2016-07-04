#!/bin/bash
#2015-12-23
#


mysql_conf_114="mysql -h192.168.110.114 -ukettle -pk3UTLe DM_VIACOM "
start_date=`date -d "now 1 days ago" +%Y-%m-%d`
end_date=`date -d "now" +%Y-%m-%d`
first_day_of_last_month=`date -d "now 1 month ago" +%Y-%m-01`
first_day_of_the_month=`date -d "now" +%Y-%m-01`
#start_date=$first_day_of_the_month
today=`date -d "now" +%Y-%m-%d`

#delete
${mysql_conf_114} -e "delete from matchedVideoEstimated where report_at >= '${start_date}' and report_at < '${end_date}';"
${mysql_conf_114} -e "delete from matchedVideoViewCompletion where report_at >= '${start_date}' and report_at < '${end_date}';"

## mongo to mysql
python /Job/VIACOM/cube_mongo/mongoToMysql.py ${start_date} ${end_date}
echo "##############################################################################"

#aggregate matchedVideoEstimated to matchedVideoEstimatedDaily
#delete
${mysql_conf_114} -e "delete from matchedVideoEstimatedDaily where report_at >= '${start_date}' and report_at < '${end_date}'"

#insert
${mysql_conf_114} -e "insert into matchedVideoEstimatedDaily (report_at, YM, trackingMeta_id, trackingWebsite_id, website_type, estimated_viewcount, ETL_DTE) select date_format(report_at, '%Y-%m-%d') as report_at,  date_format(report_at, '%Y-%m') as YM, trackingMeta_id, trackingWebsite_id, website_type, sum(estimated_viewcount) as estimated_viewcount, current_timestamp as ETL_DTE from matchedVideoEstimated where report_at >= '${start_date}' and report_at < '${end_date}' group by 1, 2, 3, 4, 5"

##aggregate matchedVideoViewCompletion to matchedVideoViewCompletionDaily
#delete
${mysql_conf_114} -e "delete from matchedVideoViewCompletionDaily where report_at >= '${start_date}' and report_at < '${end_date}'"

#insert
${mysql_conf_114} -e "insert into matchedVideoViewCompletionDaily (report_at, YM, trackingMeta_id, trackingWebsite_id, website_type, view_count, ETL_DTE) select date_format(report_at, '%Y-%m-%d') as report_at,  date_format(report_at, '%Y-%m') as YM, trackingMeta_id, trackingWebsite_id, website_type, sum(view_count) as view_count, current_timestamp as ETL_DTE from matchedVideoViewCompletion where report_at >= '${start_date}' and report_at < '${end_date}' group by 1, 2, 3, 4, 5"

##aggregate matchedVideoEstimated to matchedVideoEstimatedMonthly and delete last month data from matchedVideoViewCompletion
if [ ${today} = ${first_day_of_the_month} ];then
    ${mysql_conf_114} -e "insert into matchedVideoEstimatedMonthly (YM, trackingMeta_id, trackingWebsite_id, website_type, estimated_viewcount, ETL_DTE) select YM, trackingMeta_id, trackingWebsite_id, website_type, sum(estimated_viewcount) as estimated_viewcount,  current_timestamp as ETL_DTE from matchedVideoEstimatedDaily where website_type = 'cyberlocker' and report_at >= '${first_day_of_last_month}' and report_at < '${first_day_of_the_month}' group by 1, 2, 3, 4;"
    #delete last month data matchedVideoViewCompletion
    if [ $? -eq 0 ];then
        ${mysql_conf_114} -e "delete from matchedVideoViewCompletion where  where report_at >= '${first_day_of_last_month}' and report_at < '${first_day_of_the_month}'"
    fi
fi

echo "mysql to mongo script run end ${start_date} to ${end_date} not including ${end_date}"|mail -s "mongo to mysql" chen_weijie@vobile.cn
