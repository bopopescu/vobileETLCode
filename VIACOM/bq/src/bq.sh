#!/bin/bash
#


function query()
{   
    bq query "select * from test.yobb limit 10"    
}

query
run_status=$?
query_times=1
while [ $run_status -ne 0 ]
do
    query > query.log
    run_status=$?
    cat query.log|mail -s "big query test" chen_weijie@vobile.cn

    #query_time=$(($query_time+1))
    echo $query_times
    if [ $query_times -gt 3 ];then
        break
    fi
    ((++query_times))
done
