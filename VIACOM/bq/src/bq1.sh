#!/bin/bash
#


function query()
{   
    bq query "select * from test.yobb limit 10"    
}


function executeFunc()
{
    execute_date=`date -d now "+%Y%m%d%H%M%S"`
    eval $1 >> $1.log.${execute_date}
    run_status=$?
    execute_times=1
    while [ $run_status -ne 0 ] 
    do
        eval $1 >> $1.log.${execute_date}
        run_status=$?
        cat query.log.${execute_date}|mail -s "big query test" chen_weijie@vobile.cn

        ((++execute_times))
        #execute_times=$(($execute_times+1))
        echo $execute_times
        if [ $execute_times -gt 3 ];then
            break
        fi  
    done
}

executeFunc "query"
