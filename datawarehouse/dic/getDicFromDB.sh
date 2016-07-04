#!/bin/bash
#
hubble1=p2p-1-replica.c85gtgxi0qgc.us-west-1.rds.amazonaws.com
hubble2=p2p-2a.c85gtgxi0qgc.us-west-1.rds.amazonaws.com
hubble3=p2p-3-replica-02.c85gtgxi0qgc.us-west-1.rds.amazonaws.com
p2p=p2p-3-replica-02.c85gtgxi0qgc.us-west-1.rds.amazonaws.com
taisan=eqx-taisan-slave-db
taisanPost=eqx-taisanPost-slave-db
cyberlocker=eqx-cyberlocker-slave-db
colander=colander3.c85gtgxi0qgc.us-west-1.rds.amazonaws.com
mediawise_home=115.236.46.233 
#3308
mediawise_abroad=eqx-mediawise-slave-db
archTracker=eqx-archTracker2-slave-db
vtweb=eqx-vtweb-slave-db
insight=eqx-vDashboard-slave-db 
user=kettle 
passwd=k3UTLe
user=report
passwd=report
#port=3306
#host_list=($taisan $taisanPost $cyberlocker $colander $mediawise_home $mediawise_abroad $archTracker $vtweb $insight)
#host_name_list=(taisan taisanPost cyberlocker colander mediawise_home mediawise_abroad archTracker vtweb insight)
host_list=($hubble1 $hubble2 $hubble3)
host_name_list=(hubble1 hubble2 hubble3)
idx_host=0
for host in ${host_list[*]}
do
    if [ ${host} = "115.236.46.233" ];then
        port=3308
    else
        port=3306
    fi
    echo "mysql -h${host} -u${user} -p${passwd} -P${port}"    
    db_list=`mysql -h${host} -u${user} -p${passwd} -P${port} -e "show databases"`
    for db in ${db_list}
    do
        if [[ ${db} = "Database" || ${db} = "information_schema" || ${db} = "test" ]];then
            continue
        fi
        echo "mysql -h${host} -u${user} -p${passwd} -P${port} ${db}"
        table_list=`mysql -h${host} -u${user} -p${passwd} -P${port} ${db} -e "show tables"`
        idx=0
        for table in ${table_list}
        do
            idx=$(($idx+1))
            if [ $idx -eq 1 ];then
                continue
            fi
            echo ${table} >> ${host_name_list[${idx_host}]}.${db}
            echo "mysql -h${host} -u${user} -p${passwd} -P${port}"
            mysql -h${host} -u${user} -p${passwd} -P${port} information_schema  -e "select COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, COLUMN_COMMENT, EXTRA from COLUMNS where TABLE_SCHEMA = '${db}' and TABLE_NAME = '${table}'" >> ${host_name_list[${idx_host}]}.${db}
            echo >> ${host_name_list[${idx_host}]}.${db}
        done
    done    
    idx_host=$(($idx_host+1))
done
