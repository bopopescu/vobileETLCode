#!/bin/bash
#Date: 2015-09-10
#author: 


#generate domain list from table websiteDomain
mysql_conf_123=" -h54.67.114.123 -ukettle -pkettle DM_MANWIN2 "
list=`mysql $mysql_conf_123 -e "select websiteDomain as '' from websiteDomainList"`
i=1
domainList=''
len=`echo $list|wc -w`
count=0
for domain in $list
    do
        count=$(($count+1))
        if [ 1 -eq $count ];then
            domain1="$domain', "
            domainList=$domainList$domain1
        elif [ $len -ne $count ];then
            domain1="'$domain', "
            domainList=$domainList$domain1
        else
            domain1="'$domain"
            domainList=$domainList$domain1
        fi
    done

echo $domainList
