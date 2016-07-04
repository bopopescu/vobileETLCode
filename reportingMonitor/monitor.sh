#!/bin/bash

PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

function check_sn {
    ACTION=$1
    error=$(curl -u 'ops:7d6ba00824cca76f' --basic "http://dev-eq.vobile.net/render/?width=1057&height=565&target=taisan.eqx.sn_msn.di_${ACTION}_err&from=-10minutes&format=csv" 2>/dev/null | awk -F , '{print $NF}' | numgrep '/0.05..1/' | wc -l)
    
    if [ "$error" -eq 0 ]
    then
        echo "[$(date)] ${ACTION}: heartbeat"
        /root/xzw/bin/heartbeat.py dbpc.ops.vobile.org 5800 system storage.sn-ec2.${ACTION}  50.18.107.66
    else
        echo "[$(date)] ${ACTION}: heartbreak"
    fi
}

function check_msn {
    error=$(curl -u 'ops:7d6ba00824cca76f' --basic "http://dev-eq.vobile.net/render/?width=1057&height=565&target=taisan.eqx.sn_msn.di_2047_err&from=-10minutes&format=csv" 2>/dev/null | awk -F , '{print $NF}' | numgrep '/0.05..1/' | wc -l)
    
    if [ "$error" -eq 0 ]
    then
        echo "[$(date)] msn: heartbeat"
        /root/xzw/bin/heartbeat.py dbpc.ops.vobile.org 5800 system storage.msn-ec2  50.18.107.66
    else
        echo "[$(date)] msn: heartbreak"
    fi
}

check_sn upload
check_sn download
check_msn

