#!/bin/bash
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
HASHFILE=/Job/datawarehouse/opertaion/script/ec2_md5
TARGET=/Job/datawarehouse/opertaion/file/cpuinfo
HASH=$(curl -u vobops:dodcodyoid --digest http://ec2info.ops.vobile.org/aws/inst_list.md5)
if [ -f $HASHFILE ]
then
    OLDHASH=$(cat $HASHFILE)
else
    OLDHASH='NOHASH'
fi

if [ "$HASH" != "$OLDHASH" ]
then
    curl -u vobops:dodcodyoid --digest http://ec2info.ops.vobile.org/aws/inst_list > $TARGET
    echo "$HASH" > $HASHFILE
    service squid3 reload
fi
