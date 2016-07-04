#!/bin/bash
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
HASHFILE=/Job/executiveDashboard/Tmp_file/md5
TARGET=/Job/executiveDashboard/Tmp_file/ec2.list
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

