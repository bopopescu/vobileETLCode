#!/bin/bash
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
HASHFILE=/Job/datawarehouse/opertaion/script/phy_md5
TARGET=/Job/datawarehouse/opertaion/file/phy_cpuinfo
HASH=$(curl -u vobile:boijVegNot1 --data DATA http://216.151.23.3/sysinfo/index.php)
if [ -f $HASHFILE ]
then
    OLDHASH=$(cat $HASHFILE)
else
    OLDHASH='NOHASH'
fi

if [ "$HASH" != "$OLDHASH" ]
then
    curl -u vobile:boijVegNot1 --b http://216.151.23.3/sysinfo/index.php > $TARGET
    echo "$HASH" > $HASHFILE
    service squid3 reload
fi
