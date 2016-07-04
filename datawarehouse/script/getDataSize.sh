taisan=eqx-taisan-slave-db
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
#port=3306
host_list=($taisan $cyberlocker $colander $mediawise_home $mediawise_abroad $archTracker $vtweb $insight)
host_name_list=(taisan cyberlocker colander mediawise_home mediawise_abroad archTracker vtweb insight)
idx_host=0


# hubble
mysql -hp2p-3-replica-02.c85gtgxi0qgc.us-west-1.rds.amazonaws.com -ureport -preport << EOF > hubble-contactDB.dic
  SELECT table_name, 
              TABLE_ROWS,           
              round(SUM(data_length+index_length)/1024/1024/1024) AS "total(G)", 
              round(SUM(data_length)/1024/1024/1024) AS "data(G)",            
              round(SUM(index_length)/1024/1024/1024) AS "index(G)"  
FROM     information_schema.tables  
where information_schema.tables.TABLE_SCHEMA = 'contactDB'   
GROUP BY 1 ORDER BY 3 DESC;
exit
EOF

mysql -hp2p-3-replica-02.c85gtgxi0qgc.us-west-1.rds.amazonaws.com -ureport -preport << EOF > hubble-p2pwarehouse.dic
  SELECT table_name, 
              TABLE_ROWS,           
              round(SUM(data_length+index_length)/1024/1024/1024) AS "total(G)", 
              round(SUM(data_length)/1024/1024/1024) AS "data(G)",            
              round(SUM(index_length)/1024/1024/1024) AS "index(G)"  
FROM     information_schema.tables  
where information_schema.tables.TABLE_SCHEMA = 'p2pwarehouse'   
GROUP BY 1 ORDER BY 3 DESC;
exit
EOF

