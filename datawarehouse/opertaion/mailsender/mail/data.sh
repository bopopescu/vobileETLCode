me="/Job/datawarehouse/opertaion/mailsender/data/VTMetricsReports_`date -d -1day +%Y%m%d`.csv"

END_DATE="'"`date +%Y-%m-%d -d '1 days ago'`"'"
START_DATE="'"`date +%Y-%m-%d -d '8 days ago'`"'"
cd /opt/viewer/script/report/GlobalVDDB_RT31391/data_generator/data/
rm *.csv

ME_CON='-hhz-430 -u -p'

echo '' >  $me
mysql $ME_CON mddb_local_glb -e"set names utf8;select m.meta_uuid, m.duration 'mddb_duration',i.duration 'instance_duration',i.created_at 'instance_created' from mddb_local_glb.meta m,mediaContentInstance i where m.id=i.meta_id and m.created_at >=$START_DATE and m.created_at <$END_DATE;
;" >> $me
