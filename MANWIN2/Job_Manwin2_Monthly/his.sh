cd /Job/MANWIN2/Job_Manwin2_Monthly_New/

echo "August"
bash Job_Manwin2_Monthly.sh

echo "July"
sed -i "s/interval 1 month/interval 2 month/g" Set_Manwin2_Monthly_Variable.ktr
sed -i "s/interval 0 month/interval 1 month/g" Set_Manwin2_Monthly_Variable.ktr
bash Job_Manwin2_Monthly.sh

