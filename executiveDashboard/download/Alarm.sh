cd /Job/executiveDashboard/download

#wget --load-cookies cookies --save-cookies cookies --keep-session-cookies -O login.html --post-data "username=sun_cong&password=tynOmmuv7" http://206.99.94.88:8080/opm_web/GetAllAlarmPointAction.action?idc=EQX
#wget --load-cookies cookies --save-cookies cookies --keep-session-cookies -O OPM.html http://206.99.94.88:8080/opm_web/GetAllAlarmPointAction.action?idc=EQX

wget --load-cookies cookies -O OPM.html http://206.99.94.88:8080/opm_web/

python /Job/executiveDashboard/src/stat_alarm.py
