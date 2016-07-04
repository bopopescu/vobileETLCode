#!/usr/bin/env python
# author: 
# date: 2016-03-11
# desc: mysql connection
#coding:utf8
#

import MySQLdb

class MySQLHelper():
    def __init__(self, host, user, passwd, db_name, port = 3306, charset = 'utf8'):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.port = 3306
        self.db_name = db_name
        self.charset = charset
        #self.conn = MySQLdb.connect(host = self.host, user = self.user, passwd = self.passwd, \
        #        db = self.db_name, port = self.port, charset = self.charset)
        #self.conn.ping(True)
        #self.cur = self.conn.cursor()

        def connect(self):
            time = 0
            while True:
                try:
                    self.conn = MySQLdb.connect(host = self.host, user = self.user, passwd = self.passwd, \
                                                db = self.db_name, port = self.port, charset = self.charset)
                    self.conn.ping(True)
                    self.cur = self.conn.cursor()
                    return 1
                except Exception, e:
                    print e
                time += 1

                if time == 3:
                    break
        connect(self)


    def executeManyCMD(self, sql, data):
        self.cur.executemany(sql, data)

    def insertUpdateCMD(self, sql, data):
        self.cur.executemany(sql, data)

    def queryCMD(self, sql):
        self.cur.execute(sql)

        return self.cur.fetchall()

    def queryNoData(self, sql):
        self.cur.execute(sql)

    def commit(self):
        self.conn.commit()

    def closeConn(self):
        self.conn.close()


    def closeCur(self):
        self.cur.close()
