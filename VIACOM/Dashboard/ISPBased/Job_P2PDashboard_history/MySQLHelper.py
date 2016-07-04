import MySQLdb

class MySQLHelper:
  def __init__(self,host,user,password,db_name,charset="utf8"):
    self.host = host
    self.user = user
    self.password = password
    self.db_name = db_name
    self.charset = charset
    self.conn = MySQLdb.connect(host=self.host,user=self.user,passwd=self.password,db=self.db_name, charset=self.charset)
    self.conn.ping(True)

  def insert_sql_cmd(self,sql_cmd):
    self.cur = self.conn.cursor()
    try :
      self.cur.execute(sql_cmd)
      self.conn.commit()
      self.cur.close()
      return True
    except Exception, e:
      print "[MYSQL ERROR] : %s"%sql_cmd
      print "%s"%(str(e))
      self.cur.close()
      return False

  def update_sql_cmd(self,sql_cmd):
    self.cur = self.conn.cursor()
    try :
      self.cur.execute(sql_cmd)
      self.conn.commit()
      self.cur.close()
      return True
    except Exception, e:
      print "[MYSQL ERROR] : %s"%sql_cmd
      print "%s"%(str(e))
      self.cur.close()
      return False

  def query_sql_cmd(self,sql_cmd):
    try :
      self.cur = self.conn.cursor()
      self.cur.execute(sql_cmd)
      res = self.cur.fetchall()
      #self.cur.close()
      return res
    except Exception, e:
      print "[MYSQL ERROR] : %s"%sql_cmd
      print "%s"%(str(e))
      self.cur.close()
      return False

  def close(self):
    self.conn.close()
