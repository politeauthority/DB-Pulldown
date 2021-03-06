#!/usr/bin/python                                                                                                
# Mysql DB Driver
# This driver simplifies some of the MySQL interactions

import MySQLdb as mdb

class DriverMysql( object ):

  def __init__( self, database = None ):
    if database:
      self.host     = database['host']
      self.user     = database['user']
      self.password = database['pass']
      try:
        self.dbname = database['name']
      except:
        self.dbname = '' 

  def execute( self, query, getdict = False ):
    return self.ex( query, getdict )

  def ex(self, query, getdict = False ):
    conn = mdb.connect( self.host, self.user, self.password)
    cur = conn.cursor()
    cur.execute( query )
    if getdict == True:
      columns = cur.description
      result = []
      for value in cur.fetchall():
        tmp = {}
        for (index,column) in enumerate(value):
          tmp[columns[index][0]] = column
          result.append( tmp )
    else:
      result = cur.fetchall()

    conn.commit()
    conn.close()
    return result

  # insert
  # @params
  #   table  : str()
  #   items  : dict{ 'column' : 'value' }
  def insert(self, table, items ):
    table   = self.__format_db_table_sql( table ) 
    columns = []
    values  = []
    for column, value in items.items():
      columns.append(column)
      values.append( str(value) )
    column_sql = ''
    for column in columns:
      column_sql = column_sql + "`%s`," % column
    column_sql = column_sql.rstrip( column_sql[-1:] )
    value_sql = ''
    for value in values:
      value_sql = value_sql + '"%s",' % self.escape_string( value )
    value_sql = value_sql.rstrip( value_sql[-1:] )

    sql = """INSERT INTO %s (%s) VALUES(%s);""" % ( table, column_sql, value_sql )
    self.ex( sql )
    return True

  def update( self, table, items, where, limit = 1 ):
    table = self.__format_db_table_sql( table )
    set_sql = ''
    for column, value in items.items():
      set_sql = set_sql + '`%s`="%s", ' % ( column, value )
    set_sql = set_sql.rstrip( set_sql[-2:] )
    where_sql = ''
    for column, value in where.items():
      where_sql = where_sql + '`%s`="%s" AND ' % ( column, value )
    where_sql = where_sql.rstrip( where_sql[-4:] )

    sql = """UPDATE %s SET %s WHERE %s LIMIT %s;""" % ( table, set_sql, where_sql, limit )
    # self.ex( sql )
    return sql

  def escape_string( self, string ):
    return mdb.escape_string( string )

  def alt_con( self, host, dbuser, dbpass, dbname = None ):
    self.host     = host
    self.dbname   = dbname
    self.user     = dbuser
    self.password = dbpass
    return True

  def list_to_string( self, the_list ):
    the_string = ''
    for item in the_list:
      the_string += '"%s", ' % item
    the_string = the_string[:-2]
    return the_string

  def __format_db_table_sql( self, db_or_table_or_both ):
    if '.' in db_or_table_or_both:
      the_sql = db_or_table_or_both.split('.')
      pretty  = """`%s`.`%s`""" % ( the_sql[0], the_sql[1] )
      print pretty
    else:
      if self.dbname != '':
        pretty = """`%s`.`%s`""" % ( self.dbname, db_or_table_or_both )
      else:
        pretty = """`%s`""" % ( db_or_table_or_both )
    return pretty

# End File: __/drivers/DriverMysql