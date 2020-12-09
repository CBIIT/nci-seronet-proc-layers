# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 12:48:25 2020

@author: w1997
"""

import mysql.connector
from mysql.connector import errorcode

def connectToDB(user, password, host, database):
    try:
      cusor = mysql.connector.connect(user=user, host=host, password=password, database=database)
    except mysql.connector.Error as err:
      raise err
    else:
      print("connect successfully")
      return cusor
      #cnx.close()

def executeDB(mydb,execution):
    mydbCursor=mydb.cursor()
    try:
        mydbCursor.execute(execution)
        mydb.commit()
    except mysql.connector.Error as err:
        raise err
    else:
        print("execute successfully")