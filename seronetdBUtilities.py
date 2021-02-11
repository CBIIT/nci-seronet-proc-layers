import mysql.connector

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
