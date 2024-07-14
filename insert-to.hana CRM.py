import time
import pandas as pd
import numpy as np
import sys
from hdbcli import dbapi

def getConnection():
    return dbapi.connect(address="s1.learn.c.eu-de-1.cloud.sap", port=30015 , user="I533045", password="Luis2022!", encrypt = "True" )

def insertIntoTable(tableName, data):
    try:
        conn = getConnection()
        cursor = conn.cursor()

        #print ("Eliminando la tabla")
        #sql = "DELETE FROM %s" % tableName
        #cursor.execute(sql)

        parms = ("?," * len(data[0]))[:-1]
        sql = "INSERT INTO %s VALUES(%s);" % (tableName, parms)

        print ("Cargando los registros")
        cursor.executemany(sql, data)

        conn.commit()
    finally:
        if conn is not None:
            if conn.isconnected:
                cursor.close()
                conn.close()

start_time = time.time()

df = pd.read_csv("C:/Users/I533045/Desktop/DATA ANALYST 2023/imp_tblAPICourseTagsL2MS.csv", na_filter=False).replace(to_replace="#NA", value='') #subir CSV

# data_frame = pd.read_excel("C:/Users/I502177/Desktop/DDC Project/CRM Backups/LIC + SVC/GTM SVC 08132022.xlsx", na_filter=False , sheet_name = 'Sheet1').replace(to_replace="#NA", value='')


print(df)

tuples = [tuple(x) for x in df.values]

insertIntoTable("\"SL_DATA\".\"imp_tblAPICourseTagsL2MS\"", tuples)
            
print("successfully executed in %.2f seconds" % (time.time() - start_time))