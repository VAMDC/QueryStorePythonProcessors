# -*- coding: utf-8 -*-
import sys
import subprocess
import MySQLdb
import datetime
import dateutil
import glob
import os
from dateutil.relativedelta import relativedelta


basename = "query_store"
user = "PUT_DB_USER_HERE"
password = "PUT_DB_PASSWORD_HERE"
host = "127.0.0.1"
port = 3306
    
databaseConnection = None 
cursor = None 


class Database:
    def __init__(self,cursor,connection):
        self.cursor=cursor
        self.connection=connection


def set_connection():
    """open a mysql connection (MariaDB)"""
    global databaseConnection, cursor, basename
    databaseConnection = MySQLdb.connect(host, user, password, basename,port)
    cursor = MySQLdb.cursors.DictCursor(databaseConnection)     
    #cursor.execute("SET UNIQUE_CHECKS=0")
    cursor.execute("SET FOREIGN_KEY_CHECKS=0")
    cursor.execute("SET NAMES UTF8")
    cursor.execute("SET AUTOCOMMIT=0")
    cursor.execute("SET sql_mode='ANSI'")    # enable ansi mode for portability
    database = Database(cursor,databaseConnection)
    return database
    

def close_connection():
    global databaseConnection, cursor, basename
    cursor.close()
    basename = None
    databaseConnection = None 
    cursor = None 


def getTimes():
    """return the timestamp in Unix millesecond of One and Two years ago from now"""
    timenow = datetime.datetime.now()
    timestampOneYearAgo = timenow - relativedelta(years=1)
    timestampTwoYearAgo = timenow - relativedelta(years=2)
    

    timestampOneYearAgo = round(timestampOneYearAgo.timestamp()*1000)
    timestampTwoYearAgo = round(timestampTwoYearAgo.timestamp()*1000)


    return timestampOneYearAgo, timestampTwoYearAgo


def returnUUIDFilesToDelete(database, supTimeStamp):
    sql = 'select UUID from QueryUserLink GROUP BY UUID HAVING Max(timestamp) < \'%s\' and UUID in (select UUID FROM Queries where Queries.Doi is NULL and Queries.dataURL is not NULL)' % (supTimeStamp) 
    print(sql)
    database.cursor.execute(sql)
    result = []
    for e in cursor.fetchall():
        result.append(e["UUID"])
    return result


def returnErrorsUUIDToDelete(database,supTimestamp):
    sql = 'select UUID from Errors where Timestamp < \'%s\' ' % (supTimestamp)
    database.cursor.execute(sql)
    result = []
    for e in cursor.fetchall():
        result.append(e["UUID"])
    return result


def cleanErrorTable(database, supTimestamp):
    sql = 'delete from Errors where Timestamp < \'%s\' ' % (supTimestamp)
    print(sql)
    database.cursor.execute(sql)
    database.connection.commit()


def removeInDBLinkUUIDFile(database, UUID):
    sql = 'Update Queries set dataURL = NULL WHERE UUID like \'%s\' ' % (UUID)
    print(sql)
    database.cursor.execute(sql)
    database.connection.commit()


def cleanQueriesTableInDB(database,listUUIDToUpdate):
    for uuid in listUUIDToUpdate:
        removeInDBLinkUUIDFile(database,uuid)


def removeFilesByUUID(listUUIDToRemove):
    for uuid in listUUIDToRemove:
        
        patternXSAMSToRemove="/xsams/**/"+uuid+".xsams"

        files = glob.glob(patternXSAMSToRemove,recursive=True)
        for f in files:
            try:
                print("removing file " +f)
                os.remove(f)
            except OSError as e:
                print("Error: %s : %s" % (f, e.strerror))   

        patternZipToRemove = "/xsams/**/"+uuid+".zip"
        files = glob.glob(patternZipToRemove,recursive=True)
        for f in files:
            try:
                print("removing file " +f)
                os.remove(f)
            except OSError as e:
                print("Error: %s : %s" % (f, e.strerror))  



def main ():
    #open mysql connection
    database = set_connection()

    #Getting the timestamps for one and two years ago
    timestampOneYear, timestampTwoYears= getTimes()
    
    #processing the errors
   
    #build the list of UIUD of errors having more than one year
    listUUIDError = returnErrorsUUIDToDelete(database,timestampOneYear)
    print(listUUIDError)

    #remove the associated error files
    removeFilesByUUID(listUUIDError)
   
    #remove the entry for Errors from the database
    cleanErrorTable(database,timestampOneYear)


    #processing the queries with no errors, 
   
    #build the list of queries which are older than two years (provided no DOI is attached)
    listUUIDToDelete = returnUUIDFilesToDelete(database,timestampTwoYears)
    print(listUUIDToDelete)


    #remove the associated files, which are too olds
    removeFilesByUUID(listUUIDToDelete)

    #update the database, to remove link between queries and files
    cleanQueriesTableInDB(database, listUUIDToDelete)


    #closing the connection to the database
    database.cursor.close()
    


if __name__ == '__main__':
    main()
