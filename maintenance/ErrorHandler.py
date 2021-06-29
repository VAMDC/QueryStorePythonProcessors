#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import MySQLdb
import subprocess
import time
import os
import sys
import logging
import zipfile
import shutil
import datetime

import smtplib 
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.base import MIMEBase 
from email import encoders 

basename = "query_store"
user = "PUT_DB_USER_HERE"
password = "PUT_DB_PASSWORD_HERE"
host = "127.0.0.1"
port = 3306    

XSAMS_ERROR_DEST_FOLDER = "/xsams/error"
XSAMS_SOURCE_FOLDER = "/xsams/sources"

databaseConnection = None 
cursor = None 

fileSeparator=";"

class ErrorDetail:
    def __init__(self,uuid,token,errorMessage,parameter,timestamp):
        self.uuid=uuid
        self.token=token
        self.errorMessage=errorMessage
        self.parameter=parameter
        self.timestamp = timestamp


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



def find_files_in_error(database):
    """list queries that are in error and have not already been notified to the node curator"""
    sql = "SELECT UUID, Token, ErrorMessage, Parameters, Timestamp FROM Errors where Notified is null order by timestamp asc"
    database.cursor.execute(sql)
    result = []
    for e in cursor.fetchall():
        currentError = ErrorDetail(e["UUID"],e["Token"],e["ErrorMessage"],e["Parameters"],e["Timestamp"])
        result.append(currentError)
    return result


def getNodeFromError(errors):
    """build the list of nodes having errors (the elements of this list are not unique)"""
    toReturn = []
    for error in errors:
        nodeName = error.token.split(":")[0]
        toReturn.append(nodeName)
    return toReturn

def buildErrorMapping(errors):
    """build a map containing all the errors for a given node. The node name is the key of the map"""
    rawNodeList = getNodeFromError(errors)
    nodeMap = {node: [] for node in rawNodeList}
    for error in errors:
        nodeName = error.token.split(":")[0]
        nodeMap[nodeName].append(error)
    return nodeMap


def zip_file(sourceFile, destFile, uuid):
    """create a zip archive of the xsams file"""
    zip = zipfile.ZipFile(destFile, 'w', zipfile.ZIP_DEFLATED)
    zip.write(sourceFile, uuid + ".xsams")
    zip.close()
    

def updateNotifications(database, error):
    """when the files are notified, add the notified tag into errors into the DB"""
    sql = 'Update Errors set Notified =\'%s\' WHERE UUID like \'%s\' ' % (1, error.uuid)
    database.cursor.execute(sql)


def processFilesInError(nodemap, database):
    nodeMailMap = buildNodeMailMap()
    
    #loop over all the nodes
    for node in nodemap.keys():
        # defining the address of the people in charge of the node
        nodeCuratorMail = nodeMailMap[node]

        #creating a folder for each node, if this does not exist
        folderName = XSAMS_ERROR_DEST_FOLDER +"/"+ node
        directoryAlreadyExist = os.path.isdir(folderName)
        if (not directoryAlreadyExist):
            os.makedirs(folderName)

        # creating a csv file for putting the resume of the errors
        timestamp = str(int(round(time.time()*1000)))
        errorResumeFileName = node + timestamp +".csv"
        errorResumeFile = open(errorResumeFileName,'w')

        #writing header for the errorResumeFile
        errorResumeFile.write("query_uuid ; query_token ; timestamp ;query ; error_message ; query_produced_xsams \n")

        isMailToSend = False

        #loop over all the errors for a given node
        for error in nodemap[node]:
            # check if regular file exist
            errorFilePath = XSAMS_SOURCE_FOLDER + "/" + error.uuid + ".xsams"
            if os.path.isfile(errorFilePath): 
                isMailToSend=True
                #zip and move the file in error to a node-specific directory
                #print(errorFilePath + " file exists")
                destinationZipFile = folderName +"/" + error.uuid+ ".zip"
                zip_file(errorFilePath,destinationZipFile,error.uuid)

                # remove the original error xsams
                os.remove(errorFilePath)

                #writing detail for each error into the ad_hoc
                errorResumeFile.write(str(error.uuid)+fileSeparator+str(error.token)+fileSeparator + str(error.timestamp) + fileSeparator +str(error.parameter)+fileSeparator+str(error.errorMessage)+fileSeparator+" https://querystore.vamdc.eu/error/"+node+"/"+error.uuid +".zip"+"\n")

                #updating the database for tagging the notified error
                updateNotifications(database, error)

            else:
                print("*************"+errorFilePath + " file does not exist (base "+ node +")")
        
        errorResumeFile.close()
        
        # sending the error details by mail to the database curator if this file contain at least one line
        if isMailToSend is True:
            sendMail(errorResumeFileName, node, nodeCuratorMail)

        database.connection.commit()

        #removing the file containing the details of the errors (sent by mail)
        os.remove(errorResumeFileName)


def buildNodeMailMap():
    """Building a map containing for each node the email of its curator"""
    nodeMailMap = {}
    addressBook = open("emails.txt","r")
    for line in addressBook:
        nodeName = line.split()[0]
        address = line.split()[1]
        nodeMailMap[nodeName]=address
    return nodeMailMap


def sendMail(path_of_file_to_join, nodeName, mailreceipient):
    """function for sending mail. The first argument is the path of the file to attach to the email. The nodeName and mail receipient are 
    resepctively the name of the node & the email of its curator"""
    fromaddr = "put_your_mail@here"
    toaddr = mailreceipient
    
    # instance of MIMEMultipart 
    msg = MIMEMultipart() 
    # storing the senders email address   
    msg['From'] = fromaddr 
    # storing the receivers email address  
    msg['To'] = toaddr 
    # storing the subject  
    msg['Subject'] = "VAMDC Query Store notification : status of errors for " + nodeName +" on " + str(datetime.datetime.now())
    # string to store the body of the mail 
    body = "Dear "+ nodeName + " curator, \n Please review the errors generated by the Query Store while processing queries from "+nodeName+" in the attached file\n Regards.\n"
    # attach the body with the msg instance 
    msg.attach(MIMEText(body, 'plain')) 
    # open the file to be sent  
    attachment = open(path_of_file_to_join, "rb") 
    # instance of MIMEBase and named as p 
    p = MIMEBase('application', 'octet-stream') 
    # To change the payload into encoded form 
    p.set_payload((attachment).read()) 
    # encode into base64 
    encoders.encode_base64(p) 
    p.add_header('Content-Disposition', "attachment; filename= %s" % path_of_file_to_join) 
    # attach the instance 'p' to instance 'msg' 
    msg.attach(p) 
    # creates SMTP session 
    s = smtplib.SMTP('smtp.yourserver.com', 587) 
    # start TLS for security 
    s.starttls() 
    # Authentication 
    s.login("yourlogin", "yourpassword") 
    # Converts the Multipart msg into a string 
    text = msg.as_string() 
    # sending the mail 
    s.sendmail(fromaddr, toaddr, text) 
    # terminating the session 
    s.quit() 


def main ():
    #open mysql connection
    database = set_connection()

    # get All the errors from the database
    errors = find_files_in_error(database)
    
    #build the map Node-Errors
    nodemap = buildErrorMapping(errors)
    
    #processing the files into the filesystem
    processFilesInError(nodemap,database)

    #closing the connection
    database.cursor.close()
    


if __name__ == '__main__':
    main()
