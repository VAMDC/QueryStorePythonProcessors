“””” This script is authored jointly by 
the Paris Observatory VAMDC team (C.M. Zwölf and N. Moreau) and 
the Fireblock.io team
for integrating into the VAMDC Query Store the certification 
facilities provided by Fireblock.io
This collaboration was performed on November 2018
“”” 

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import MySQLdb
import subprocess
import time
import os
import json
import sys
import logging
import zipfile
import shutil

XSAMS_SOURCE_FOLDER = "/xsamsfile/source/folder"
XSAMS_DEST_FOLDER = "/xsamsfile/processed/folder"
FILE_EXTENSION=".xsams"
KEY_FILE_PATH = "/absolute/path/to/your/private/key/file"
LOG_PATH="/absolute/path/to/log/file"

basename = "databaseName"
user = "databaseUser"
password = "databasePassword"
host = "127.0.0.1"
port = 3306    
    
databaseConnection = None 
cursor = None 

def set_connection():
    """open a mysql connection (MariaDB)"""
    global databaseConnection, cursor, basename
    databaseConnection = MySQLdb.connect(host, user, password, basename )
    cursor = MySQLdb.cursors.DictCursor(databaseConnection)     
    #cursor.execute("SET UNIQUE_CHECKS=0")
    cursor.execute("SET FOREIGN_KEY_CHECKS=0")
    cursor.execute("SET NAMES UTF8")
    cursor.execute("SET AUTOCOMMIT=0")
    cursor.execute("SET sql_mode='ANSI'")    # enable ansi mode for portability
    return cursor


def signature(cmd, f):
    """sign with cmd commad the f file"""
    logger = logging.getLogger()
    try :
        cmdTxt = ' '.join(cmd)
        logger.info('sign: %s' % cmdTxt)
        output = subprocess.check_output(cmd)
        out = output.decode()
        # out = out.replace('\n', '').replace('\r', '')
        jout = json.loads(out)
        logger.info('%s signed', f)
        return jout['hash']
    except subprocess.CalledProcessError as e:
        if e.returncode == 60:
            # already signed
            out = e.output.decode()
            # out = out.replace('\n', '').replace('\r', '')
            jout = json.loads(out)
            logger.info('%s already signed', f)
            return jout['hash']
        else:
            logger.error(e.output.decode())
            return None
            
def find_files_to_process(cursor):
    """list files to sign (and create a zip)"""
    sql = "SELECT UUID FROM Queries WHERE dataURL is  NOT NULL AND biblioGraphicReferences is NOT NULL"
    cursor.execute(sql)
    result = []
    for f in cursor.fetchall():
        filepath = XSAMS_SOURCE_FOLDER + "/" + f['UUID'] + FILE_EXTENSION
        # check if regular file exist
        if os.path.isfile(filepath): 
            result.append(f["UUID"])
    logger = logging.getLogger()
    logger.info('files to process: %s' % str(result))
    return result


def insert_signature_in_db(cursor, signature, uuid):
    """when the files are signed, add the hash of the xsams file"""
    sql = 'Update Queries set signature =\'%s\' WHERE UUID like \'%s\' ' % (signature, uuid)
    print(sql)
    cursor.execute(sql)



def zip_file(tmp_folder, f):
    """create a zip archive of the xsams file"""
    src = tmp_folder + '/' + f + FILE_EXTENSION
    dst = XSAMS_DEST_FOLDER + '/' + f + '.zip'
    zip = zipfile.ZipFile(dst, 'w', zipfile.ZIP_DEFLATED)
    zip.write(src, f + FILE_EXTENSION)
    zip.close()
    logger = logging.getLogger()
    logger.info('create zip archive %s' % f + '.zip')
    
    
    
def certify_file(tmp_folder, f, cursor):
    """certify xsams and zip files"""  
    cmd_sign_xsams = [ 'fio', 'sign', '-j', '-f', KEY_FILE_PATH, tmp_folder + "/" + f + FILE_EXTENSION ]
    cmd_sign_zip = [ 'fio', 'sign', '-j', '-f', KEY_FILE_PATH, XSAMS_DEST_FOLDER + "/" + f + '.zip' ]
    # sign xsams
    hash = signature(cmd_sign_xsams, f + FILE_EXTENSION)
    hash2 = signature(cmd_sign_zip, f + '.zip')
    if (hash and hash2):
        insert_signature_in_db(cursor, hash, f)
        
        
 def main ():
    """main function"""
    # define logger

    fh = logging.FileHandler(LOG_PATH+'/ZipAndSign.log')
    fh.setLevel(logging.INFO)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    #stream_handler = logging.StreamHandler()
    #stream_handler.setLevel(logging.INFO)
    #logger.addHandler(stream_handler)
    logger.addHandler(fh) 
    


    executionTimestamp = str(int(round(time.time()*1000)))
    logger.info('Execution at '+executionTimestamp)

    # open mysql connection
    cursor = set_connection()

    # list absolute filepaths
    files = find_files_to_process(cursor)
    #files = files[-10:]

    # create tmp_folder
    tmp_folder = XSAMS_SOURCE_FOLDER + "/" + executionTimestamp
    os.makedirs(tmp_folder)

    # move
    for f in files:
        fp = XSAMS_SOURCE_FOLDER + "/" + f + FILE_EXTENSION
        shutil.move(fp, tmp_folder)

    # zip
    for f in files:
        zip_file(tmp_folder, f)

    # sign
    for f in files:
        certify_file(tmp_folder, f, cursor)

    # close mysql connection
    cursor.close()

    # delete temp folder
    shutil.rmtree(tmp_folder)

if __name__ == '__main__':
    main()

