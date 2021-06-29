#!/bin/bash

# datestamp YYYYMMDD

DATE=$(date +"%Y%m%d")

# Directory where dumps are stored

BACKUP_DIR="/path/to/dumps/"

# Identifiants MySQL

MYSQL_USER="PUT_DB_USER_HERE"
MYSQL_PASSWORD="PUT_DB_PASSWORD_HERE"

# MySQL system command

MYSQL=/usr/bin/mysql
MYSQLDUMP=/usr/bin/mysqldump

# Number of days the dumps are kept

RETENTION=7

# Create a new directory into backup directory location for this date

mkdir -p $BACKUP_DIR/$DATE

# Dumb the databases in seperate names and gzip the .sql file

db="query_store"

$MYSQLDUMP --force --opt --user=$MYSQL_USER -p$MYSQL_PASSWORD --skip-lock-tables --events --databases $db | gzip > "$BACKUP_DIR/$DATE/$db.sql.gz"


# Remove files older than X days

find $BACKUP_DIR/* -mtime +$RETENTION -delete
