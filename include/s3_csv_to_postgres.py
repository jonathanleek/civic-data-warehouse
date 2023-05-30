import csv
import psycopg2
import os
import glob
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def download_from_s3(key: str, bucket_name: str, s3_conn_id: str) -> str:
    download_dest = "/tmp/" + filename
    hook = S3Hook(aws_conn_id=s3_conn_id)
    file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=download_dest)
    return file_name

def upload_to_postgres(filename, postgres_conn):
    tablename = filename.replace("/tmp/", "").replace(".csv", "")
    fileInput = open(filename, "r")
    # Extract first line of file
    firstLine = fileInput.readline().strip()

    # Split columns into an array [...]
    columns = firstLine.split(",")

    # Build SQL code to drop table if exists and create table
    sqlQueryCreate = 'DROP TABLE IF EXISTS ' + tablename + ";\n"
    sqlQueryCreate += 'CREATE TABLE' + tablename + "("

    # some loop or function according to your requiremennt
    # Define columns for table
    for column in columns:
        sqlQueryCreate += column + " VARCHAR(64),\n"

    sqlQueryCreate = sqlQueryCreate[:-2]
    sqlQueryCreate += ");"

    conn = postgres_conn
    cur = conn.cursor()
    cur.execute(sqlQueryCreate)
    conn.commit()
    cur.close()

def s3_to_postgres(key, bucket, s3_conn_id, postgres_conn_id):
    download_from_s3(key = key, bucket_name=bucket, s3_conn_id= s3_conn_id)
    upload_to_postgres(filename = "/tmp/" + key, postgres_conn=postgres_conn_id)














#
#
# conn = psycopg2.connect("host= hostnamexx dbname=dbnamexx user= usernamexx password=
# pwdxx
# ")
# print("Connecting to Database")
#
# csvPath = "./TestDataLGA/"
#
# # Loop through each CSV
# for filename in glob.glob(csvPath + "*.csv"):
# # Create a table name
#     tablename = filename.replace("./TestDataLGA\\", "").replace(".csv", "")
# print
# tablename
#
# # Open file
# fileInput = open(filename, "r")
#
# # Extract first line of file
# firstLine = fileInput.readline().strip()
#
# # Split columns into an array [...]
# columns = firstLine.split(",")
#
# # Build SQL code to drop table if exists and create table
# sqlQueryCreate = 'DROP TABLE IF EXISTS ' + tablename + ";\n"
# sqlQueryCreate += 'CREATE TABLE' + tablename + "("
#
# # some loop or function according to your requiremennt
# # Define columns for table
# for column in columns:
#     sqlQueryCreate += column + " VARCHAR(64),\n"
#
# sqlQueryCreate = sqlQueryCreate[:-2]
# sqlQueryCreate += ");"
#
# cur = conn.cursor()
# cur.execute(sqlQueryCreate)
# conn.commit()
# cur.close()