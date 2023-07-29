import csv
import psycopg2
import os
import glob
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

def download_from_s3(key: str, bucket_name: str, s3_conn_id: str) -> str:
    download_dest = "/tmp/"
    print(download_dest)
    hook = S3Hook(aws_conn_id=s3_conn_id)
    filename = hook.download_file(key=key, bucket_name=bucket_name, local_path=download_dest)
    print(filename +  " downloaded")
    os.rename(src=filename, dst=download_dest + key)
    print(filename + " renamed to " + download_dest + key)

def execute_query(query, conn_id):
    # with PostgresHook(postgres_conn_id=conn_id).get_conn() as conn:
    #     with conn.cursor() as cur:
    #         cur.execute(query)
    hook = PostgresHook(postgres_conn_id=conn_id)
    hook.run(sql=query)
def upload_to_postgres(filename, postgres_conn):
    #TODO Add handling of empty files
    tablename = filename.replace("/tmp/", "").replace(".csv", "").replace("-", "_")
    fileInput = open(filename, "r")
    # Extract first line of file
    firstLine = fileInput.readline().strip()

    # Split columns into an array [...]
    columns = firstLine.split(",")

    # Build SQL code to drop table if exists and create table
    # sqlQueryCreate = 'DROP TABLE IF EXISTS CDW.STAGING.' + tablename + ";\n"
    sqlQueryCreate = ""
    sqlQueryCreate += 'CREATE TABLE CDW.STAGING.' + tablename + "("

    # some loop or function according to your requiremennt
    # Define columns for table
    for column in columns:
        if column == "Desc":
            column = "Description"
        sqlQueryCreate += column + " VARCHAR(64),\n"

    sqlQueryCreate = sqlQueryCreate[:-2]
    sqlQueryCreate += ");"
    print(sqlQueryCreate)

    # run sqlQueryCreate in Postgres
     execute_query(sqlQueryCreate, postgres_conn)

def s3_to_postgres(bucket, s3_conn_id, postgres_conn_id, key):
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