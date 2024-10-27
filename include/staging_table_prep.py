import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
from io import StringIO
import re


def download_from_s3(key: str, bucket_name: str, s3_conn_id: str) -> str:
    download_dest = "/tmp/"
    print(download_dest)
    hook = S3Hook(aws_conn_id=s3_conn_id)
    filename = hook.download_file(
        key=key, bucket_name=bucket_name, local_path=download_dest
    )
    print(filename + " downloaded")
    os.rename(src=filename, dst=download_dest + key)
    print(filename + " renamed to " + download_dest + key)


def execute_query(query, conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    hook.run(sql=query)


def clean_column_name(column_name):
    # Convert to lowercase, replace spaces with underscores, and remove special characters
    column_name = column_name.lower().replace(" ", "_")
    column_name = re.sub(r"\W", "", column_name)  # Remove non-alphanumeric characters

    # Check for reserved keywords and rename if necessary
    reserved_keywords = {
        "all",
        "analyse",
        "analyze",
        "and",
        "any",
        "array",
        "as",
        "asc",
        "asymmetric",
        "authorization",
        "binary",
        "both",
        "case",
        "cast",
        "check",
        "collate",
        "collation",
        "column",
        "concurrently",
        "constraint",
        "create",
        "cross",
        "current_catalog",
        "current_date",
        "current_role",
        "current_time",
        "current_timestamp",
        "current_user",
        "default",
        "deferrable",
        "desc",
        "distinct",
        "do",
        "else",
        "end",
        "except",
        "false",
        "fetch",
        "for",
        "foreign",
        "freeze",
        "from",
        "full",
        "grant",
        "group",
        "having",
        "ilike",
        "in",
        "initially",
        "inner",
        "intersect",
        "into",
        "is",
        "isnull",
        "join",
        "lateral",
        "leading",
        "left",
        "like",
        "limit",
        "localtime",
        "localtimestamp",
        "natural",
        "not",
        "notnull",
        "null",
        "offset",
        "on",
        "only",
        "or",
        "order",
        "outer",
        "overlaps",
        "placing",
        "primary",
        "references",
        "returning",
        "right",
        "select",
        "session_user",
        "similar",
        "some",
        "symmetric",
        "table",
        "then",
        "to",
        "trailing",
        "true",
        "union",
        "unique",
        "user",
        "using",
        "variadic",
        "verbose",
        "when",
        "where",
        "window",
        "with",
    }

    if column_name in reserved_keywords:
        column_name += "_"

    return column_name


def create_table_in_postgres(filename, postgres_conn):
    tablename = filename.replace("/tmp/", "").replace(".csv", "").replace("-", "_")
    with open(filename, "r") as fileInput:
        # Extract first line of file
        firstLine = fileInput.readline().strip()

    # Split columns into an array
    columns = firstLine.split(",")
    print("List of columns:")
    for column in columns:
        print(column)

    # Build SQL code to drop table if exists and create table
    sqlQueryCreate = ""
    sqlQueryCreate += "CREATE TABLE IF NOT EXISTS CDW.STAGING." + tablename + " (\n"

    # Define columns for table
    for column in columns:
        cleaned_column = clean_column_name(column)
        sqlQueryCreate += cleaned_column + " VARCHAR(64),\n"

    sqlQueryCreate = sqlQueryCreate[:-2]
    sqlQueryCreate += ");"
    print(sqlQueryCreate)

    # Run sqlQueryCreate in Postgres
    execute_query(sqlQueryCreate, postgres_conn)


def create_staging_table(bucket, s3_conn_id, postgres_conn_id, key):
    download_from_s3(key=key, bucket_name=bucket, s3_conn_id=s3_conn_id)
    print("Downloaded " + key)
    print("Attempting to create table for " + key)
    create_table_in_postgres(filename="/tmp/" + key, postgres_conn=postgres_conn_id)


def SQL_INSERT_STATEMENT_FROM_DATAFRAME(SOURCE, TARGET):
    # Generate the SQL insert statement from dataframe
    sql_texts = []
    for index, row in SOURCE.iterrows():
        cleaned_columns = [clean_column_name(col) for col in SOURCE.columns]
        sql_texts.append(
            "INSERT INTO CDW.STAGING."
            + TARGET
            + " ("
            + ", ".join(cleaned_columns)
            + ") VALUES "
            + str(tuple(row.values))
        )
    return sql_texts


def populate_staging_table(bucket, s3_conn_id, postgres_conn, key):
    # Import table from S3 bucket to a pandas dataframe and convert to an array
    print("Attempting to populate " + key)
    hook = S3Hook(aws_conn_id=s3_conn_id)
    obj = hook.read_key(bucket_name=bucket, key=key)
    df = pd.read_csv(StringIO(obj))
    for column in df.columns:
        if df[column].dtype == object:
            df[column] = df[column].replace("'", "''", inplace=True)
    df.replace(np.nan, "None", inplace=True)
    records = df.to_records(index=True)

    # Read table from S3 bucket
    filename = "/tmp/" + key
    tablename = filename.replace("/tmp/", "").replace(".csv", "").replace("-", "_")
    columns = list(df.columns)

    # Build SQL code to insert data into table
    sqlQueryInsert = SQL_INSERT_STATEMENT_FROM_DATAFRAME(df, tablename)
    print(sqlQueryInsert)
    # run sqlQueryCreate in Postgres
    execute_query(sqlQueryInsert, postgres_conn)
