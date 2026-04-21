import os
import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log import logging_mixin
import pandas as pd
import re

logger = logging.getLogger()
staging_download_dest = "/tmp/stage/"
prefix_delimiter = "/"


def get_latest_s3_prefix(bucket: str, s3_conn_id: str):
    
    prefix_list:list[str] = \
        S3Hook(aws_conn_id=s3_conn_id).list_prefixes(bucket_name=bucket, delimiter=prefix_delimiter)

    if prefix_list is None or prefix_list.count == 0:
        logger.warning("No prefixes found. Assuming files are in root of bucket.")
        return ""

    prefix_list.sort(reverse=True)

    logger.info("Using prefix %s", prefix_list[0])

    return prefix_list[0]


def ensure_staging_directory():
    
    logger.info(f"Preparing staging directory '{staging_download_dest}'")
    
    if os.path.exists(staging_download_dest):
        logger.info(f"clearing all files in {staging_download_dest}")
        for root, dirs, files in os.walk(staging_download_dest):
            for file in files:
                file_to_remove = os.path.join(root, file)
                logger.debug(f"removing file {file_to_remove}")
                os.remove(file_to_remove)
        for root, dirs, files in os.walk(staging_download_dest):
            for dir in dirs:
                subdir_to_remove = os.path.join(root, dir)
                logger.debug(f"removing subdirectory {subdir_to_remove}")
                os.rmdir(subdir_to_remove)
        logger.info("tmp directory cleared")
        
    if not os.path.exists(staging_download_dest):
        logger.info(f"Creating directory {staging_download_dest}")
        os.makedirs(staging_download_dest)


def download_from_s3(bucket_name: str, s3_conn_id: str, key: str):

    logger.info(f"Using tmp directory '{staging_download_dest}'")
    filename_only = key.split(prefix_delimiter)[-1]
    dest_file = os.path.join(staging_download_dest, filename_only)
    logger.info(f"Downloading file '{dest_file}'")
    
    # KRT note 04/06/26 :
    # Using S3Hook.download_file() seems to be async for me, but I'm 
    #  unable to see what and how. All I can see is that I get occasional
    #  zero-byte files. So, right now, I'm using the underlying
    #  connection's download_file() call, which should be synchronous and
    #  therefore fully block until the file download completes.
    S3Hook(aws_conn_id=s3_conn_id) \
        .get_conn() \
        .download_file(bucket_name, key, dest_file)

    # If we get another download error, throw here to block all further
    #  processing.
    if os.path.getsize(dest_file) == 0:
        raise Exception(f"Downloaded file {dest_file} is zero bytes")

    logger.info(f"{dest_file} downloaded")
    

def create_staging_table(postgres_conn_id, key):
    
    filename = staging_download_dest + key.split(prefix_delimiter)[-1]
    logger.info("Attempting to create table for " + filename)
    create_table_in_postgres(filename=filename, postgres_conn=postgres_conn_id)


def populate_staging_table(postgres_conn, key):
    
    filename = staging_download_dest + key.split(prefix_delimiter)[-1]
    tablename = filename.replace(staging_download_dest, "").replace(".csv", "").replace("-", "_")
    logger.info(f"Attempting to import file {filename} into table {tablename}")

    df = pd.read_csv(filename, dtype=str)

    sqlBulkCopy = BULK_COPY_STATEMENT_FROM_DATAFRAME(df, tablename)
    logger.debug(sqlBulkCopy)
    bulk_load_csv(sqlBulkCopy, filename, postgres_conn)


def execute_query(query, conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id, log_sql=(logger.level==logging.DEBUG))
    hook.run(sql=query)


def bulk_load_csv(bulkCopySql, filename, conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id, log_sql=(logger.level==logging.DEBUG))
    hook.copy_expert(sql=bulkCopySql, filename=filename)


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
    
    tablename = filename.replace(staging_download_dest, "").replace(".csv", "").replace("-", "_")

    df = pd.read_csv(
            filename,
            dtype=str
        )

    columns = [clean_column_name(col) for col in df.columns]
    logger.info("List of columns:")
    for col_index, column in enumerate(columns):
        logger.info(f"column index {col_index} is '{column}'")

    # Build SQL code to drop table if exists and create table
    sqlQueryCreate = "CREATE TABLE IF NOT EXISTS CDW.STAGING." + tablename + " ("
    columns = [col + " VARCHAR" for col in columns]
    sqlQueryCreate += ", ".join(columns)
    sqlQueryCreate += ");"
    logger.debug(sqlQueryCreate)

    # Run sqlQueryCreate in Postgres
    execute_query(sqlQueryCreate, postgres_conn)



def BULK_COPY_STATEMENT_FROM_DATAFRAME(SOURCE, TARGET):
    cleaned_columns = [clean_column_name(col) for col in SOURCE.columns]
    return "COPY CDW.STAGING." + TARGET + " (" + ", ".join(cleaned_columns) + ") FROM STDIN WITH CSV HEADER"

