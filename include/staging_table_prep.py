import logging
import os
import re
import shutil

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log import logging_mixin

logger = logging.getLogger(__name__)
staging_download_dest = "/tmp/stage/"
prefix_delimiter = "/"


def get_latest_s3_prefix(bucket: str, s3_conn_id: str):
    """
    This will grab the latest 'prefix', or S3 directory, from the S3 connection and bucket name passed in.

    We assume that the prefixes are time strings which can be sorted. Thus we take the latest one.

    There are no checks to see if the prefix is valid! This can cause many issues, especially if the state of
     the directory is invalid.

    Args:
        bucket: S3 bucket name
        s3_conn_id: S3 connection string

    Returns:
        Name of the latest prefix in the S3 bucket
    """

    prefix_list: list[str] = S3Hook(aws_conn_id=s3_conn_id).list_prefixes(
        bucket_name=bucket, delimiter=prefix_delimiter
    )

    if not prefix_list:
        logger.warning("No prefixes found. Assuming files are in root of bucket.")
        return ""

    prefix_list.sort(reverse=True)

    logger.info("Using prefix %s", prefix_list[0])

    return prefix_list[0]


def ensure_empty_staging_directory():
    logger = logging_mixin.LoggingMixin().logger()

    logger.info(f"Preparing staging directory '{staging_download_dest}'")

    # If the path exists, we want to remove it, to ensure that it is empty. This is best done by
    #  fully removing the path.
    # If the path removal call fails, we will log a warning on path creation.
    if os.path.exists(staging_download_dest):
        logger.info(
            f"Staging directory {staging_download_dest} exists - clearing and deleting"
        )
        shutil.rmtree(staging_download_dest, ignore_errors=True)
        logger.info("Staging directory removed")

    # If the path does not exist, as it shouldn't, create the directory.
    #  If the path exists (likely because the removal process failed), add a warning at this time.
    if not os.path.exists(staging_download_dest):
        logger.info(f"Creating staging directory {staging_download_dest}")
        os.makedirs(staging_download_dest)
    else:
        logger.warning(
            f"Staging directory {staging_download_dest} exists at creation time. Directory likely contains old files."
        )


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
    S3Hook(aws_conn_id=s3_conn_id).get_conn().download_file(bucket_name, key, dest_file)

    logger.info(f"{dest_file} downloaded")


def create_staging_table(postgres_conn_id, key):
    filename = staging_download_dest + key.split(prefix_delimiter)[-1]
    logger.info("Attempting to create table for " + filename)
    create_table_in_postgres(filename=filename, postgres_conn=postgres_conn_id)


def populate_staging_table(postgres_conn, key):

    filename = staging_download_dest + key.split(prefix_delimiter)[-1]
    tablename = (
        filename.replace(staging_download_dest, "")
        .replace(".csv", "")
        .replace("-", "_")
    )
    logger.info(f"Attempting to import file {filename} into table {tablename}")

    df = pd.read_csv(filename, dtype=str)

    sqlBulkCopy = BULK_COPY_STATEMENT_FROM_DATAFRAME(df, tablename)
    logger.debug(sqlBulkCopy)
    bulk_load_csv(sqlBulkCopy, filename, postgres_conn)


def execute_query(query, conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    hook.run(sql=query)


def bulk_load_csv(bulkCopySql, filename, conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
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

    tablename = (
        filename.replace(staging_download_dest, "")
        .replace(".csv", "")
        .replace("-", "_")
    )

    df = pd.read_csv(filename, dtype=str)

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
    return (
        "COPY CDW.STAGING."
        + TARGET
        + " ("
        + ", ".join(cleaned_columns)
        + ") FROM STDIN WITH CSV HEADER"
    )
