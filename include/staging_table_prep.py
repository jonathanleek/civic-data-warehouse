import os
import logging
from logging import Logger
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log import logging_mixin
import pandas as pd
from io import StringIO
import re


def download_from_s3(key: str, bucket_name: str, s3_conn_id: str):
    logger = logging_mixin.LoggingMixin().logger()
    download_dest = "/tmp/"
    hook = S3Hook(aws_conn_id=s3_conn_id)
    filename = hook.download_file(
        key=key, bucket_name=bucket_name, local_path=download_dest
    )
    logger.info(filename + " downloaded")
    os.rename(src=filename, dst=download_dest + key)
    logger.info(filename + " renamed to " + download_dest + key)


def execute_query(query, conn_id, logger:Logger):
    hook = PostgresHook(postgres_conn_id=conn_id, log_sql=(logger.level==logging.DEBUG))
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
    logger = logging_mixin.LoggingMixin().logger()
    tablename = filename.replace("/tmp/", "").replace(".csv", "").replace("-", "_")
    df = pd.read_csv(filename, dtype=str)

    columns = [clean_column_name(col) for col in df.columns]
    logger.info("List of columns:")
    for col_index, column in enumerate(columns):
        logger.info(f"column index {col_index} is '{column}'")

    # Rebuild the staging table each run so its schema matches the latest source file.
    # This avoids stale column definitions from prior runs, including the old VARCHAR(64) width.
    sqlQueryCreate = ""
    sqlQueryCreate += "DROP TABLE IF EXISTS CDW.STAGING." + tablename + ";\n"
    sqlQueryCreate += "CREATE TABLE IF NOT EXISTS CDW.STAGING." + tablename + " (\n"

    # Staging is the raw landing area, so keep source values lossless.
    # Use TEXT instead of VARCHAR(64) to avoid truncating long descriptions,
    # owner names, legal descriptions, and permit text.
    for column in columns:
        cleaned_column = clean_column_name(column)
        sqlQueryCreate += cleaned_column + " TEXT,\n"

    sqlQueryCreate = sqlQueryCreate[:-2]
    sqlQueryCreate += ");"
    logger.debug(sqlQueryCreate)

    # Run sqlQueryCreate in Postgres
    execute_query(sqlQueryCreate, postgres_conn, logger)


def create_staging_table(bucket, s3_conn_id, postgres_conn_id, key):
    logger = logging_mixin.LoggingMixin().logger()
    download_from_s3(key=key, bucket_name=bucket, s3_conn_id=s3_conn_id)
    logger.info("Downloaded " + key)
    logger.info("Attempting to create table for " + key)
    create_table_in_postgres(filename="/tmp/" + key, postgres_conn=postgres_conn_id)


def populate_staging_table(bucket, s3_conn_id, postgres_conn, key):
    logger = logging_mixin.LoggingMixin().logger()
    # Read everything as text so IDs like ParcelId keep leading zeroes.
    logger.info("Attempting to populate " + key)
    hook = S3Hook(aws_conn_id=s3_conn_id)
    obj = hook.read_key(bucket_name=bucket, key=key)
    df = pd.read_csv(StringIO(obj), dtype=str)
    df = df.where(pd.notna(df), None)

    filename = "/tmp/" + key
    tablename = filename.replace("/tmp/", "").replace(".csv", "").replace("-", "_")
    columns = [clean_column_name(col) for col in df.columns]
    rows = df.itertuples(index=False, name=None)

    postgres_hook = PostgresHook(
        postgres_conn_id=postgres_conn,
        log_sql=(logger.level == logging.DEBUG),
    )
    postgres_hook.insert_rows(
        table=f"staging.{tablename}",
        rows=rows,
        target_fields=columns,
        commit_every=1000,
        executemany=True,
        fast_executemany=True,
    )
