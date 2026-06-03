import logging
import os
import re
from logging import Logger

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log import logging_mixin

# Identifiers cannot be passed as bound parameters, so any value interpolated
# into a DDL/COPY statement must be validated against a strict allowlist first.
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")


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


def execute_query(query, conn_id, logger: Logger):
    hook = PostgresHook(
        postgres_conn_id=conn_id, log_sql=(logger.level == logging.DEBUG)
    )
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

    if not SAFE_IDENTIFIER.match(tablename):
        raise ValueError(f"Refusing to build DDL for unsafe table name {tablename!r}")

    # Drop and recreate so the schema always matches the current CSV. The schema
    # reset only truncates rows, so a CREATE TABLE IF NOT EXISTS would keep a
    # stale schema from a previous run.
    sqlQueryCreate = f"DROP TABLE IF EXISTS CDW.STAGING.{tablename} CASCADE;\n"
    sqlQueryCreate += "CREATE TABLE CDW.STAGING." + tablename + " (\n"

    # Staging is a raw landing zone: use TEXT so source values are never
    # truncated or rejected for exceeding a fixed width.
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


def staging_table_name(key: str) -> str:
    """Derive the staging table name from an S3 key.

    Mirrors the transformation in create_table_in_postgres so the populate step
    targets the same table the create step built.
    """
    tablename = key.replace(".csv", "").replace("-", "_")
    if not SAFE_IDENTIFIER.match(tablename):
        raise ValueError(
            f"Refusing to build COPY for unsafe table name {tablename!r} (from key {key!r})"
        )
    return tablename


def populate_staging_table(bucket, s3_conn_id, postgres_conn, key):
    logger = logging_mixin.LoggingMixin().logger()
    logger.info("Attempting to populate " + key)

    tablename = staging_table_name(key)

    # Stream the object to local disk instead of loading it into memory; source
    # files can be hundreds of MB and the previous row-by-row INSERT approach
    # built the entire dataset as SQL strings in RAM, which triggered OOM kills.
    hook = S3Hook(aws_conn_id=s3_conn_id)
    local_path = hook.download_file(key=key, bucket_name=bucket, local_path="/tmp/")

    try:
        # Derive column names exactly as create_table_in_postgres does, via
        # pandas, so they match the table even for quirks like blank headers
        # (pandas labels them "Unnamed: N"). Mapping by name rather than position
        # means an upstream column reorder still loads correctly, and an unknown
        # column fails loudly instead of silently shifting every value over.
        header = pd.read_csv(local_path, nrows=0, dtype=str).columns
        columns = [clean_column_name(col) for col in header]
        for column in columns:
            if not SAFE_IDENTIFIER.match(column):
                raise ValueError(f"Unsafe column name {column!r} in {key}")
        column_list = ", ".join(columns)

        # Bulk-load via COPY: libpq streams the file so memory stays flat
        # regardless of size, and Postgres handles CSV quoting/escaping. HEADER
        # true skips the first line; the explicit column list controls mapping.
        # The unquoted table name folds to lower case to match CREATE TABLE.
        copy_sql = (
            f"COPY cdw.staging.{tablename} ({column_list}) "
            "FROM STDIN WITH (FORMAT CSV, HEADER true)"
        )
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn)
        pg_hook.copy_expert(sql=copy_sql, filename=local_path)
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

    logger.info("Populated cdw.staging." + tablename)
