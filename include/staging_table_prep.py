import csv
import logging
import os
import re
import shutil

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger()
staging_download_dest = "/tmp/stage/"
prefix_delimiter = "/"

# Identifiers cannot be passed as bound parameters, so any value interpolated
# into a DDL/COPY statement must match this strict allowlist. The clean_* helpers
# below are total: they always emit a conforming identifier, so this pattern now
# serves as a defense-in-depth assertion (it should never fail) rather than an
# input gate that rejects awkward-but-legitimate source names.
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")

# Postgres silently truncates identifiers to NAMEDATALEN - 1 bytes. Names must
# be cut to this length BEFORE deduplication, or two long names that differ
# only past this point dedupe as "unique" and then collide inside Postgres
# ("column specified more than once"). Cleaned names are ASCII, so chars == bytes.
PG_MAX_IDENTIFIER_LENGTH = 63


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
    tablename = staging_table_name(key)
    logger.info(f"Attempting to import file {filename} into table {tablename}")

    columns = read_header_columns(filename)
    # Defense in depth: clean_column_names always returns conforming names.
    for column in columns:
        if not SAFE_IDENTIFIER.match(column):
            raise ValueError(f"Unsafe column name {column!r} in {key}")
    column_list = ", ".join(columns)

    # Bulk-load via COPY: libpq streams the file so memory stays flat
    # regardless of size, and Postgres handles CSV quoting/escaping. COPY maps
    # CSV fields to the column list POSITIONALLY — HEADER true only discards
    # the first record without comparing names (and HEADER MATCH cannot be
    # used, since cleaned identifiers differ from the raw header fields). The
    # load is correct only because this column list comes from the same
    # read_header_columns parse the create step used on the same file.
    # The unquoted table name folds to lower case to match CREATE TABLE.
    copy_sql = (
        f"COPY cdw.staging.{tablename} ({column_list}) "
        "FROM STDIN WITH (FORMAT CSV, HEADER true)"
    )
    logger.debug(copy_sql)
    bulk_load_csv(copy_sql, filename, postgres_conn)


def execute_query(query, conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    hook.run(sql=query)


def bulk_load_csv(bulkCopySql, filename, conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    hook.copy_expert(sql=bulkCopySql, filename=filename)


def clean_column_name(column_name):
    # Source headers are schema we cannot ask upstream to change, so coerce any
    # name into a legal identifier instead of rejecting it. Lowercase, turn
    # spaces into underscores, then drop everything that is not an ASCII
    # identifier character (this strips punctuation and non-ASCII letters such
    # as accented characters).
    column_name = column_name.lower().replace(" ", "_")
    column_name = re.sub(r"[^a-z0-9_]", "", column_name)

    # A header of only stripped characters leaves nothing behind; give it a base
    # name so the column still loads (callers dedupe collisions).
    if not column_name:
        column_name = "column"

    # Unquoted identifiers cannot start with a digit.
    if column_name[0].isdigit():
        column_name = "_" + column_name

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

    # Truncate to what Postgres will actually store, so callers dedupe the
    # name Postgres sees rather than a longer one it would silently cut.
    return column_name[:PG_MAX_IDENTIFIER_LENGTH]


def clean_column_names(raw_columns):
    """Clean a full list of source headers, resolving collisions deterministically.

    Two distinct headers can clean to the same identifier (e.g. "Area" and
    "area#", or several blank headers all becoming "column"). Duplicates would
    break CREATE TABLE and silently corrupt a COPY column mapping, so the second
    and later occurrences get a numeric suffix. The order of raw_columns drives
    the suffixes, so create and populate must clean the same header sequence to
    agree on names.
    """
    cleaned = []
    seen = set()
    for raw in raw_columns:
        name = clean_column_name(raw)
        candidate = name
        suffix = 1
        while candidate in seen:
            # The suffix must fit within the identifier limit too, or the
            # disambiguation gets truncated away and the collision returns.
            tag = f"_{suffix}"
            candidate = name[: PG_MAX_IDENTIFIER_LENGTH - len(tag)] + tag
            suffix += 1
        seen.add(candidate)
        cleaned.append(candidate)
    return cleaned


def read_header_columns(filename):
    """Read the file's first CSV record and clean it into column names.

    Uses the csv module rather than pandas so the header is the first physical
    record — exactly the line COPY ... HEADER true discards. pandas skips blank
    leading lines, which would desynchronize the two: pandas names the columns
    from line 2 while COPY discards line 1 and loads the real header as data.

    Both the create and populate steps must call this on the same file so
    CREATE TABLE and the COPY column list come from one parse.
    """
    with open(filename, newline="") as f:
        header = next(csv.reader(f), None)
    if not header:
        raise ValueError(f"No header row found in {filename}")
    return clean_column_names(header)


def clean_table_name(key):
    """Coerce an S3 key into a legal staging table identifier.

    File names, like schema, are not something we can ask upstream to change, so
    this mirrors clean_column_name: never reject, always emit a usable name.
    Used by both the create and populate steps so they target the same table.
    """
    # Lowercase first so ".CSV" is stripped too, and removesuffix so the
    # extension is only removed from the end, not from mid-name occurrences.
    name = key.lower().removesuffix(".csv").replace("-", "_")
    name = re.sub(r"[^a-z0-9_]", "", name)
    if not name:
        name = "table"
    if name[0].isdigit():
        name = "_" + name
    return name[:PG_MAX_IDENTIFIER_LENGTH]


def create_table_in_postgres(filename, postgres_conn):
    tablename = clean_table_name(filename.replace(staging_download_dest, ""))

    columns = read_header_columns(filename)
    logger.info("List of columns:")
    for col_index, column in enumerate(columns):
        logger.info(f"column index {col_index} is '{column}'")

    # Defense in depth: clean_table_name always returns a conforming identifier,
    # so a mismatch here means a bug in the cleaner, not bad input.
    if not SAFE_IDENTIFIER.match(tablename):
        raise ValueError(f"Refusing to build DDL for unsafe table name {tablename!r}")

    # The DAG's first task resets staging wholesale (DROP SCHEMA ... CASCADE in
    # include/sql/drop_and_create_staging.sql), so in a normal run this table
    # never pre-exists. The DROP here is retry-safety only: a re-run of this
    # task must not inherit a half-created table from a failed attempt.
    sqlQueryCreate = f"DROP TABLE IF EXISTS CDW.STAGING.{tablename};\n"
    sqlQueryCreate += "CREATE TABLE CDW.STAGING." + tablename + " (\n"

    # Staging is a raw landing zone: use TEXT so source values are never
    # truncated or rejected for exceeding a fixed width.
    sqlQueryCreate += ",\n".join(f"{column} TEXT" for column in columns)
    sqlQueryCreate += ");"
    logger.debug(sqlQueryCreate)

    # Run sqlQueryCreate in Postgres
    execute_query(sqlQueryCreate, postgres_conn)


def staging_table_name(key: str) -> str:
    """Derive the staging table name from an S3 key.

    Mirrors the transformation in create_table_in_postgres so the populate step
    targets the same table the create step built.
    """
    tablename = clean_table_name(key.split(prefix_delimiter)[-1])
    # Defense in depth: clean_table_name always returns a conforming identifier.
    if not SAFE_IDENTIFIER.match(tablename):
        raise ValueError(
            f"Refusing to build COPY for unsafe table name {tablename!r} (from key {key!r})"
        )
    return tablename
