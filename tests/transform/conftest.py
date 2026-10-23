"""Harness for the staging -> current transform tests.

These tests execute the real SQL files (include/transform_layers.py) against a
scratch Postgres. They are skipped unless CDW_TEST_DATABASE_URL points at a
database named `cdw` (the DDL uses cdw.<schema>.<table> names), e.g.

    CDW_TEST_DATABASE_URL=postgresql://postgres@localhost:5432/cdw pytest tests/transform

The database is wiped (staging, crosswalk, etl, current, history) by the tests.
"""

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from include.transform_layers import SQL_DIR, layer_files  # noqa: E402

from .fixtures import STAGING_TABLES  # noqa: E402

DSN = os.environ.get("CDW_TEST_DATABASE_URL")

# The cdw_creation DAG's setup scripts, in its task order.
SETUP_FILES = [
    "drop_and_create_staging.sql",
    "create_crosswalk.sql",
    "create_etl.sql",
    "create_current.sql",
    "create_history.sql",
]


@pytest.fixture(scope="session")
def pg():
    if not DSN:
        pytest.skip("CDW_TEST_DATABASE_URL not set")
    psycopg2 = pytest.importorskip("psycopg2")
    conn = psycopg2.connect(DSN)
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        if cur.fetchone()[0] != "cdw":
            pytest.skip("CDW_TEST_DATABASE_URL must name a database called cdw")
    yield conn
    conn.close()


def run_sql(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def run_file(conn, rel_path):
    run_sql(conn, (SQL_DIR / rel_path).read_text())


def query(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    conn.commit()
    return rows


def scalar(conn, sql, params=None):
    return query(conn, sql, params)[0][0]


def load_staging(conn, tables=STAGING_TABLES):
    """Create all-TEXT staging tables (as staging_table_prep does) and fill them."""
    from psycopg2 import sql

    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS staging CASCADE; CREATE SCHEMA staging;")
        for table, rows in tables.items():
            columns = sorted({c for row in rows for c in row})
            cur.execute(
                sql.SQL("CREATE TABLE staging.{} ({})").format(
                    sql.Identifier(table),
                    sql.SQL(", ").join(
                        sql.SQL("{} text").format(sql.Identifier(c)) for c in columns
                    ),
                )
            )
            for row in rows:
                cur.execute(
                    sql.SQL("INSERT INTO staging.{} ({}) VALUES ({})").format(
                        sql.Identifier(table),
                        sql.SQL(", ").join(map(sql.Identifier, columns)),
                        sql.SQL(", ").join(sql.Placeholder() * len(columns)),
                    ),
                    [row.get(c) for c in columns],
                )
    conn.commit()


def reset_database(conn):
    run_sql(conn, "DROP SCHEMA IF EXISTS crosswalk, etl, current, history CASCADE")
    for f in SETUP_FILES:
        run_file(conn, f)
    run_sql(
        conn,
        "SELECT crosswalk.set_active_namespace('840', '29510', 'St. Louis City')",
    )


def run_layers(conn, layers):
    for layer in layers:
        for f in layer_files(layer):
            run_file(conn, f)
