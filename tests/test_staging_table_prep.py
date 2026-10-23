"""Unit tests for the identifier cleaning in include/staging_table_prep.py."""

import pytest

pytest.importorskip("airflow.providers.amazon")
pytest.importorskip("airflow.providers.postgres")

from include.staging_table_prep import (  # noqa: E402
    PG_MAX_IDENTIFIER_LENGTH,
    SAFE_IDENTIFIER,
    assert_safe_identifier,
    clean_column_names,
    clean_postgres_name,
    clean_table_name,
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("OwnerName", "ownername"),
        ("Owner Name", "owner_name"),
        ("Area#", "area"),
        ("2020 Value", "_2020_value"),
        ("Café", "caf"),
        ("", "column_"),  # fallback is itself a reserved word
        ("###", "column_"),
        ("select", "select_"),
        ("x" * 100, "x" * PG_MAX_IDENTIFIER_LENGTH),
    ],
)
def test_clean_postgres_name(raw, expected):
    assert clean_postgres_name(raw) == expected
    assert SAFE_IDENTIFIER.match(expected)


@pytest.mark.parametrize(
    "key, expected",
    [
        ("prcl_Prcl.csv", "prcl_prcl"),
        ("PRCL-BLDGALL.CSV", "prcl_bldgall"),
        ("2024_permits.csv", "_2024_permits"),
        ("my.csv.backup.csv", "mycsvbackup"),
        ("user.csv", "user_"),  # reserved word: previously produced invalid DDL
        (".csv", "table_"),  # previously "table": invalid DDL
    ],
)
def test_clean_table_name(key, expected):
    assert clean_table_name(key) == expected


def test_clean_column_names_dedupes_after_truncation():
    long = "a" * 70
    names = clean_column_names([long, long + "b", "Area", "area#"])
    assert names == [
        "a" * 63,
        "a" * 61 + "_1",
        "area",
        "area_1",
    ]


@pytest.mark.parametrize("bad", ["1abc", "Abc", "a-b", "a b", "", "x" * 64])
def test_assert_safe_identifier_rejects(bad):
    with pytest.raises(ValueError):
        assert_safe_identifier(bad, "test")


def test_assert_safe_identifier_returns_name():
    assert assert_safe_identifier("_ok_1", "test") == "_ok_1"
