import logging
import re
from typing import Any

import usaddress

logger = logging.getLogger(__name__)

SOURCE_SCHEMA = "staging"
SOURCE_TABLE = "prcl_prcl"
TARGET_SCHEMA = "current"
TARGET_TABLE = "int_prcl_owners"

SOURCE_COLUMNS = [
    "ownername",
    "ownername2",
    "owneraddr",
    "ownercity",
    "ownerstate",
    "ownercountry",
    "ownerzip",
]

TARGET_COLUMNS = [
    "ownername",
    "ownername2",
    "owneraddr_raw",
    "owneraddr_cleaned",
    "house_number",
    "predir",
    "street_name",
    "street_suffix",
    "postdir",
    "unit_type",
    "unit_number",
    "po_box",
    "embedded_zip_from_owneraddr",
    "ownerzip_canonical",
    "canonical_address_key",
    "parse_status",
    "parse_error",
]

BATCH_SIZE = 1000

ZIP_RE = re.compile(r"\b(\d{5})(?:[-\s]?(\d{4}))?\b")
SPACE_RE = re.compile(r"\s+")

DIRECTION_ABBREVIATIONS = {
    "NORTH": "N",
    "SOUTH": "S",
    "EAST": "E",
    "WEST": "W",
    "NORTHEAST": "NE",
    "NORTHWEST": "NW",
    "SOUTHEAST": "SE",
    "SOUTHWEST": "SW",
}

LABEL_MAP = {
    "AddressNumber": "house_number",
    "StreetNamePreDirectional": "predir",
    "StreetName": "street_name",
    "StreetNamePostType": "street_suffix",
    "StreetNamePostDirectional": "postdir",
    "OccupancyType": "unit_type",
    "OccupancyIdentifier": "unit_number",
}

PO_BOX_LABELS = [
    "USPSBoxGroupType",
    "USPSBoxGroupID",
    "USPSBoxType",
    "USPSBoxID",
]


def rebuild_int_prcl_owners(postgres_conn_id: str = "cdw-dev") -> int:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, log_sql=False)
    conn = hook.get_conn()
    inserted_rows = 0

    try:
        with conn.cursor() as cursor:
            _ensure_source_columns(cursor)
            _recreate_target_table(cursor)

        with conn.cursor() as read_cursor, conn.cursor() as write_cursor:
            read_cursor.execute(_source_sql())
            while True:
                rows = read_cursor.fetchmany(BATCH_SIZE)
                if not rows:
                    break

                parsed_rows = [_parse_owner_row(row) for row in rows]
                _insert_rows(write_cursor, parsed_rows)
                inserted_rows += len(parsed_rows)

        with conn.cursor() as cursor:
            _create_indexes(cursor)

        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Failed to rebuild %s.%s", TARGET_SCHEMA, TARGET_TABLE)
        raise
    finally:
        conn.close()

    logger.info("Rebuilt %s.%s with %s rows", TARGET_SCHEMA, TARGET_TABLE, inserted_rows)
    return inserted_rows


def parse_owner_record(
    ownername: Any,
    ownername2: Any,
    owneraddr: Any,
    ownercity: Any,
    ownerstate: Any,
    ownercountry: Any,
    ownerzip: Any,
) -> dict[str, str | None]:
    ownername_clean = _clean_text(ownername)
    ownername2_clean = _clean_text(ownername2)
    owneraddr_raw = _clean_text(owneraddr)
    ownercity_clean = _clean_text(ownercity)
    ownerstate_clean = _clean_text(ownerstate)
    ownercountry_clean = _clean_text(ownercountry)
    ownerzip_canonical = canonicalize_zip(ownerzip)

    if owneraddr_raw is None:
        return {
            "ownername": ownername_clean,
            "ownername2": ownername2_clean,
            "owneraddr_raw": None,
            "owneraddr_cleaned": None,
            "house_number": None,
            "predir": None,
            "street_name": None,
            "street_suffix": None,
            "postdir": None,
            "unit_type": None,
            "unit_number": None,
            "po_box": None,
            "embedded_zip_from_owneraddr": None,
            "ownerzip_canonical": ownerzip_canonical,
            "canonical_address_key": _build_canonical_address_key(
                {},
                ownercity_clean,
                ownerstate_clean,
                ownercountry_clean,
                ownerzip_canonical,
                None,
            ),
            "parse_status": "missing",
            "parse_error": None,
        }

    tagged_address, parse_status, parse_error = _tag_owner_address(owneraddr_raw)
    components = _extract_components(tagged_address)
    embedded_zip = canonicalize_zip(tagged_address.get("ZipCode")) or _extract_zip(owneraddr_raw)
    components["po_box"] = _build_po_box(tagged_address)

    owneraddr_cleaned = _build_cleaned_owneraddr(components, owneraddr_raw)
    canonical_address_key = _build_canonical_address_key(
        components,
        ownercity_clean,
        ownerstate_clean,
        ownercountry_clean,
        ownerzip_canonical,
        embedded_zip,
    )

    return {
        "ownername": ownername_clean,
        "ownername2": ownername2_clean,
        "owneraddr_raw": owneraddr_raw,
        "owneraddr_cleaned": owneraddr_cleaned,
        "house_number": components.get("house_number"),
        "predir": components.get("predir"),
        "street_name": components.get("street_name"),
        "street_suffix": components.get("street_suffix"),
        "postdir": components.get("postdir"),
        "unit_type": components.get("unit_type"),
        "unit_number": components.get("unit_number"),
        "po_box": components.get("po_box"),
        "embedded_zip_from_owneraddr": embedded_zip,
        "ownerzip_canonical": ownerzip_canonical,
        "canonical_address_key": canonical_address_key,
        "parse_status": parse_status,
        "parse_error": parse_error,
    }


def canonicalize_zip(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None

    match = ZIP_RE.search(text)
    if not match:
        return _normalize_component(text)

    if match.group(2):
        return f"{match.group(1)}-{match.group(2)}"

    return match.group(1)


def _ensure_source_columns(cursor) -> None:
    cursor.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = %s
          and table_name = %s
        """,
        (SOURCE_SCHEMA, SOURCE_TABLE),
    )
    available_columns = {row[0] for row in cursor.fetchall()}
    missing_columns = sorted(set(SOURCE_COLUMNS) - available_columns)

    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"{SOURCE_SCHEMA}.{SOURCE_TABLE} is missing required columns: {missing}")


def _recreate_target_table(cursor) -> None:
    cursor.execute(
        f"""
        drop table if exists {TARGET_SCHEMA}.{TARGET_TABLE};

        create table {TARGET_SCHEMA}.{TARGET_TABLE} (
            ownername text,
            ownername2 text,
            owneraddr_raw text,
            owneraddr_cleaned text,
            house_number text,
            predir text,
            street_name text,
            street_suffix text,
            postdir text,
            unit_type text,
            unit_number text,
            po_box text,
            embedded_zip_from_owneraddr text,
            ownerzip_canonical text,
            canonical_address_key text,
            parse_status text,
            parse_error text
        );
        """
    )


def _source_sql() -> str:
    selected_columns = ", ".join(SOURCE_COLUMNS)
    return f"""
        select distinct {selected_columns}
        from {SOURCE_SCHEMA}.{SOURCE_TABLE}
        where coalesce(
            nullif(trim(ownername), ''),
            nullif(trim(ownername2), ''),
            nullif(trim(owneraddr), ''),
            nullif(trim(ownercity), ''),
            nullif(trim(ownerstate), ''),
            nullif(trim(ownercountry), ''),
            nullif(trim(ownerzip), '')
        ) is not null
        order by {selected_columns}
    """


def _insert_rows(cursor, rows: list[dict[str, str | None]]) -> None:
    values_template = ", ".join(["%s"] * len(TARGET_COLUMNS))
    insert_sql = f"""
        insert into {TARGET_SCHEMA}.{TARGET_TABLE} ({", ".join(TARGET_COLUMNS)})
        values ({values_template})
    """
    cursor.executemany(
        insert_sql,
        [tuple(row[column] for column in TARGET_COLUMNS) for row in rows],
    )


def _create_indexes(cursor) -> None:
    cursor.execute(
        f"""
        create index int_prcl_owners_canonical_address_key_idx
            on {TARGET_SCHEMA}.{TARGET_TABLE} (canonical_address_key);

        create index int_prcl_owners_parse_status_idx
            on {TARGET_SCHEMA}.{TARGET_TABLE} (parse_status);
        """
    )


def _parse_owner_row(row: tuple[Any, ...]) -> dict[str, str | None]:
    return parse_owner_record(*row)


def _tag_owner_address(owneraddr_raw: str) -> tuple[dict[str, str], str, str | None]:
    try:
        tagged_address, _address_type = usaddress.tag(owneraddr_raw)
        return dict(tagged_address), "parsed", None
    except usaddress.RepeatedLabelError as exc:
        tagged_address = _tag_repeated_label_address(getattr(exc, "parsed_string", []))
        return tagged_address, "ambiguous", _compact_parse_error(exc)
    except Exception as exc:
        return {}, "error", f"{type(exc).__name__}: {exc}"


def _tag_repeated_label_address(parsed_tokens: list[tuple[str, str]]) -> dict[str, str]:
    tagged_address: dict[str, list[str]] = {}

    for token, label in parsed_tokens:
        tagged_address.setdefault(label, []).append(token)

    return {label: _normalize_component(" ".join(tokens)) for label, tokens in tagged_address.items()}


def _extract_components(tagged_address: dict[str, str]) -> dict[str, str | None]:
    components: dict[str, str | None] = {}

    for source_label, target_column in LABEL_MAP.items():
        component = _normalize_component(tagged_address.get(source_label))
        if target_column in {"predir", "postdir"} and component:
            component = DIRECTION_ABBREVIATIONS.get(component, component)
        components[target_column] = component

    return components


def _build_po_box(tagged_address: dict[str, str]) -> str | None:
    po_box = _join_parts([tagged_address.get(label) for label in PO_BOX_LABELS])
    if po_box is None:
        return None

    return _normalize_component(po_box.replace("P O", "PO").replace("P.O.", "PO"))


def _build_cleaned_owneraddr(components: dict[str, str | None], owneraddr_raw: str) -> str | None:
    po_box = components.get("po_box")
    unit = _join_parts([components.get("unit_type"), components.get("unit_number")])

    if po_box:
        return _join_parts([po_box, unit])

    street_address = _join_parts(
        [
            components.get("house_number"),
            components.get("predir"),
            components.get("street_name"),
            components.get("street_suffix"),
            components.get("postdir"),
        ]
    )
    cleaned_address = _join_parts([street_address, unit])
    if cleaned_address:
        return cleaned_address

    return _normalize_component(owneraddr_raw)


def _build_canonical_address_key(
    components: dict[str, str | None],
    ownercity: str | None,
    ownerstate: str | None,
    ownercountry: str | None,
    ownerzip_canonical: str | None,
    embedded_zip: str | None,
) -> str | None:
    address_part = components.get("po_box") or _join_parts(
        [
            components.get("house_number"),
            components.get("predir"),
            components.get("street_name"),
            components.get("street_suffix"),
            components.get("postdir"),
            components.get("unit_type"),
            components.get("unit_number"),
        ]
    )
    zip_part = ownerzip_canonical or embedded_zip
    key_parts = [
        address_part,
        _normalize_component(ownercity),
        _normalize_component(ownerstate),
        _normalize_component(ownercountry),
        zip_part,
    ]
    canonical_key = "|".join(part or "" for part in key_parts)

    if canonical_key == "||||":
        return None

    return canonical_key


def _extract_zip(value: str) -> str | None:
    match = ZIP_RE.search(value)
    if not match:
        return None

    if match.group(2):
        return f"{match.group(1)}-{match.group(2)}"

    return match.group(1)


def _compact_parse_error(exc: Exception) -> str:
    message = getattr(exc, "message", None) or str(exc)
    return SPACE_RE.sub(" ", message).strip()


def _join_parts(parts: list[Any]) -> str | None:
    clean_parts = [_normalize_component(part) for part in parts]
    clean_parts = [part for part in clean_parts if part]

    if not clean_parts:
        return None

    return " ".join(clean_parts)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None

    text = SPACE_RE.sub(" ", str(value)).strip()
    if not text:
        return None

    return text


def _normalize_component(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None

    text = text.upper()
    text = text.replace(",", " ")
    text = text.replace(".", " ")
    text = SPACE_RE.sub(" ", text).strip()

    return text or None
