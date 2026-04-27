import hashlib
import logging
from typing import Any

logger = logging.getLogger(__name__)

SOURCE_SCHEMA = "current"
SOURCE_TABLE = "int_prcl_owners"
TARGET_SCHEMA = "current"
TARGET_TABLE = "int_prcl_owner_match_candidates"

DEFAULT_MATCH_PROBABILITY_THRESHOLD = 0.75
RANDOM_SAMPLE_MAX_PAIRS = 1_000_000

ADDITIONAL_COLUMNS_TO_RETAIN = [
    "owner_record_key",
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
]

SPLINK_STRING_COLUMNS = [
    "unique_id",
    "owner_record_key",
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
    "ownername_norm",
    "ownername2_norm",
    "owneraddr_cleaned_norm",
    "street_name_norm",
    "unit_number_norm",
    "ownername_block_key",
    "ownerzip5",
]

OUTPUT_COLUMNS = [
    "match_id",
    "owner_record_id_l",
    "owner_record_id_r",
    "owner_record_key_l",
    "owner_record_key_r",
    "match_probability",
    "match_weight",
    "match_key",
    "ownername_l",
    "ownername_r",
    "ownername2_l",
    "ownername2_r",
    "owneraddr_raw_l",
    "owneraddr_raw_r",
    "owneraddr_cleaned_l",
    "owneraddr_cleaned_r",
    "house_number_l",
    "house_number_r",
    "predir_l",
    "predir_r",
    "street_name_l",
    "street_name_r",
    "street_suffix_l",
    "street_suffix_r",
    "postdir_l",
    "postdir_r",
    "unit_type_l",
    "unit_type_r",
    "unit_number_l",
    "unit_number_r",
    "po_box_l",
    "po_box_r",
    "embedded_zip_from_owneraddr_l",
    "embedded_zip_from_owneraddr_r",
    "ownerzip_canonical_l",
    "ownerzip_canonical_r",
    "canonical_address_key_l",
    "canonical_address_key_r",
    "gamma_ownername_norm",
    "gamma_ownername2_norm",
    "gamma_owneraddr_cleaned_norm",
    "gamma_house_number",
    "gamma_street_name_norm",
    "gamma_street_suffix",
    "gamma_unit_number_norm",
    "gamma_po_box",
    "gamma_ownerzip5",
]


def rebuild_int_prcl_owner_match_candidates(
    postgres_conn_id: str = "cdw-dev",
    threshold_match_probability: float = DEFAULT_MATCH_PROBABILITY_THRESHOLD,
) -> int:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, log_sql=False)
    owners_df = hook.get_pandas_df(_owner_match_input_sql())

    if len(owners_df) < 2:
        _replace_match_table(hook, [])
        logger.info(
            "Rebuilt %s.%s with 0 rows; fewer than two owner rows were available",
            TARGET_SCHEMA,
            TARGET_TABLE,
        )
        return 0

    predictions_df = _run_splink_predictions(owners_df, threshold_match_probability)
    prepared_df = _prepare_predictions_for_insert(predictions_df)
    _replace_match_table(hook, _dataframe_records(prepared_df))

    logger.info("Rebuilt %s.%s with %s rows", TARGET_SCHEMA, TARGET_TABLE, len(prepared_df))
    return len(prepared_df)


def _run_splink_predictions(owners_df, threshold_match_probability: float):
    try:
        from splink import DuckDBAPI, Linker
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Splink is not installed in the running Airflow environment. "
            "Run `astro dev restart --no-cache` so the image installs "
            "`splink>=4,<5` from requirements.txt."
        ) from exc

    owners_df = _prepare_splink_input(owners_df)
    settings = _build_splink_settings()
    linker = Linker(owners_df, settings, db_api=DuckDBAPI())
    _train_splink_model(linker)

    predictions = linker.inference.predict(
        threshold_match_probability=threshold_match_probability
    )
    return predictions.as_pandas_dataframe()


def _prepare_splink_input(owners_df):
    owners_df = owners_df.copy()

    for column in SPLINK_STRING_COLUMNS:
        if column in owners_df:
            owners_df[column] = owners_df[column].astype("string")

    return owners_df


def _build_splink_settings():
    from splink import SettingsCreator, block_on
    import splink.comparison_library as cl

    return SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="unique_id",
        comparisons=[
            cl.JaroWinklerAtThresholds(
                "ownername_norm",
                [0.98, 0.95, 0.90, 0.85],
            ).configure(term_frequency_adjustments=True),
            cl.JaroWinklerAtThresholds(
                "ownername2_norm",
                [0.98, 0.95, 0.90],
            ),
            cl.JaroWinklerAtThresholds(
                "owneraddr_cleaned_norm",
                [0.98, 0.95, 0.90, 0.85],
            ).configure(term_frequency_adjustments=True),
            cl.ExactMatch("house_number"),
            cl.JaroWinklerAtThresholds(
                "street_name_norm",
                [0.98, 0.95, 0.90],
            ),
            cl.ExactMatch("street_suffix"),
            cl.ExactMatch("unit_number_norm"),
            cl.ExactMatch("po_box"),
            cl.ExactMatch("ownerzip5").configure(term_frequency_adjustments=True),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("canonical_address_key"),
            block_on("ownerzip5", "house_number", "street_name_norm"),
            block_on("house_number", "street_name_norm"),
            block_on("ownername_block_key", "ownerzip5"),
            block_on("po_box", "ownerzip5"),
        ],
        retain_matching_columns=True,
        retain_intermediate_calculation_columns=True,
        additional_columns_to_retain=ADDITIONAL_COLUMNS_TO_RETAIN,
    )


def _train_splink_model(linker) -> None:
    from splink import block_on

    deterministic_rules = [
        block_on("canonical_address_key"),
        block_on("ownername_norm", "ownerzip5", "house_number"),
        block_on("ownername_norm", "owneraddr_cleaned_norm"),
    ]
    _try_splink_training_step(
        "estimate_probability_two_random_records_match",
        linker.training.estimate_probability_two_random_records_match,
        deterministic_rules,
        recall=0.70,
    )

    _try_splink_training_step(
        "estimate_u_using_random_sampling",
        linker.training.estimate_u_using_random_sampling,
        max_pairs=RANDOM_SAMPLE_MAX_PAIRS,
        seed=1,
    )

    em_training_rules = [
        block_on("canonical_address_key"),
        block_on("ownerzip5", "house_number", "street_name_norm"),
        block_on("ownername_block_key", "ownerzip5"),
    ]
    for blocking_rule in em_training_rules:
        _try_splink_training_step(
            "estimate_parameters_using_expectation_maximisation",
            linker.training.estimate_parameters_using_expectation_maximisation,
            blocking_rule,
            estimate_without_term_frequencies=True,
        )


def _try_splink_training_step(step_name: str, training_callable, *args, **kwargs) -> None:
    try:
        training_callable(*args, **kwargs)
    except Exception as exc:
        logger.warning(
            "Splink training step %s failed; continuing with defaults: %s",
            step_name,
            _compact_error(exc),
        )


def _compact_error(exc: Exception, max_length: int = 500) -> str:
    message = " ".join(str(exc).split())
    if len(message) <= max_length:
        return message

    return f"{message[:max_length]}..."


def _owner_match_input_sql() -> str:
    return f"""
        with numbered as (
            select
                row_number() over (
                    order by
                        ownername nulls last,
                        ownername2 nulls last,
                        owneraddr_raw nulls last,
                        ownerzip_canonical nulls last,
                        canonical_address_key nulls last
                ) as owner_record_id,
                *
            from {SOURCE_SCHEMA}.{SOURCE_TABLE}
        )

        select
            owner_record_id::text as unique_id,
            owner_record_id,
            md5(concat_ws(
                '|',
                coalesce(ownername, ''),
                coalesce(ownername2, ''),
                coalesce(owneraddr_raw, ''),
                coalesce(owneraddr_cleaned, ''),
                coalesce(house_number, ''),
                coalesce(predir, ''),
                coalesce(street_name, ''),
                coalesce(street_suffix, ''),
                coalesce(postdir, ''),
                coalesce(unit_type, ''),
                coalesce(unit_number, ''),
                coalesce(po_box, ''),
                coalesce(embedded_zip_from_owneraddr, ''),
                coalesce(ownerzip_canonical, ''),
                coalesce(canonical_address_key, '')
            )) as owner_record_key,
            ownername,
            ownername2,
            owneraddr_raw,
            owneraddr_cleaned,
            house_number,
            predir,
            street_name,
            street_suffix,
            postdir,
            unit_type,
            unit_number,
            po_box,
            embedded_zip_from_owneraddr,
            ownerzip_canonical,
            canonical_address_key,
            {_normalize_sql("ownername")} as ownername_norm,
            {_normalize_sql("ownername2")} as ownername2_norm,
            {_normalize_sql("coalesce(owneraddr_cleaned, owneraddr_raw)")} as owneraddr_cleaned_norm,
            {_normalize_sql("street_name")} as street_name_norm,
            {_normalize_sql("unit_number")} as unit_number_norm,
            nullif(
                left(
                    regexp_replace(upper(coalesce(ownername, '')), '[^A-Z0-9]+', '', 'g'),
                    8
                ),
                ''
            ) as ownername_block_key,
            nullif(
                left(
                    regexp_replace(
                        coalesce(ownerzip_canonical, embedded_zip_from_owneraddr, ''),
                        '[^0-9]+',
                        '',
                        'g'
                    ),
                    5
                ),
                ''
            ) as ownerzip5
        from numbered
        where coalesce(ownername, owneraddr_cleaned, owneraddr_raw, canonical_address_key) is not null
    """


def owner_match_input_sql() -> str:
    return _owner_match_input_sql()


def _normalize_sql(expression: str) -> str:
    return (
        "nullif(trim(regexp_replace("
        f"upper(coalesce({expression}, '')), "
        "'[^A-Z0-9]+', "
        "' ', "
        "'g'"
        ")), '')"
    )


def _prepare_predictions_for_insert(predictions_df):
    import pandas as pd

    prepared_df = pd.DataFrame()

    if predictions_df.empty:
        for column in OUTPUT_COLUMNS:
            prepared_df[column] = pd.Series(dtype=object)
        return prepared_df

    prepared_df["match_id"] = predictions_df.apply(
        lambda row: _match_id(row["owner_record_key_l"], row["owner_record_key_r"]),
        axis=1,
    )
    prepared_df["owner_record_id_l"] = pd.to_numeric(
        predictions_df["unique_id_l"],
        errors="coerce",
    ).astype("Int64")
    prepared_df["owner_record_id_r"] = pd.to_numeric(
        predictions_df["unique_id_r"],
        errors="coerce",
    ).astype("Int64")

    for output_column in OUTPUT_COLUMNS:
        if output_column in prepared_df:
            continue

        source_column = output_column
        if source_column in predictions_df:
            prepared_df[output_column] = predictions_df[source_column]
        else:
            prepared_df[output_column] = None

    prepared_df = prepared_df[OUTPUT_COLUMNS]
    _coerce_prediction_types(prepared_df)
    prepared_df = prepared_df.drop_duplicates(subset=["match_id"])
    prepared_df = prepared_df.sort_values(
        by=["match_probability", "match_weight"],
        ascending=[False, False],
        na_position="last",
    )

    return prepared_df


def _coerce_prediction_types(prepared_df) -> None:
    import pandas as pd

    for column in ["owner_record_id_l", "owner_record_id_r", "match_key"]:
        prepared_df[column] = pd.to_numeric(prepared_df[column], errors="coerce").astype("Int64")

    for column in ["match_probability", "match_weight"]:
        prepared_df[column] = pd.to_numeric(prepared_df[column], errors="coerce")

    gamma_columns = [column for column in OUTPUT_COLUMNS if column.startswith("gamma_")]
    for column in gamma_columns:
        prepared_df[column] = pd.to_numeric(prepared_df[column], errors="coerce").astype("Int64")


def _match_id(owner_record_key_l: Any, owner_record_key_r: Any) -> str:
    keys = sorted([str(owner_record_key_l), str(owner_record_key_r)])
    return hashlib.md5("|".join(keys).encode("utf-8")).hexdigest()


def _replace_match_table(hook, rows: list[tuple[Any, ...]]) -> None:
    from psycopg2.extras import execute_values

    conn = hook.get_conn()

    try:
        with conn.cursor() as cursor:
            _recreate_match_table(cursor)

            if rows:
                insert_sql = f"""
                    insert into {TARGET_SCHEMA}.{TARGET_TABLE} ({", ".join(OUTPUT_COLUMNS)})
                    values %s
                """
                execute_values(cursor, insert_sql, rows, page_size=1000)

            _create_indexes(cursor)

        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Failed to rebuild %s.%s", TARGET_SCHEMA, TARGET_TABLE)
        raise
    finally:
        conn.close()


def _recreate_match_table(cursor) -> None:
    cursor.execute(
        f"""
        drop view if exists {TARGET_SCHEMA}.prcl_owner_match_review_queue;

        drop table if exists {TARGET_SCHEMA}.{TARGET_TABLE};

        create table {TARGET_SCHEMA}.{TARGET_TABLE} (
            match_id text primary key,
            owner_record_id_l bigint,
            owner_record_id_r bigint,
            owner_record_key_l text,
            owner_record_key_r text,
            match_probability double precision,
            match_weight double precision,
            match_key integer,
            ownername_l text,
            ownername_r text,
            ownername2_l text,
            ownername2_r text,
            owneraddr_raw_l text,
            owneraddr_raw_r text,
            owneraddr_cleaned_l text,
            owneraddr_cleaned_r text,
            house_number_l text,
            house_number_r text,
            predir_l text,
            predir_r text,
            street_name_l text,
            street_name_r text,
            street_suffix_l text,
            street_suffix_r text,
            postdir_l text,
            postdir_r text,
            unit_type_l text,
            unit_type_r text,
            unit_number_l text,
            unit_number_r text,
            po_box_l text,
            po_box_r text,
            embedded_zip_from_owneraddr_l text,
            embedded_zip_from_owneraddr_r text,
            ownerzip_canonical_l text,
            ownerzip_canonical_r text,
            canonical_address_key_l text,
            canonical_address_key_r text,
            gamma_ownername_norm integer,
            gamma_ownername2_norm integer,
            gamma_owneraddr_cleaned_norm integer,
            gamma_house_number integer,
            gamma_street_name_norm integer,
            gamma_street_suffix integer,
            gamma_unit_number_norm integer,
            gamma_po_box integer,
            gamma_ownerzip5 integer,
            created_at timestamptz not null default now()
        );
        """
    )


def _create_indexes(cursor) -> None:
    cursor.execute(
        f"""
        create index int_prcl_owner_match_candidates_probability_idx
            on {TARGET_SCHEMA}.{TARGET_TABLE} (match_probability desc);

        create index int_prcl_owner_match_candidates_left_owner_idx
            on {TARGET_SCHEMA}.{TARGET_TABLE} (owner_record_id_l);

        create index int_prcl_owner_match_candidates_right_owner_idx
            on {TARGET_SCHEMA}.{TARGET_TABLE} (owner_record_id_r);
        """
    )


def _dataframe_records(prepared_df) -> list[tuple[Any, ...]]:
    return [
        tuple(_python_scalar(value) for value in row)
        for row in prepared_df[OUTPUT_COLUMNS].itertuples(index=False, name=None)
    ]


def _python_scalar(value: Any) -> Any:
    try:
        import pandas as pd

        if pd.isna(value):
            return None
    except TypeError:
        pass

    if hasattr(value, "item"):
        return value.item()

    return value
