import logging
from collections import defaultdict

from include.prcl_owner_matching import owner_match_input_sql

logger = logging.getLogger(__name__)

TARGET_SCHEMA = "current"
MART_TABLE = "parcel_owner_mart"
REVIEWS_TABLE = "prcl_owner_match_reviews"
REVIEW_QUEUE_VIEW = "prcl_owner_match_review_queue"
REVIEW_GROUP_TABLE = "prcl_owner_review_groups"

AUTO_MATCH_WEIGHT_THRESHOLD = 42.0
POSSIBLE_MATCH_WEIGHT_THRESHOLD = 30.0
INFERRED_GROUP_DISCREPANCY_TYPE = "accepted_owner_group"
INFERRED_GROUP_REVIEWER = "system_owner_group"


def rebuild_parcel_owner_mart(
    postgres_conn_id: str = "cdw-dev",
    auto_match_weight_threshold: float = AUTO_MATCH_WEIGHT_THRESHOLD,
    possible_match_weight_threshold: float = POSSIBLE_MATCH_WEIGHT_THRESHOLD,
) -> int:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, log_sql=False)
    conn = hook.get_conn()

    try:
        with conn.cursor() as cursor:
            cursor.execute(_review_table_sql())
            cursor.execute(
                _review_queue_view_sql(
                    possible_match_weight_threshold=possible_match_weight_threshold,
                    auto_match_weight_threshold=auto_match_weight_threshold,
                )
            )
            cursor.execute(
                _parcel_owner_mart_sql(
                    auto_match_weight_threshold=auto_match_weight_threshold,
                )
            )
            cursor.execute(f"select count(*) from {TARGET_SCHEMA}.{MART_TABLE};")
            mart_row_count = cursor.fetchone()[0]

        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Failed to rebuild %s.%s", TARGET_SCHEMA, MART_TABLE)
        raise
    finally:
        conn.close()

    logger.info("Rebuilt %s.%s with %s rows", TARGET_SCHEMA, MART_TABLE, mart_row_count)
    return mart_row_count


def apply_accepted_review_groups(
    postgres_conn_id: str = "cdw-dev",
    possible_match_weight_threshold: float = POSSIBLE_MATCH_WEIGHT_THRESHOLD,
    auto_match_weight_threshold: float = AUTO_MATCH_WEIGHT_THRESHOLD,
) -> int:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, log_sql=False)
    conn = hook.get_conn()

    try:
        with conn.cursor() as cursor:
            cursor.execute(_review_table_sql())
            accepted_edges = _accepted_review_edges(cursor)
            owner_group_rows = _owner_group_rows(accepted_edges)
            _replace_review_group_table(cursor, owner_group_rows)
            inferred_review_count = _insert_inferred_group_reviews(
                cursor,
                possible_match_weight_threshold=possible_match_weight_threshold,
                auto_match_weight_threshold=auto_match_weight_threshold,
            )

        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Failed to apply accepted owner review groups")
        raise
    finally:
        conn.close()

    logger.info("Inserted %s inferred accepted owner review rows", inferred_review_count)
    return inferred_review_count


def _accepted_review_edges(cursor) -> list[tuple[str, str]]:
    cursor.execute(
        f"""
        select
            c.owner_record_key_l,
            c.owner_record_key_r
        from {TARGET_SCHEMA}.int_prcl_owner_match_candidates c
        join {TARGET_SCHEMA}.{REVIEWS_TABLE} r
            on r.match_id = c.match_id
        where r.review_decision = 'accepted'
          and c.owner_record_key_l is not null
          and c.owner_record_key_r is not null;
        """
    )
    return [(row[0], row[1]) for row in cursor.fetchall()]


def _owner_group_rows(accepted_edges: list[tuple[str, str]]) -> list[tuple[str, str, int]]:
    parent: dict[str, str] = {}

    def find(owner_record_key: str) -> str:
        parent.setdefault(owner_record_key, owner_record_key)
        if parent[owner_record_key] != owner_record_key:
            parent[owner_record_key] = find(parent[owner_record_key])

        return parent[owner_record_key]

    def union(left_key: str, right_key: str) -> None:
        left_root = find(left_key)
        right_root = find(right_key)

        if left_root == right_root:
            return

        parent[max(left_root, right_root)] = min(left_root, right_root)

    for left_key, right_key in accepted_edges:
        union(left_key, right_key)

    components: dict[str, list[str]] = defaultdict(list)
    for owner_record_key in parent:
        components[find(owner_record_key)].append(owner_record_key)

    owner_group_rows = []
    for owner_record_keys in components.values():
        if len(owner_record_keys) < 2:
            continue

        owner_group_key = min(owner_record_keys)
        group_size = len(owner_record_keys)
        owner_group_rows.extend(
            (owner_record_key, owner_group_key, group_size)
            for owner_record_key in owner_record_keys
        )

    return sorted(owner_group_rows)


def _replace_review_group_table(
    cursor,
    owner_group_rows: list[tuple[str, str, int]],
) -> None:
    from psycopg2.extras import execute_values

    cursor.execute(
        f"""
        drop table if exists {TARGET_SCHEMA}.{REVIEW_GROUP_TABLE};

        create table {TARGET_SCHEMA}.{REVIEW_GROUP_TABLE} (
            owner_record_key text primary key,
            owner_group_key text not null,
            owner_group_size integer not null,
            created_at timestamptz not null default now()
        );
        """
    )

    if owner_group_rows:
        execute_values(
            cursor,
            f"""
            insert into {TARGET_SCHEMA}.{REVIEW_GROUP_TABLE} (
                owner_record_key,
                owner_group_key,
                owner_group_size
            )
            values %s
            """,
            owner_group_rows,
            page_size=1000,
        )

    cursor.execute(
        f"""
        create index {REVIEW_GROUP_TABLE}_group_key_idx
            on {TARGET_SCHEMA}.{REVIEW_GROUP_TABLE} (owner_group_key);
        """
    )


def _insert_inferred_group_reviews(
    cursor,
    possible_match_weight_threshold: float,
    auto_match_weight_threshold: float,
) -> int:
    cursor.execute(
        f"""
        with inferred_candidates as (
            select
                c.match_id
            from {TARGET_SCHEMA}.int_prcl_owner_match_candidates c
            join {TARGET_SCHEMA}.{REVIEW_GROUP_TABLE} gl
                on gl.owner_record_key = c.owner_record_key_l
            join {TARGET_SCHEMA}.{REVIEW_GROUP_TABLE} gr
                on gr.owner_record_key = c.owner_record_key_r
            left join {TARGET_SCHEMA}.{REVIEWS_TABLE} r
                on r.match_id = c.match_id
            where gl.owner_group_key = gr.owner_group_key
              and c.match_weight >= {possible_match_weight_threshold}
              and c.match_weight < {auto_match_weight_threshold}
              and r.match_id is null
        ),

        inserted as (
            insert into {TARGET_SCHEMA}.{REVIEWS_TABLE} (
                match_id,
                review_decision,
                reviewer,
                reviewed_at,
                review_notes,
                discrepancy_type,
                other_reason
            )
            select
                match_id,
                'accepted',
                '{INFERRED_GROUP_REVIEWER}',
                now(),
                'Auto accepted: owner records are already connected through accepted human-reviewed matches.',
                '{INFERRED_GROUP_DISCREPANCY_TYPE}',
                null
            from inferred_candidates
            on conflict (match_id) do nothing
            returning match_id
        )

        select count(*) from inserted;
        """
    )
    return int(cursor.fetchone()[0])


def _review_table_sql() -> str:
    return f"""
        create table if not exists {TARGET_SCHEMA}.{REVIEWS_TABLE} (
            match_id text primary key,
            review_decision text not null
                check (review_decision in ('accepted', 'rejected', 'needs_more_info')),
            reviewer text not null default current_user,
            reviewed_at timestamptz not null default now(),
            review_notes text,
            discrepancy_type text,
            other_reason text
        );

        alter table {TARGET_SCHEMA}.{REVIEWS_TABLE}
            add column if not exists discrepancy_type text;

        alter table {TARGET_SCHEMA}.{REVIEWS_TABLE}
            add column if not exists other_reason text;

        create index if not exists prcl_owner_match_reviews_decision_idx
            on {TARGET_SCHEMA}.{REVIEWS_TABLE} (review_decision);

        create index if not exists prcl_owner_match_reviews_discrepancy_type_idx
            on {TARGET_SCHEMA}.{REVIEWS_TABLE} (discrepancy_type);
    """


def _review_queue_view_sql(
    possible_match_weight_threshold: float,
    auto_match_weight_threshold: float,
) -> str:
    return f"""
        drop view if exists {TARGET_SCHEMA}.{REVIEW_QUEUE_VIEW};

        create view {TARGET_SCHEMA}.{REVIEW_QUEUE_VIEW} as
        select
            c.match_id,
            coalesce(r.review_decision, 'pending') as review_status,
            r.reviewer,
            r.reviewed_at,
            r.review_notes,
            r.discrepancy_type,
            r.other_reason,
            c.match_probability,
            c.match_weight,
            c.owner_record_id_l,
            c.owner_record_id_r,
            c.owner_record_key_l,
            c.owner_record_key_r,
            c.ownername_l,
            c.ownername_r,
            c.ownername2_l,
            c.ownername2_r,
            c.owneraddr_raw_l,
            c.owneraddr_raw_r,
            c.owneraddr_cleaned_l,
            c.owneraddr_cleaned_r,
            c.house_number_l,
            c.house_number_r,
            c.predir_l,
            c.predir_r,
            c.street_name_l,
            c.street_name_r,
            c.street_suffix_l,
            c.street_suffix_r,
            c.postdir_l,
            c.postdir_r,
            c.unit_type_l,
            c.unit_type_r,
            c.unit_number_l,
            c.unit_number_r,
            c.po_box_l,
            c.po_box_r,
            c.embedded_zip_from_owneraddr_l,
            c.embedded_zip_from_owneraddr_r,
            c.ownerzip_canonical_l,
            c.ownerzip_canonical_r,
            c.canonical_address_key_l,
            c.canonical_address_key_r,
            c.gamma_ownername_norm,
            c.gamma_ownername2_norm,
            c.gamma_owneraddr_cleaned_norm,
            c.gamma_house_number,
            c.gamma_street_name_norm,
            c.gamma_street_suffix,
            c.gamma_unit_number_norm,
            c.gamma_po_box,
            c.gamma_ownerzip5
        from {TARGET_SCHEMA}.int_prcl_owner_match_candidates c
        left join {TARGET_SCHEMA}.{REVIEWS_TABLE} r
            on r.match_id = c.match_id
        where c.match_weight >= {possible_match_weight_threshold}
          and c.match_weight < {auto_match_weight_threshold};
    """


def _parcel_owner_mart_sql(auto_match_weight_threshold: float) -> str:
    return f"""
        drop table if exists {TARGET_SCHEMA}.{MART_TABLE};

        create table {TARGET_SCHEMA}.{MART_TABLE} as
        with
        candidate_edges as (
            select
                c.match_id,
                c.owner_record_id_l,
                c.owner_record_id_r,
                c.owner_record_key_l,
                c.owner_record_key_r,
                c.match_probability,
                c.match_weight,
                case
                    when r.review_decision = 'accepted'
                        then 'splink_matched_human_reviewed'
                    else 'splink_auto_high_confidence'
                end as match_method,
                coalesce(r.review_decision, 'auto_accepted') as review_status,
                r.reviewer,
                r.reviewed_at,
                r.review_notes,
                r.discrepancy_type,
                r.other_reason,
                c.ownername_l,
                c.ownername_r,
                c.ownername2_l,
                c.ownername2_r,
                c.owneraddr_raw_l,
                c.owneraddr_raw_r,
                c.owneraddr_cleaned_l,
                c.owneraddr_cleaned_r,
                c.house_number_l,
                c.house_number_r,
                c.predir_l,
                c.predir_r,
                c.street_name_l,
                c.street_name_r,
                c.street_suffix_l,
                c.street_suffix_r,
                c.postdir_l,
                c.postdir_r,
                c.unit_type_l,
                c.unit_type_r,
                c.unit_number_l,
                c.unit_number_r,
                c.po_box_l,
                c.po_box_r,
                c.embedded_zip_from_owneraddr_l,
                c.embedded_zip_from_owneraddr_r,
                c.ownerzip_canonical_l,
                c.ownerzip_canonical_r,
                c.canonical_address_key_l,
                c.canonical_address_key_r,
                c.gamma_ownername_norm,
                c.gamma_ownername2_norm,
                c.gamma_owneraddr_cleaned_norm,
                c.gamma_house_number,
                c.gamma_street_name_norm,
                c.gamma_street_suffix,
                c.gamma_unit_number_norm,
                c.gamma_po_box,
                c.gamma_ownerzip5
            from {TARGET_SCHEMA}.int_prcl_owner_match_candidates c
            left join {TARGET_SCHEMA}.{REVIEWS_TABLE} r
                on r.match_id = c.match_id
            where r.review_decision = 'accepted'
               or (
                    c.match_weight >= {auto_match_weight_threshold}
                    and coalesce(r.review_decision, '') <> 'rejected'
               )
        ),

        candidate_owner_ids as (
            select owner_record_id_l as owner_record_id from candidate_edges
            union
            select owner_record_id_r as owner_record_id from candidate_edges
        ),

        owner_records_raw as (
            select
                owner_input.*,
                nullif(split_part(owner_input.canonical_address_key, '|', 2), '') as ownercity_norm,
                nullif(split_part(owner_input.canonical_address_key, '|', 3), '') as ownerstate_norm,
                nullif(split_part(owner_input.canonical_address_key, '|', 4), '') as ownercountry_norm
            from ({owner_match_input_sql()}) owner_input
            join candidate_owner_ids coi
                on coi.owner_record_id = owner_input.owner_record_id
        ),

        owner_records as (
            select
                owner_records_raw.*,
                md5(concat_ws(
                    '|',
                    coalesce(ownername, ''),
                    coalesce(ownername2, ''),
                    coalesce(owneraddr_raw, ''),
                    coalesce(ownercity_norm, ''),
                    coalesce(ownerstate_norm, ''),
                    coalesce(ownercountry_norm, ''),
                    coalesce(ownerzip_canonical, '')
                )) as source_owner_match_key
            from owner_records_raw
        ),

        parcel_source_raw as (
            select
                p.handle as parcel_handle,
                p.parcel9,
                p.cityblock,
                p.parcel,
                p.ownercode,
                p.ownerrank,
                nullif(trim(concat_ws(
                    ' ',
                    nullif(trim(p.lowaddrnum), ''),
                    nullif(trim(p.stpredir), ''),
                    nullif(trim(p.stname), ''),
                    nullif(trim(p.sttype), ''),
                    nullif(trim(p.stsufdir), ''),
                    nullif(trim(p.stdunitnum), '')
                )), '') as parcel_address_raw,
                nullif(trim(p.zip), '') as parcel_zip,
                p.ward10,
                p.nbrhd,
                p.zoning,
                p.propertyclasscode,
                p.asdland,
                p.asdimprove,
                p.asdtotal,
                {_clean_text_sql("p.ownername")} as ownername_clean,
                {_clean_text_sql("p.ownername2")} as ownername2_clean,
                {_clean_text_sql("p.owneraddr")} as owneraddr_raw_clean,
                {_normalize_component_sql("p.ownercity")} as ownercity_norm,
                {_normalize_component_sql("p.ownerstate")} as ownerstate_norm,
                {_normalize_component_sql("p.ownercountry")} as ownercountry_norm,
                {_canonical_zip_sql("p.ownerzip")} as ownerzip_canonical,
                {_clean_text_sql("p.ownercity")} as ownercity,
                {_clean_text_sql("p.ownerstate")} as ownerstate,
                {_clean_text_sql("p.ownercountry")} as ownercountry,
                {_clean_text_sql("p.ownerzip")} as ownerzip
            from staging.prcl_prcl p
        ),

        parcel_source as (
            select
                parcel_source_raw.*,
                md5(concat_ws(
                    '|',
                    coalesce(ownername_clean, ''),
                    coalesce(ownername2_clean, ''),
                    coalesce(owneraddr_raw_clean, ''),
                    coalesce(ownercity_norm, ''),
                    coalesce(ownerstate_norm, ''),
                    coalesce(ownercountry_norm, ''),
                    coalesce(ownerzip_canonical, '')
                )) as source_owner_match_key
            from parcel_source_raw
        ),

        owner_parcel_rollup as (
            select
                o.owner_record_id,
                count(distinct p.parcel_handle) as parcel_count,
                array_remove(array_agg(distinct p.parcel_handle order by p.parcel_handle), null) as parcel_handles,
                coalesce(
                    jsonb_agg(
                        distinct jsonb_build_object(
                            'parcel_handle', p.parcel_handle,
                            'parcel9', p.parcel9,
                            'cityblock', p.cityblock,
                            'parcel', p.parcel,
                            'ownercode', p.ownercode,
                            'ownerrank', p.ownerrank,
                            'parcel_address_raw', p.parcel_address_raw,
                            'parcel_zip', p.parcel_zip,
                            'ward10', p.ward10,
                            'nbrhd', p.nbrhd,
                            'zoning', p.zoning,
                            'propertyclasscode', p.propertyclasscode,
                            'asdland', p.asdland,
                            'asdimprove', p.asdimprove,
                            'asdtotal', p.asdtotal,
                            'ownercity', p.ownercity,
                            'ownerstate', p.ownerstate,
                            'ownercountry', p.ownercountry,
                            'ownerzip', p.ownerzip
                        )
                    ) filter (where p.parcel_handle is not null),
                    '[]'::jsonb
                ) as parcels
            from owner_records o
            left join parcel_source p
                on p.source_owner_match_key = o.source_owner_match_key
            group by o.owner_record_id
        )

        select
            ce.match_id,
            ce.match_method,
            ce.review_status,
            ce.reviewer,
            ce.reviewed_at,
            ce.review_notes,
            ce.discrepancy_type,
            ce.other_reason,
            ce.match_probability,
            ce.match_weight,
            ce.owner_record_id_l,
            ce.owner_record_id_r,
            ce.owner_record_key_l,
            ce.owner_record_key_r,
            ce.ownername_l,
            ce.ownername_r,
            ce.ownername2_l,
            ce.ownername2_r,
            ce.owneraddr_raw_l,
            ce.owneraddr_raw_r,
            ce.owneraddr_cleaned_l,
            ce.owneraddr_cleaned_r,
            ce.house_number_l,
            ce.house_number_r,
            ce.predir_l,
            ce.predir_r,
            ce.street_name_l,
            ce.street_name_r,
            ce.street_suffix_l,
            ce.street_suffix_r,
            ce.postdir_l,
            ce.postdir_r,
            ce.unit_type_l,
            ce.unit_type_r,
            ce.unit_number_l,
            ce.unit_number_r,
            ce.po_box_l,
            ce.po_box_r,
            ce.embedded_zip_from_owneraddr_l,
            ce.embedded_zip_from_owneraddr_r,
            ce.ownerzip_canonical_l,
            ce.ownerzip_canonical_r,
            ce.canonical_address_key_l,
            ce.canonical_address_key_r,
            ce.gamma_ownername_norm,
            ce.gamma_ownername2_norm,
            ce.gamma_owneraddr_cleaned_norm,
            ce.gamma_house_number,
            ce.gamma_street_name_norm,
            ce.gamma_street_suffix,
            ce.gamma_unit_number_norm,
            ce.gamma_po_box,
            ce.gamma_ownerzip5,
            coalesce(pl.parcel_count, 0) as parcel_count_l,
            coalesce(pr.parcel_count, 0) as parcel_count_r,
            coalesce(pl.parcel_count, 0) + coalesce(pr.parcel_count, 0) as total_parcel_count,
            coalesce(pl.parcel_handles, array[]::text[]) as parcel_handles_l,
            coalesce(pr.parcel_handles, array[]::text[]) as parcel_handles_r,
            coalesce(pl.parcels, '[]'::jsonb) as parcels_l,
            coalesce(pr.parcels, '[]'::jsonb) as parcels_r,
            now() as created_at
        from candidate_edges ce
        left join owner_parcel_rollup pl
            on pl.owner_record_id = ce.owner_record_id_l
        left join owner_parcel_rollup pr
            on pr.owner_record_id = ce.owner_record_id_r
        order by
            ce.match_weight desc,
            ce.match_probability desc,
            ce.match_id;

        create unique index parcel_owner_mart_match_id_idx
            on {TARGET_SCHEMA}.{MART_TABLE} (match_id);

        create index parcel_owner_mart_match_weight_idx
            on {TARGET_SCHEMA}.{MART_TABLE} (match_weight desc);

        create index parcel_owner_mart_owner_records_idx
            on {TARGET_SCHEMA}.{MART_TABLE} (owner_record_id_l, owner_record_id_r);
    """


def _clean_text_sql(expression: str) -> str:
    return (
        "nullif(regexp_replace("
        f"trim(coalesce({expression}, '')), "
        "'\\s+', "
        "' ', "
        "'g'"
        "), '')"
    )


def _normalize_component_sql(expression: str) -> str:
    return (
        "nullif(trim(regexp_replace("
        f"replace(replace(upper(coalesce({expression}, '')), ',', ' '), '.', ' '), "
        "'\\s+', "
        "' ', "
        "'g'"
        ")), '')"
    )


def _canonical_zip_sql(expression: str) -> str:
    digits = f"regexp_replace(coalesce({expression}, ''), '[^0-9]+', '', 'g')"
    normalized_text = _normalize_component_sql(expression)
    return (
        "case "
        f"when length({digits}) = 9 then left({digits}, 5) || '-' || substring({digits} from 6 for 4) "
        f"when length({digits}) >= 5 then left({digits}, 5) "
        f"else {normalized_text} "
        "end"
    )
