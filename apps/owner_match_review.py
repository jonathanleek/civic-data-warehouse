import os
from collections import defaultdict
from contextlib import contextmanager
from difflib import SequenceMatcher
from html import escape
from typing import Any

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st
from streamlit_shortcuts import shortcut_button


QUEUE_TABLE = "current.prcl_owner_match_review_queue"
REVIEWS_TABLE = "current.prcl_owner_match_reviews"
MATCH_CANDIDATES_TABLE = "current.int_prcl_owner_match_candidates"
REVIEW_GROUP_TABLE = "current.prcl_owner_review_groups"
SUGGESTIONS_TABLE = "current.prcl_owner_match_suggestions"
POSSIBLE_MATCH_WEIGHT_THRESHOLD = 30.0
AUTO_MATCH_WEIGHT_THRESHOLD = 42.0
SUGGESTION_MIN_LABELED_EXAMPLES = 3
SUGGESTION_MIN_CONFIDENCE = 0.80

EXACT_GAMMA_SIGNALS = {
    "gamma_house_number",
    "gamma_street_suffix",
    "gamma_unit_number_norm",
    "gamma_po_box",
    "gamma_ownerzip5",
}

FUZZY_GAMMA_THRESHOLDS = {
    "gamma_ownername_norm": [0.98, 0.95, 0.90, 0.85],
    "gamma_ownername2_norm": [0.98, 0.95, 0.90],
    "gamma_owneraddr_cleaned_norm": [0.98, 0.95, 0.90, 0.85],
    "gamma_street_name_norm": [0.98, 0.95, 0.90],
}

GAMMA_LABELS = {
    "gamma_ownername_norm": "Owner name",
    "gamma_ownername2_norm": "Owner name 2",
    "gamma_owneraddr_cleaned_norm": "Owner address",
    "gamma_house_number": "House number",
    "gamma_street_name_norm": "Street name",
    "gamma_street_suffix": "Street suffix",
    "gamma_unit_number_norm": "Unit number",
    "gamma_po_box": "PO box",
    "gamma_ownerzip5": "Owner ZIP",
}

DISCREPANCY_TYPE_OPTIONS = [
    "",
    "missing_country",
    "state_country_mixup",
    "misspelling",
    "middle_initial",
    "name_order",
    "person_vs_llc",
    "differing_llc_name",
    "one_owner_vs_two",
    "ownername_split",
    "ownername2_difference",
    "punctuation",
    "abbreviation",
    "business_suffix",
    "care_of_difference",
    "dba_or_aka",
    "trust_difference",
    "lessee_question",
    "same_family_or_surname_change",
    "department_difference",
    "various_stl_city_dept",
    "address_cleanup",
    "unit_difference",
    "zip_difference",
    "accepted_owner_group",
    "OTHER",
]

DISCREPANCY_TYPE_LABELS = {
    "": "(none)",
    "missing_country": "Missing country",
    "state_country_mixup": "State/country mixup",
    "misspelling": "Misspelling",
    "middle_initial": "Middle initial",
    "name_order": "NAME ORDER",
    "person_vs_llc": "PERSON vs. LLC",
    "differing_llc_name": "Differing LLC name",
    "one_owner_vs_two": "1 OWNER vs. 2",
    "ownername_split": "Owner name split",
    "ownername2_difference": "Owner name 2 difference",
    "punctuation": "Punctuation",
    "abbreviation": "Abbreviation",
    "business_suffix": "Business suffix",
    "care_of_difference": "C/O difference",
    "dba_or_aka": "DBA / AKA",
    "trust_difference": "Trust difference",
    "lessee_question": "Lessee?",
    "same_family_or_surname_change": "Same family / surname change",
    "department_difference": "Department difference",
    "various_stl_city_dept": "VARIOUS STL CITY DEPT",
    "address_cleanup": "Address cleanup",
    "unit_difference": "Unit difference",
    "zip_difference": "ZIP difference",
    "accepted_owner_group": "Accepted owner group",
    "OTHER": "OTHER",
}


st.set_page_config(
    page_title="Owner Match Review",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .owner-field {
        line-height: 1.55;
        margin: 0 0 0.7rem;
    }

    .owner-field-label {
        font-weight: 700;
    }

    .diff-value {
        white-space: pre-wrap;
        overflow-wrap: anywhere;
    }

    .diff-left {
        background: #ffe1e1;
        border-radius: 3px;
        color: #991b1b;
        padding: 0 1px;
    }

    .diff-right {
        background: #dcfce7;
        border-radius: 3px;
        color: #14532d;
        padding: 0 1px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def secret_or_default(name: str, default: str) -> str:
    try:
        return st.secrets.get(name, default)
    except Exception:
        return default


def format_discrepancy_type(value: str) -> str:
    return DISCREPANCY_TYPE_LABELS.get(value, value.replace("_", " ").title())


def db_config() -> dict[str, Any]:
    return {
        "host": os.getenv("CDW_DB_HOST", secret_or_default("CDW_DB_HOST", "localhost")),
        "port": int(os.getenv("CDW_DB_PORT", secret_or_default("CDW_DB_PORT", "5433"))),
        "dbname": os.getenv("CDW_DB_NAME", secret_or_default("CDW_DB_NAME", "cdw")),
        "user": os.getenv("CDW_DB_USER", secret_or_default("CDW_DB_USER", "cdw_user")),
        "password": os.getenv(
            "CDW_DB_PASSWORD",
            secret_or_default("CDW_DB_PASSWORD", "cdw_password"),
        ),
    }


@contextmanager
def db_connection():
    conn = psycopg2.connect(**db_config())
    try:
        yield conn
    finally:
        conn.close()


def query_one(sql: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    with db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(sql, params or {})
            row = cursor.fetchone()
            return dict(row) if row else None


def query_df(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    with db_connection() as conn:
        return pd.read_sql_query(sql, conn, params=params or {})


def execute(sql: str, params: dict[str, Any]) -> None:
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
        conn.commit()


def refresh_match_suggestions(
    min_labeled_examples: int = SUGGESTION_MIN_LABELED_EXAMPLES,
    min_confidence: float = SUGGESTION_MIN_CONFIDENCE,
) -> int:
    row = query_one(
        f"""
        drop table if exists {SUGGESTIONS_TABLE};

        create table {SUGGESTIONS_TABLE} as
        with features as (
            select
                q.match_id,
                q.review_status,
                q.match_weight,
                q.match_probability,
                q.owner_record_key_l,
                q.owner_record_key_r,
                concat_ws(
                    '|',
                    q.gamma_ownername_norm,
                    q.gamma_ownername2_norm,
                    q.gamma_owneraddr_cleaned_norm,
                    q.gamma_house_number,
                    q.gamma_street_name_norm,
                    q.gamma_street_suffix,
                    q.gamma_unit_number_norm,
                    q.gamma_po_box,
                    q.gamma_ownerzip5,
                    (coalesce(q.ownername_l, '') = coalesce(q.ownername_r, ''))::text,
                    (coalesce(q.owneraddr_cleaned_l, '') = coalesce(q.owneraddr_cleaned_r, ''))::text,
                    (coalesce(q.ownerzip_canonical_l, '') = coalesce(q.ownerzip_canonical_r, ''))::text
                ) as feature_signature
            from {QUEUE_TABLE} q
        ),

        label_counts as (
            select
                feature_signature,
                review_status as suggested_decision,
                count(*) as label_count
            from features
            where review_status in ('accepted', 'rejected', 'needs_more_info')
            group by feature_signature, review_status
        ),

        label_totals as (
            select
                feature_signature,
                sum(label_count) as total_labeled
            from label_counts
            group by feature_signature
        ),

        ranked as (
            select
                lc.feature_signature,
                lc.suggested_decision,
                lc.label_count,
                lt.total_labeled,
                lc.label_count::numeric / nullif(lt.total_labeled, 0) as suggestion_confidence,
                row_number() over (
                    partition by lc.feature_signature
                    order by lc.label_count desc, lc.suggested_decision
                ) as rn
            from label_counts lc
            join label_totals lt
                on lt.feature_signature = lc.feature_signature
        )

        select
            f.match_id,
            r.suggested_decision,
            r.suggestion_confidence,
            r.label_count,
            r.total_labeled,
            f.feature_signature,
            f.match_weight,
            f.match_probability,
            f.owner_record_key_l,
            f.owner_record_key_r,
            now() as generated_at
        from features f
        join ranked r
            on r.feature_signature = f.feature_signature
           and r.rn = 1
        where f.review_status = 'pending'
          and r.total_labeled >= %(min_labeled_examples)s
          and r.suggestion_confidence >= %(min_confidence)s;

        alter table {SUGGESTIONS_TABLE}
            add primary key (match_id);

        create index prcl_owner_match_suggestions_decision_idx
            on {SUGGESTIONS_TABLE} (suggested_decision, suggestion_confidence desc);

        select count(*)::int as suggestion_count
        from {SUGGESTIONS_TABLE};
        """,
        {
            "min_labeled_examples": min_labeled_examples,
            "min_confidence": min_confidence,
        },
    )
    return int(row["suggestion_count"]) if row else 0


def suggestion_counts() -> dict[str, int]:
    try:
        rows = query_df(
            f"""
            select suggested_decision, count(*)::int as count
            from {SUGGESTIONS_TABLE}
            group by suggested_decision
            """
        )
    except Exception:
        return {"accepted": 0, "rejected": 0, "needs_more_info": 0, "total": 0}

    counts = {"accepted": 0, "rejected": 0, "needs_more_info": 0, "total": 0}
    for row in rows.itertuples(index=False):
        counts[row.suggested_decision] = int(row.count)
        counts["total"] += int(row.count)

    return counts


def ensure_suggestions_table() -> None:
    execute(
        f"""
        create table if not exists {SUGGESTIONS_TABLE} (
            match_id text primary key,
            suggested_decision text,
            suggestion_confidence numeric,
            label_count bigint,
            total_labeled bigint,
            feature_signature text,
            match_weight double precision,
            match_probability double precision,
            owner_record_key_l text,
            owner_record_key_r text,
            generated_at timestamptz
        )
        """,
        {},
    )


def owner_group_rows(accepted_edges: list[tuple[str, str]]) -> list[tuple[str, str, int]]:
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

    rows = []
    for owner_record_keys in components.values():
        if len(owner_record_keys) < 2:
            continue

        owner_group_key = min(owner_record_keys)
        owner_group_size = len(owner_record_keys)
        rows.extend(
            (owner_record_key, owner_group_key, owner_group_size)
            for owner_record_key in owner_record_keys
        )

    return sorted(rows)


def apply_owner_review_groups() -> int:
    accepted_edges_df = query_df(
        f"""
        select
            c.owner_record_key_l,
            c.owner_record_key_r
        from {MATCH_CANDIDATES_TABLE} c
        join {REVIEWS_TABLE} r
            on r.match_id = c.match_id
        where r.review_decision = 'accepted'
          and c.owner_record_key_l is not null
          and c.owner_record_key_r is not null
        """
    )
    accepted_edges = [
        (row.owner_record_key_l, row.owner_record_key_r)
        for row in accepted_edges_df.itertuples(index=False)
    ]
    group_rows = owner_group_rows(accepted_edges)

    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                drop table if exists {REVIEW_GROUP_TABLE};

                create table {REVIEW_GROUP_TABLE} (
                    owner_record_key text primary key,
                    owner_group_key text not null,
                    owner_group_size integer not null,
                    created_at timestamptz not null default now()
                );
                """
            )

            if group_rows:
                psycopg2.extras.execute_values(
                    cursor,
                    f"""
                    insert into {REVIEW_GROUP_TABLE} (
                        owner_record_key,
                        owner_group_key,
                        owner_group_size
                    )
                    values %s
                    """,
                    group_rows,
                    page_size=1000,
                )

            cursor.execute(
                f"""
                create index prcl_owner_review_groups_group_key_idx
                    on {REVIEW_GROUP_TABLE} (owner_group_key);

                with inferred_candidates as (
                    select
                        c.match_id
                    from {MATCH_CANDIDATES_TABLE} c
                    join {REVIEW_GROUP_TABLE} gl
                        on gl.owner_record_key = c.owner_record_key_l
                    join {REVIEW_GROUP_TABLE} gr
                        on gr.owner_record_key = c.owner_record_key_r
                    left join {REVIEWS_TABLE} r
                        on r.match_id = c.match_id
                    where gl.owner_group_key = gr.owner_group_key
                      and c.match_weight >= %(possible_match_weight_threshold)s
                      and c.match_weight < %(auto_match_weight_threshold)s
                      and r.match_id is null
                ),

                inserted as (
                    insert into {REVIEWS_TABLE} (
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
                        'system_owner_group',
                        now(),
                        'Auto accepted: owner records are already connected through accepted human-reviewed matches.',
                        'accepted_owner_group',
                        null
                    from inferred_candidates
                    on conflict (match_id) do nothing
                    returning match_id
                )

                select count(*) from inserted;
                """,
                {
                    "possible_match_weight_threshold": POSSIBLE_MATCH_WEIGHT_THRESHOLD,
                    "auto_match_weight_threshold": AUTO_MATCH_WEIGHT_THRESHOLD,
                },
            )
            inferred_review_count = int(cursor.fetchone()[0])

        conn.commit()

    return inferred_review_count


def queue_counts(min_weight: float, max_weight: float) -> dict[str, int]:
    row = query_one(
        f"""
        select
            count(*) filter (where review_status = 'pending')::int as pending,
            count(*) filter (where review_status = 'accepted')::int as accepted,
            count(*) filter (where review_status = 'rejected')::int as rejected,
            count(*) filter (where review_status = 'needs_more_info')::int as needs_more_info,
            count(*)::int as total
        from {QUEUE_TABLE}
        where match_weight >= %(min_weight)s
          and match_weight < %(max_weight)s
        """,
        {"min_weight": min_weight, "max_weight": max_weight},
    )
    return row or {
        "pending": 0,
        "accepted": 0,
        "rejected": 0,
        "needs_more_info": 0,
        "total": 0,
    }


def fetch_match(
    min_weight: float,
    max_weight: float,
    statuses: list[str],
    suggested_decisions: list[str],
    current_match_id: str | None,
    skipped_match_ids: set[str],
) -> dict[str, Any] | None:
    suggestion_filter_sql = ""
    if suggested_decisions:
        suggestion_filter_sql = "and s.suggested_decision = any(%(suggested_decisions)s)"

    if current_match_id:
        row = query_one(
            f"""
            select
                q.*,
                s.suggested_decision,
                s.suggestion_confidence,
                s.label_count,
                s.total_labeled,
                s.feature_signature,
                s.generated_at as suggestion_generated_at
            from {QUEUE_TABLE} q
            left join {SUGGESTIONS_TABLE} s
                on s.match_id = q.match_id
            where q.match_id = %(match_id)s
              and q.match_weight >= %(min_weight)s
              and q.match_weight < %(max_weight)s
              and q.review_status = any(%(statuses)s)
              and not (q.match_id = any(%(skipped_match_ids)s))
              {suggestion_filter_sql}
            """,
            {
                "match_id": current_match_id,
                "min_weight": min_weight,
                "max_weight": max_weight,
                "statuses": statuses,
                "suggested_decisions": suggested_decisions,
                "skipped_match_ids": list(skipped_match_ids),
            },
        )
        if row:
            return row

    return query_one(
        f"""
        select
            q.*,
            s.suggested_decision,
            s.suggestion_confidence,
            s.label_count,
            s.total_labeled,
            s.feature_signature,
            s.generated_at as suggestion_generated_at
        from {QUEUE_TABLE} q
        left join {SUGGESTIONS_TABLE} s
            on s.match_id = q.match_id
        where q.match_weight >= %(min_weight)s
          and q.match_weight < %(max_weight)s
          and q.review_status = any(%(statuses)s)
          and not (q.match_id = any(%(skipped_match_ids)s))
          {suggestion_filter_sql}
        order by
            q.review_status = 'pending' desc,
            s.suggestion_confidence desc nulls last,
            s.total_labeled desc nulls last,
            q.match_weight desc,
            q.match_probability desc,
            q.match_id
        limit 1
        """,
        {
            "min_weight": min_weight,
            "max_weight": max_weight,
            "statuses": statuses,
            "suggested_decisions": suggested_decisions,
            "skipped_match_ids": list(skipped_match_ids),
        },
    )


def fetch_parcels_for_owner(owner_record_key: str) -> pd.DataFrame:
    return query_df(
        """
        with owner_record as (
            select
                owner_record_key,
                ownername,
                ownername2,
                owneraddr_raw,
                ownerzip_canonical,
                nullif(split_part(canonical_address_key, '|', 2), '') as ownercity_norm,
                nullif(split_part(canonical_address_key, '|', 3), '') as ownerstate_norm,
                nullif(split_part(canonical_address_key, '|', 4), '') as ownercountry_norm
            from (
                select
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
                    *
                from current.int_prcl_owners
            ) owners
            where owner_record_key = %(owner_record_key)s
        ),

        owner_lookup as (
            select
                *,
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
            from owner_record
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
                nullif(regexp_replace(trim(coalesce(p.ownername, '')), '\\s+', ' ', 'g'), '') as ownername_clean,
                nullif(regexp_replace(trim(coalesce(p.ownername2, '')), '\\s+', ' ', 'g'), '') as ownername2_clean,
                nullif(regexp_replace(trim(coalesce(p.owneraddr, '')), '\\s+', ' ', 'g'), '') as owneraddr_raw_clean,
                nullif(trim(regexp_replace(replace(replace(upper(coalesce(p.ownercity, '')), ',', ' '), '.', ' '), '\\s+', ' ', 'g')), '') as ownercity_norm,
                nullif(trim(regexp_replace(replace(replace(upper(coalesce(p.ownerstate, '')), ',', ' '), '.', ' '), '\\s+', ' ', 'g')), '') as ownerstate_norm,
                nullif(trim(regexp_replace(replace(replace(upper(coalesce(p.ownercountry, '')), ',', ' '), '.', ' '), '\\s+', ' ', 'g')), '') as ownercountry_norm,
                case
                    when length(regexp_replace(coalesce(p.ownerzip, ''), '[^0-9]+', '', 'g')) = 9
                        then left(regexp_replace(coalesce(p.ownerzip, ''), '[^0-9]+', '', 'g'), 5)
                            || '-'
                            || substring(regexp_replace(coalesce(p.ownerzip, ''), '[^0-9]+', '', 'g') from 6 for 4)
                    when length(regexp_replace(coalesce(p.ownerzip, ''), '[^0-9]+', '', 'g')) >= 5
                        then left(regexp_replace(coalesce(p.ownerzip, ''), '[^0-9]+', '', 'g'), 5)
                    else nullif(trim(regexp_replace(replace(replace(upper(coalesce(p.ownerzip, '')), ',', ' '), '.', ' '), '\\s+', ' ', 'g')), '')
                end as ownerzip_canonical
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
        )

        select
            p.parcel_handle,
            p.parcel9,
            p.parcel_address_raw,
            p.parcel_zip,
            p.ward10,
            p.nbrhd,
            p.zoning,
            p.propertyclasscode,
            p.asdland,
            p.asdimprove,
            p.asdtotal
        from parcel_source p
        join owner_lookup o
            on o.source_owner_match_key = p.source_owner_match_key
        order by p.parcel_handle
        """,
        {"owner_record_key": owner_record_key},
    )


def fetch_owner_details(owner_record_key: str) -> dict[str, Any] | None:
    return query_one(
        """
        select
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
            nullif(split_part(canonical_address_key, '|', 1), '') as canonical_address,
            nullif(split_part(canonical_address_key, '|', 2), '') as ownercity_norm,
            nullif(split_part(canonical_address_key, '|', 3), '') as ownerstate_norm,
            nullif(split_part(canonical_address_key, '|', 4), '') as ownercountry_norm,
            nullif(split_part(canonical_address_key, '|', 5), '') as canonical_zip,
            parse_status,
            parse_error
        from (
            select
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
                *
            from current.int_prcl_owners
        ) owners
        where owner_record_key = %(owner_record_key)s
        """,
        {"owner_record_key": owner_record_key},
    )


def record_review(
    match_id: str,
    decision: str,
    reviewer: str,
    notes: str,
    discrepancy_type: str,
    other_reason: str,
) -> None:
    execute(
        f"""
        insert into {REVIEWS_TABLE} (
            match_id,
            review_decision,
            reviewer,
            review_notes,
            discrepancy_type,
            other_reason
        )
        values (
            %(match_id)s,
            %(decision)s,
            %(reviewer)s,
            %(notes)s,
            %(discrepancy_type)s,
            %(other_reason)s
        )
        on conflict (match_id) do update
        set review_decision = excluded.review_decision,
            reviewer = excluded.reviewer,
            review_notes = excluded.review_notes,
            discrepancy_type = excluded.discrepancy_type,
            other_reason = excluded.other_reason,
            reviewed_at = now()
        """,
        {
            "match_id": match_id,
            "decision": decision,
            "reviewer": reviewer,
            "notes": notes or None,
            "discrepancy_type": discrepancy_type or None,
            "other_reason": other_reason or None,
        },
    )


def clear_current_match() -> None:
    st.session_state.pop("current_match_id", None)
    st.session_state["review_notes_key_version"] += 1


def current_review_notes_key() -> str:
    return f"review_notes_{st.session_state['review_notes_key_version']}"


def current_discrepancy_type_key() -> str:
    return f"discrepancy_type_{st.session_state['review_notes_key_version']}"


def current_other_reason_key() -> str:
    return f"other_reason_{st.session_state['review_notes_key_version']}"


def display_value(value: Any) -> str:
    if value is None or value == "":
        return "(blank)"

    return str(value)


def first_present(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value

    return None


def diff_html(left_value: Any, right_value: Any) -> tuple[str, str, bool]:
    left = display_value(left_value)
    right = display_value(right_value)
    matcher = SequenceMatcher(a=left, b=right, autojunk=False)
    left_parts = []
    right_parts = []

    for operation, left_start, left_end, right_start, right_end in matcher.get_opcodes():
        left_text = escape(left[left_start:left_end])
        right_text = escape(right[right_start:right_end])

        if operation == "equal":
            left_parts.append(left_text)
            right_parts.append(right_text)
        else:
            if left_text:
                left_parts.append(f'<span class="diff-left">{left_text}</span>')
            if right_text:
                right_parts.append(f'<span class="diff-right">{right_text}</span>')

    return "".join(left_parts), "".join(right_parts), left != right


def owner_display_specs(match: dict[str, Any]) -> list[tuple[str, Any, Any]]:
    return [
        ("Owner name", match.get("ownername_l"), match.get("ownername_r")),
        ("Owner name 2", match.get("ownername2_l"), match.get("ownername2_r")),
        (
            "Owner address",
            first_present(match.get("owneraddr_cleaned_l"), match.get("owneraddr_raw_l")),
            first_present(match.get("owneraddr_cleaned_r"), match.get("owneraddr_raw_r")),
        ),
        ("Owner ZIP", match.get("ownerzip_canonical_l"), match.get("ownerzip_canonical_r")),
    ]


def parsed_address_specs(match: dict[str, Any]) -> list[tuple[str, Any, Any]]:
    return [
        ("House number", match.get("house_number_l"), match.get("house_number_r")),
        ("Predir", match.get("predir_l"), match.get("predir_r")),
        ("Street name", match.get("street_name_l"), match.get("street_name_r")),
        ("Street suffix", match.get("street_suffix_l"), match.get("street_suffix_r")),
        ("Postdir", match.get("postdir_l"), match.get("postdir_r")),
        ("Unit type", match.get("unit_type_l"), match.get("unit_type_r")),
        ("Unit number", match.get("unit_number_l"), match.get("unit_number_r")),
        ("PO box", match.get("po_box_l"), match.get("po_box_r")),
    ]


def source_owner_specs(
    details_l: dict[str, Any] | None,
    details_r: dict[str, Any] | None,
) -> list[tuple[str, Any, Any]]:
    details_l = details_l or {}
    details_r = details_r or {}

    return [
        ("Raw owner address", details_l.get("owneraddr_raw"), details_r.get("owneraddr_raw")),
        (
            "Cleaned owner address",
            details_l.get("owneraddr_cleaned"),
            details_r.get("owneraddr_cleaned"),
        ),
        ("Canonical address", details_l.get("canonical_address"), details_r.get("canonical_address")),
        ("Owner city", details_l.get("ownercity_norm"), details_r.get("ownercity_norm")),
        ("Owner state", details_l.get("ownerstate_norm"), details_r.get("ownerstate_norm")),
        ("Owner country", details_l.get("ownercountry_norm"), details_r.get("ownercountry_norm")),
        ("Canonical ZIP", details_l.get("canonical_zip"), details_r.get("canonical_zip")),
        (
            "Embedded ZIP from owner address",
            details_l.get("embedded_zip_from_owneraddr"),
            details_r.get("embedded_zip_from_owneraddr"),
        ),
        (
            "Canonical address key",
            details_l.get("canonical_address_key"),
            details_r.get("canonical_address_key"),
        ),
        ("Parse status", details_l.get("parse_status"), details_r.get("parse_status")),
        ("Parse error", details_l.get("parse_error"), details_r.get("parse_error")),
    ]


def build_diff_cache(
    specs: list[tuple[str, Any, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        label: {
            "left": left_html,
            "right": right_html,
            "changed": changed,
        }
        for label, left_html, right_html, changed in (
            (label, *diff_html(left_value, right_value))
            for label, left_value, right_value in specs
        )
    }


def render_diff_line(label: str, diff_cache: dict[str, dict[str, Any]], side: str) -> None:
    if label not in diff_cache:
        return

    value_html = diff_cache[label][side]
    st.markdown(
        f"""
        <div class="owner-field">
            <span class="owner-field-label">{escape(label)}:</span>
            <span class="diff-value">{value_html}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def gamma_meaning(signal: str, value: Any) -> str:
    try:
        gamma = int(value)
    except (TypeError, ValueError):
        return "not returned"

    if gamma == -1:
        return "missing or null on one or both sides"

    if signal in EXACT_GAMMA_SIGNALS:
        if gamma == 1:
            return "exact match"
        if gamma == 0:
            return "different"
        return "unexpected exact-match bucket"

    thresholds = FUZZY_GAMMA_THRESHOLDS.get(signal)
    if not thresholds:
        return "raw Splink comparison bucket"

    if gamma == len(thresholds) + 1:
        return "exact match"

    if gamma == 0:
        return f"below {thresholds[-1]:.2f} similarity"

    threshold_index = len(thresholds) - gamma
    if 0 <= threshold_index < len(thresholds):
        return f"Jaro-Winkler similarity >= {thresholds[threshold_index]:.2f}"

    return "unexpected fuzzy-match bucket"


def decision_button(label: str, shortcut: str, decision: str, match: dict[str, Any], primary=False):
    if shortcut_button(
        label,
        shortcut,
        key=f"{decision}_{match['match_id']}",
        type="primary" if primary else "secondary",
        use_container_width=True,
        hint=False,
    ):
        record_review(
            match_id=match["match_id"],
            decision=decision,
            reviewer=st.session_state["reviewer"],
            notes=st.session_state.get(current_review_notes_key(), ""),
            discrepancy_type=st.session_state.get(current_discrepancy_type_key(), ""),
            other_reason=st.session_state.get(current_other_reason_key(), ""),
        )
        clear_current_match()
        st.rerun()


def skip_button(match: dict[str, Any]) -> None:
    if shortcut_button(
        "Skip",
        "s",
        key=f"skip_{match['match_id']}",
        use_container_width=True,
        hint=False,
    ):
        st.session_state["skipped_match_ids"].add(match["match_id"])
        clear_current_match()
        st.rerun()


def owner_panel(
    side: str,
    match: dict[str, Any],
    parcels: pd.DataFrame,
    owner_diff_cache: dict[str, dict[str, Any]],
    parsed_diff_cache: dict[str, dict[str, Any]],
    source_diff_cache: dict[str, dict[str, Any]],
) -> None:
    side_label = "Left" if side == "left" else "Right"
    record_key_field = "owner_record_key_l" if side == "left" else "owner_record_key_r"

    st.subheader(side_label)
    for label in ["Owner name", "Owner name 2", "Owner address", "Owner ZIP"]:
        render_diff_line(label, owner_diff_cache, side)

    st.caption(f"Record key: `{match.get(record_key_field)}`")

    with st.expander("Parsed address pieces", expanded=False):
        for label in [
            "House number",
            "Predir",
            "Street name",
            "Street suffix",
            "Postdir",
            "Unit type",
            "Unit number",
            "PO box",
        ]:
            render_diff_line(label, parsed_diff_cache, side)

    with st.expander("Source owner fields", expanded=False):
        for label in [
            "Raw owner address",
            "Cleaned owner address",
            "Canonical address",
            "Owner city",
            "Owner state",
            "Owner country",
            "Canonical ZIP",
            "Embedded ZIP from owner address",
            "Canonical address key",
            "Parse status",
            "Parse error",
        ]:
            render_diff_line(label, source_diff_cache, side)

    st.markdown("Parcels")
    if parcels.empty:
        st.info("No parcels found for this owner record.")
    else:
        st.dataframe(
            parcels,
            hide_index=True,
            use_container_width=True,
            height=min(260, 42 + 35 * len(parcels)),
        )


def render_match(match: dict[str, Any]) -> None:
    parcels_l = fetch_parcels_for_owner(match["owner_record_key_l"])
    parcels_r = fetch_parcels_for_owner(match["owner_record_key_r"])
    owner_details_l = fetch_owner_details(match["owner_record_key_l"])
    owner_details_r = fetch_owner_details(match["owner_record_key_r"])
    owner_specs = owner_display_specs(match)
    owner_diff_cache = build_diff_cache(owner_specs)
    parsed_diff_cache = build_diff_cache(parsed_address_specs(match))
    source_diff_cache = build_diff_cache(source_owner_specs(owner_details_l, owner_details_r))

    metric_cols = st.columns(5)
    metric_cols[0].metric("Match Weight", f"{match['match_weight']:.2f}")
    metric_cols[1].metric("Probability", f"{match['match_probability']:.6f}")
    metric_cols[2].metric("Review Status", match["review_status"])
    metric_cols[3].metric("Left Parcels", len(parcels_l))
    metric_cols[4].metric("Right Parcels", len(parcels_r))

    if match.get("suggested_decision"):
        st.info(
            "Suggestion: "
            f"{match['suggested_decision']} "
            f"({float(match['suggestion_confidence']):.0%} confidence from "
            f"{int(match['label_count'])}/{int(match['total_labeled'])} labeled matches "
            "with the same feature signature)."
        )

    st.divider()

    if not any(details["changed"] for details in owner_diff_cache.values()):
        st.caption("Displayed owner fields are text-identical on this candidate.")

    left, right = st.columns(2, gap="large")
    with left:
        owner_panel(
            "left",
            match,
            parcels_l,
            owner_diff_cache,
            parsed_diff_cache,
            source_diff_cache,
        )
    with right:
        owner_panel(
            "right",
            match,
            parcels_r,
            owner_diff_cache,
            parsed_diff_cache,
            source_diff_cache,
        )

    st.divider()
    st.markdown("Comparison Signals")
    with st.expander("How to read comparison signals", expanded=False):
        st.markdown(
            """
            Gamma values are Splink's field-level comparison buckets. Higher values mean
            stronger agreement for that field, and `-1` means one or both sides were
            missing. Exact-match fields use `1` for exact, `0` for different, and `-1`
            for missing. Fuzzy text fields use Jaro-Winkler similarity buckets, with an
            extra top bucket for exact matches. Splink converts these buckets into the
            overall match weight and probability; the gamma values are not simply added
            together.
            """
        )
    gamma_cols = [
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
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "field": GAMMA_LABELS[column],
                    "signal": column,
                    "value": match.get(column),
                    "meaning": gamma_meaning(column, match.get(column)),
                }
                for column in gamma_cols
            ]
        ),
        hide_index=True,
        use_container_width=True,
    )


def main() -> None:
    st.title("Owner Match Review")

    ensure_suggestions_table()
    st.session_state.setdefault("skipped_match_ids", set())
    st.session_state.setdefault("reviewer", os.getenv("USERNAME") or os.getenv("USER") or "reviewer")
    st.session_state.setdefault("review_notes_key_version", 0)

    with st.sidebar:
        st.header("Review")
        st.text_input("Reviewer", key="reviewer")
        min_weight = st.number_input("Minimum weight", value=30.0, step=1.0)
        max_weight = st.number_input("Maximum weight", value=42.0, step=1.0)
        statuses = st.multiselect(
            "Statuses",
            ["pending", "accepted", "rejected", "needs_more_info"],
            default=["pending"],
        )
        suggested_decisions = st.multiselect(
            "Suggested decision",
            ["accepted", "rejected", "needs_more_info"],
            default=[],
        )
        st.selectbox(
            "Discrepancy type",
            DISCREPANCY_TYPE_OPTIONS,
            format_func=format_discrepancy_type,
            key=current_discrepancy_type_key(),
        )
        st.text_area("Other reason", key=current_other_reason_key(), height=80)
        st.text_area("Notes for next decision", key=current_review_notes_key(), height=100)

        if st.button("Apply Accepted Groups", use_container_width=True):
            with st.spinner("Applying accepted owner groups..."):
                inferred_count = apply_owner_review_groups()
            st.success(f"Accepted {inferred_count:,} already-connected candidate pairs.")

        if st.button("Refresh Suggestions", use_container_width=True):
            with st.spinner("Refreshing suggestions..."):
                suggestion_count = refresh_match_suggestions()
            st.success(f"Created {suggestion_count:,} pending suggestions.")

        counts = queue_counts(min_weight=min_weight, max_weight=max_weight)
        suggestions = suggestion_counts()
        st.metric("Pending", counts["pending"])
        st.metric("Accepted", counts["accepted"])
        st.metric("Rejected", counts["rejected"])
        st.metric("Needs Info", counts["needs_more_info"])
        st.metric("Suggestions", suggestions["total"])
        st.caption(
            "Suggested: "
            f"{suggestions['accepted']:,} accepted, "
            f"{suggestions['rejected']:,} rejected, "
            f"{suggestions['needs_more_info']:,} needs info"
        )

    if not statuses:
        st.warning("Select at least one status.")
        return

    match = fetch_match(
        min_weight=min_weight,
        max_weight=max_weight,
        statuses=statuses,
        suggested_decisions=suggested_decisions,
        current_match_id=st.session_state.get("current_match_id"),
        skipped_match_ids=st.session_state["skipped_match_ids"],
    )

    if not match:
        st.success("No matches found for the selected filters.")
        return

    st.session_state["current_match_id"] = match["match_id"]

    action_cols = st.columns(4)
    with action_cols[0]:
        decision_button("Accept", "a", "accepted", match, primary=True)
    with action_cols[1]:
        decision_button("Reject", "r", "rejected", match)
    with action_cols[2]:
        decision_button("Needs More Info", "m", "needs_more_info", match)
    with action_cols[3]:
        skip_button(match)

    render_match(match)


if __name__ == "__main__":
    main()
