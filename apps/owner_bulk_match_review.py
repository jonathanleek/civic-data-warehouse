import os
from contextlib import contextmanager
from typing import Any

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st


QUEUE_TABLE = "current.prcl_owner_match_review_queue"
REVIEWS_TABLE = "current.prcl_owner_match_reviews"
SUGGESTIONS_TABLE = "current.prcl_owner_match_suggestions"
OWNERS_TABLE = "current.int_prcl_owners"

DECISION_LABEL_TO_VALUE = {
    "": "",
    "Accept": "accepted",
    "Reject": "rejected",
    "More Info": "needs_more_info",
}
DECISION_OPTIONS = list(DECISION_LABEL_TO_VALUE)

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
DISCREPANCY_LABEL_TO_VALUE = {
    label: value for value, label in DISCREPANCY_TYPE_LABELS.items()
}
DISCREPANCY_LABEL_OPTIONS = [
    DISCREPANCY_TYPE_LABELS[value] for value in DISCREPANCY_TYPE_OPTIONS
]


st.set_page_config(
    page_title="Owner Bulk Match Review",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .owner-line {
        line-height: 1.45;
        margin: 0 0 0.35rem;
    }

    .owner-label {
        font-weight: 700;
    }

    .small-mono {
        color: #6b7280;
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
        font-size: 0.78rem;
        overflow-wrap: anywhere;
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


def query_df(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    with db_connection() as conn:
        return pd.read_sql_query(sql, conn, params=params or {})


def execute(sql: str, params: dict[str, Any] | None = None) -> None:
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params or {})
        conn.commit()


def ensure_suggestions_table() -> None:
    execute(
        f"""
        create table if not exists {SUGGESTIONS_TABLE} (
            match_id text primary key,
            suggested_decision text,
            suggestion_confidence numeric,
            label_count integer,
            total_labeled integer,
            feature_signature text,
            generated_at timestamptz not null default now()
        )
        """
    )


def suggestion_filter_sql(suggested_decisions: list[str]) -> str:
    if not suggested_decisions:
        return ""

    return "and s.suggested_decision = any(%(suggested_decisions)s)"


def fetch_anchor_choices(
    min_weight: float,
    max_weight: float,
    statuses: list[str],
    suggested_decisions: list[str],
    owner_search: str,
    limit: int,
) -> pd.DataFrame:
    search_text = owner_search.strip()
    return query_df(
        f"""
        with pairs as (
            select
                q.*,
                s.suggested_decision,
                s.suggestion_confidence
            from {QUEUE_TABLE} q
            left join {SUGGESTIONS_TABLE} s
                on s.match_id = q.match_id
            where q.match_weight >= %(min_weight)s
              and q.match_weight < %(max_weight)s
              and q.review_status = any(%(statuses)s)
              {suggestion_filter_sql(suggested_decisions)}
        ),

        anchor_rows as (
            select
                owner_record_key_l as owner_record_key,
                ownername_l as ownername,
                ownername2_l as ownername2,
                coalesce(owneraddr_cleaned_l, owneraddr_raw_l) as owneraddr,
                ownerzip_canonical_l as ownerzip,
                match_weight,
                suggested_decision
            from pairs

            union all

            select
                owner_record_key_r as owner_record_key,
                ownername_r as ownername,
                ownername2_r as ownername2,
                coalesce(owneraddr_cleaned_r, owneraddr_raw_r) as owneraddr,
                ownerzip_canonical_r as ownerzip,
                match_weight,
                suggested_decision
            from pairs
        ),

        grouped as (
            select
                owner_record_key,
                max(ownername) as ownername,
                max(ownername2) as ownername2,
                max(owneraddr) as owneraddr,
                max(ownerzip) as ownerzip,
                count(*)::int as candidate_count,
                max(match_weight) as top_weight,
                count(*) filter (where suggested_decision is not null)::int as suggested_count
            from anchor_rows
            where owner_record_key is not null
            group by owner_record_key
        )

        select *
        from grouped
        where (
            %(owner_search)s = ''
            or concat_ws(
                ' ',
                owner_record_key,
                ownername,
                ownername2,
                owneraddr,
                ownerzip
            ) ilike %(owner_search_like)s
        )
        order by candidate_count desc, top_weight desc, ownername nulls last
        limit %(limit)s
        """,
        {
            "min_weight": min_weight,
            "max_weight": max_weight,
            "statuses": statuses,
            "suggested_decisions": suggested_decisions,
            "owner_search": search_text,
            "owner_search_like": f"%{search_text}%",
            "limit": limit,
        },
    )


def fetch_candidate_pairs(
    anchor_key: str,
    min_weight: float,
    max_weight: float,
    statuses: list[str],
    suggested_decisions: list[str],
    batch_size: int,
) -> pd.DataFrame:
    return query_df(
        f"""
        select
            q.*,
            s.suggested_decision,
            s.suggestion_confidence,
            s.label_count,
            s.total_labeled
        from {QUEUE_TABLE} q
        left join {SUGGESTIONS_TABLE} s
            on s.match_id = q.match_id
        where q.match_weight >= %(min_weight)s
          and q.match_weight < %(max_weight)s
          and q.review_status = any(%(statuses)s)
          and (
              q.owner_record_key_l = %(anchor_key)s
              or q.owner_record_key_r = %(anchor_key)s
          )
          {suggestion_filter_sql(suggested_decisions)}
        order by
            q.review_status = 'pending' desc,
            s.suggestion_confidence desc nulls last,
            s.total_labeled desc nulls last,
            q.match_weight desc,
            q.match_probability desc,
            q.match_id
        limit %(batch_size)s
        """,
        {
            "anchor_key": anchor_key,
            "min_weight": min_weight,
            "max_weight": max_weight,
            "statuses": statuses,
            "suggested_decisions": suggested_decisions,
            "batch_size": batch_size,
        },
    )


def fetch_owner_details(owner_keys: list[str]) -> pd.DataFrame:
    clean_keys = sorted({key for key in owner_keys if key})
    if not clean_keys:
        return pd.DataFrame()

    return query_df(
        f"""
        with owner_records as (
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
                nullif(split_part(canonical_address_key, '|', 2), '') as ownercity,
                nullif(split_part(canonical_address_key, '|', 3), '') as ownerstate,
                nullif(split_part(canonical_address_key, '|', 4), '') as ownercountry,
                parse_status,
                parse_error
            from {OWNERS_TABLE}
        )

        select *
        from owner_records
        where owner_record_key = any(%(owner_keys)s)
        """,
        {"owner_keys": clean_keys},
    )


def normalize_text(value: Any) -> str:
    if pd.isna(value) or value is None:
        return ""

    return " ".join(str(value).strip().upper().split())


def display_value(value: Any) -> str:
    if pd.isna(value) or value is None or value == "":
        return ""

    return str(value)


def first_present(*values: Any) -> Any:
    for value in values:
        if not pd.isna(value) and value is not None and value != "":
            return value

    return ""


def short_key(value: Any) -> str:
    text = display_value(value)
    return text[:8] if text else ""


def format_suggestion(row: pd.Series) -> str:
    decision = display_value(row.get("suggested_decision"))
    if not decision:
        return ""

    confidence = row.get("suggestion_confidence")
    if pd.isna(confidence) or confidence is None:
        return decision

    return f"{decision} ({float(confidence):.0%})"


def owner_detail_map(details: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if details.empty:
        return {}

    return {
        str(row["owner_record_key"]): row.to_dict()
        for _, row in details.iterrows()
    }


def counterpart_suffix(row: pd.Series, anchor_key: str) -> tuple[str, str]:
    if row["owner_record_key_l"] == anchor_key:
        return "l", "r"

    return "r", "l"


def value_for_side(row: pd.Series, field: str, suffix: str) -> Any:
    return row.get(f"{field}_{suffix}")


def changed_fields(
    row: pd.Series,
    left_suffix: str,
    right_suffix: str,
    left_details: dict[str, Any],
    right_details: dict[str, Any],
) -> str:
    field_pairs = [
        (
            "name",
            value_for_side(row, "ownername", left_suffix),
            value_for_side(row, "ownername", right_suffix),
        ),
        (
            "name2",
            value_for_side(row, "ownername2", left_suffix),
            value_for_side(row, "ownername2", right_suffix),
        ),
        (
            "address",
            first_present(
                value_for_side(row, "owneraddr_cleaned", left_suffix),
                value_for_side(row, "owneraddr_raw", left_suffix),
            ),
            first_present(
                value_for_side(row, "owneraddr_cleaned", right_suffix),
                value_for_side(row, "owneraddr_raw", right_suffix),
            ),
        ),
        ("city", left_details.get("ownercity"), right_details.get("ownercity")),
        ("state", left_details.get("ownerstate"), right_details.get("ownerstate")),
        ("country", left_details.get("ownercountry"), right_details.get("ownercountry")),
        (
            "zip",
            value_for_side(row, "ownerzip_canonical", left_suffix),
            value_for_side(row, "ownerzip_canonical", right_suffix),
        ),
        (
            "unit",
            value_for_side(row, "unit_number", left_suffix),
            value_for_side(row, "unit_number", right_suffix),
        ),
    ]

    changed = [
        label
        for label, left_value, right_value in field_pairs
        if normalize_text(left_value) != normalize_text(right_value)
    ]
    return ", ".join(changed) if changed else "text-identical"


def build_editor_rows(
    pairs: pd.DataFrame,
    anchor_key: str,
    details_by_key: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    left_details = details_by_key.get(anchor_key, {})

    for _, pair in pairs.iterrows():
        left_suffix, right_suffix = counterpart_suffix(pair, anchor_key)
        right_key = value_for_side(pair, "owner_record_key", right_suffix)
        right_details = details_by_key.get(str(right_key), {})
        right_address = first_present(
            value_for_side(pair, "owneraddr_cleaned", right_suffix),
            value_for_side(pair, "owneraddr_raw", right_suffix),
        )
        rows.append(
            {
                "decision": "",
                "discrepancy_type": DISCREPANCY_TYPE_LABELS[""],
                "other_reason": "",
                "review_notes": "",
                "match_weight": round(float(pair["match_weight"]), 2),
                "probability": round(float(pair["match_probability"]), 6),
                "suggestion": format_suggestion(pair),
                "status": pair["review_status"],
                "right_ownername": display_value(
                    value_for_side(pair, "ownername", right_suffix)
                ),
                "right_ownername2": display_value(
                    value_for_side(pair, "ownername2", right_suffix)
                ),
                "right_owneraddr": display_value(right_address),
                "right_city": display_value(right_details.get("ownercity")),
                "right_state": display_value(right_details.get("ownerstate")),
                "right_country": display_value(right_details.get("ownercountry")),
                "right_zip": display_value(
                    value_for_side(pair, "ownerzip_canonical", right_suffix)
                ),
                "differences": changed_fields(
                    pair,
                    left_suffix,
                    right_suffix,
                    left_details,
                    right_details,
                ),
                "match_id": pair["match_id"],
                "right_record": short_key(right_key),
            }
        )

    return pd.DataFrame(rows)


def review_rows_from_editor(
    edited: pd.DataFrame,
    reviewer: str,
) -> list[dict[str, Any]]:
    review_rows: list[dict[str, Any]] = []
    for _, row in edited.iterrows():
        decision = DECISION_LABEL_TO_VALUE.get(str(row.get("decision", "")), "")
        if not decision:
            continue

        discrepancy_label = str(row.get("discrepancy_type", DISCREPANCY_TYPE_LABELS[""]))
        review_rows.append(
            {
                "match_id": row["match_id"],
                "decision": decision,
                "reviewer": reviewer.strip() or "reviewer",
                "review_notes": display_value(row.get("review_notes")) or None,
                "discrepancy_type": DISCREPANCY_LABEL_TO_VALUE.get(discrepancy_label)
                or None,
                "other_reason": display_value(row.get("other_reason")) or None,
            }
        )

    return review_rows


def save_reviews(review_rows: list[dict[str, Any]]) -> None:
    with db_connection() as conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_batch(
                cursor,
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
                    %(review_notes)s,
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
                review_rows,
            )
        conn.commit()


def anchor_label(row: pd.Series) -> str:
    ownername = display_value(row.get("ownername")) or "(blank owner)"
    owneraddr = display_value(row.get("owneraddr"))
    ownerzip = display_value(row.get("ownerzip"))
    return (
        f"{int(row['candidate_count'])} matches | "
        f"{float(row['top_weight']):.2f} | "
        f"{short_key(row['owner_record_key'])} | "
        f"{ownername} | {owneraddr} | {ownerzip}"
    )


def render_owner_line(label: str, value: Any) -> None:
    text = display_value(value) or "(blank)"
    st.markdown(
        f"""
        <div class="owner-line">
            <span class="owner-label">{label}:</span> {text}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_anchor_panel(
    anchor: pd.Series,
    anchor_details: dict[str, Any],
    selected_count: int,
) -> None:
    metric_cols = st.columns(4)
    metric_cols[0].metric("Candidate Rows", f"{selected_count:,}")
    metric_cols[1].metric("Owner Total", f"{int(anchor['candidate_count']):,}")
    metric_cols[2].metric("Top Weight", f"{float(anchor['top_weight']):.2f}")
    metric_cols[3].metric("Suggested Rows", f"{int(anchor['suggested_count']):,}")

    st.subheader("Left Owner")
    render_owner_line("Owner name", anchor_details.get("ownername") or anchor.get("ownername"))
    render_owner_line("Owner name 2", anchor_details.get("ownername2") or anchor.get("ownername2"))
    render_owner_line(
        "Owner address",
        first_present(
            anchor_details.get("owneraddr_cleaned"),
            anchor_details.get("owneraddr_raw"),
            anchor.get("owneraddr"),
        ),
    )
    render_owner_line("Owner city", anchor_details.get("ownercity"))
    render_owner_line("Owner state", anchor_details.get("ownerstate"))
    render_owner_line("Owner country", anchor_details.get("ownercountry"))
    render_owner_line(
        "Owner ZIP",
        anchor_details.get("ownerzip_canonical") or anchor.get("ownerzip"),
    )
    st.markdown(
        f'<div class="small-mono">Record key: {anchor["owner_record_key"]}</div>',
        unsafe_allow_html=True,
    )

    with st.expander("Parsed address pieces", expanded=False):
        parsed_cols = st.columns(4)
        parsed_cols[0].write(f"House number: {display_value(anchor_details.get('house_number')) or '(blank)'}")
        parsed_cols[1].write(f"Predir: {display_value(anchor_details.get('predir')) or '(blank)'}")
        parsed_cols[2].write(f"Street: {display_value(anchor_details.get('street_name')) or '(blank)'}")
        parsed_cols[3].write(f"Suffix: {display_value(anchor_details.get('street_suffix')) or '(blank)'}")
        parsed_cols = st.columns(4)
        parsed_cols[0].write(f"Postdir: {display_value(anchor_details.get('postdir')) or '(blank)'}")
        parsed_cols[1].write(f"Unit type: {display_value(anchor_details.get('unit_type')) or '(blank)'}")
        parsed_cols[2].write(f"Unit number: {display_value(anchor_details.get('unit_number')) or '(blank)'}")
        parsed_cols[3].write(f"PO box: {display_value(anchor_details.get('po_box')) or '(blank)'}")


def main() -> None:
    st.title("Owner Bulk Match Review")
    ensure_suggestions_table()

    st.session_state.setdefault(
        "reviewer",
        os.getenv("USERNAME") or os.getenv("USER") or "reviewer",
    )

    if st.session_state.get("bulk_submit_message"):
        st.success(st.session_state.pop("bulk_submit_message"))

    with st.sidebar:
        st.header("Batch")
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
        owner_search = st.text_input("Find left owner")
        batch_size = st.number_input("Rows for selected owner", value=25, min_value=5, max_value=100, step=5)
        anchor_limit = st.number_input("Left owner choices", value=250, min_value=25, max_value=1000, step=25)

    if not statuses:
        st.warning("Choose at least one status.")
        return

    anchors = fetch_anchor_choices(
        min_weight=min_weight,
        max_weight=max_weight,
        statuses=statuses,
        suggested_decisions=suggested_decisions,
        owner_search=owner_search,
        limit=int(anchor_limit),
    )

    if anchors.empty:
        st.info("No owner records match these filters.")
        return

    label_by_key = {
        row["owner_record_key"]: anchor_label(row)
        for _, row in anchors.iterrows()
    }
    anchor_key = st.selectbox(
        "Left owner",
        anchors["owner_record_key"].tolist(),
        format_func=lambda key: label_by_key.get(key, key),
    )
    anchor = anchors[anchors["owner_record_key"] == anchor_key].iloc[0]

    pairs = fetch_candidate_pairs(
        anchor_key=anchor_key,
        min_weight=min_weight,
        max_weight=max_weight,
        statuses=statuses,
        suggested_decisions=suggested_decisions,
        batch_size=int(batch_size),
    )

    if pairs.empty:
        st.info("This owner has no candidate rows under the current filters.")
        return

    right_keys = []
    for _, pair in pairs.iterrows():
        _, right_suffix = counterpart_suffix(pair, anchor_key)
        right_keys.append(value_for_side(pair, "owner_record_key", right_suffix))
    details = fetch_owner_details([anchor_key, *right_keys])
    details_by_key = owner_detail_map(details)
    editor_rows = build_editor_rows(pairs, anchor_key, details_by_key)

    left_col, right_col = st.columns([0.9, 1.8], gap="large")
    with left_col:
        render_anchor_panel(anchor, details_by_key.get(anchor_key, {}), len(editor_rows))

    with right_col:
        st.subheader("Possible Matches")
        st.caption("Selections are staged here. Nothing is saved until you click Submit Ratings.")

        with st.form("bulk_review_form"):
            edited = st.data_editor(
                editor_rows,
                hide_index=True,
                use_container_width=True,
                height=min(780, 42 + 36 * len(editor_rows)),
                column_order=[
                    "decision",
                    "discrepancy_type",
                    "other_reason",
                    "review_notes",
                    "match_weight",
                    "probability",
                    "suggestion",
                    "right_ownername",
                    "right_ownername2",
                    "right_owneraddr",
                    "right_city",
                    "right_state",
                    "right_country",
                    "right_zip",
                    "differences",
                    "status",
                    "right_record",
                    "match_id",
                ],
                disabled=[
                    column
                    for column in editor_rows.columns
                    if column
                    not in {
                        "decision",
                        "discrepancy_type",
                        "other_reason",
                        "review_notes",
                    }
                ],
                column_config={
                    "decision": st.column_config.SelectboxColumn(
                        "Decision",
                        options=DECISION_OPTIONS,
                        width="small",
                    ),
                    "discrepancy_type": st.column_config.SelectboxColumn(
                        "Discrepancy",
                        options=DISCREPANCY_LABEL_OPTIONS,
                        width="medium",
                    ),
                    "other_reason": st.column_config.TextColumn("Other reason", width="medium"),
                    "review_notes": st.column_config.TextColumn("Notes", width="medium"),
                    "match_weight": st.column_config.NumberColumn("Weight", format="%.2f", width="small"),
                    "probability": st.column_config.NumberColumn("Probability", format="%.6f", width="small"),
                    "right_ownername": st.column_config.TextColumn("Right owner", width="large"),
                    "right_ownername2": st.column_config.TextColumn("Right owner 2", width="medium"),
                    "right_owneraddr": st.column_config.TextColumn("Right address", width="large"),
                    "right_city": st.column_config.TextColumn("City", width="small"),
                    "right_state": st.column_config.TextColumn("State", width="small"),
                    "right_country": st.column_config.TextColumn("Country", width="small"),
                    "right_zip": st.column_config.TextColumn("ZIP", width="small"),
                    "differences": st.column_config.TextColumn("Changed fields", width="medium"),
                    "right_record": st.column_config.TextColumn("Right key", width="small"),
                    "match_id": st.column_config.TextColumn("Match id", width="medium"),
                },
                key=f"bulk_editor_{anchor_key}_{min_weight}_{max_weight}_{','.join(statuses)}_{','.join(suggested_decisions)}",
            )

            submitted = st.form_submit_button("Submit Ratings", type="primary", use_container_width=True)

        if submitted:
            review_rows = review_rows_from_editor(edited, st.session_state["reviewer"])
            if not review_rows:
                st.warning("No decisions selected, so nothing was saved.")
            else:
                save_reviews(review_rows)
                st.session_state["bulk_submit_message"] = (
                    f"Saved {len(review_rows):,} ratings for {display_value(anchor.get('ownername')) or 'selected owner'}."
                )
                st.rerun()


if __name__ == "__main__":
    main()
