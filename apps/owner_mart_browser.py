import os
from collections import Counter, defaultdict
from contextlib import contextmanager
from typing import Any

import pandas as pd
import psycopg2
import streamlit as st


st.set_page_config(
    page_title="Owner Mart Browser",
    layout="wide",
    initial_sidebar_state="expanded",
)


MART_TABLE = "current.parcel_owner_mart"
OWNERS_TABLE = "current.int_prcl_owners"


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


@st.cache_data(show_spinner="Loading owner mart edges...")
def fetch_mart_edges() -> pd.DataFrame:
    return query_df(
        f"""
        select
            match_id,
            match_method,
            review_status,
            reviewer,
            reviewed_at,
            discrepancy_type,
            match_probability,
            match_weight,
            owner_record_key_l,
            owner_record_key_r,
            ownername_l,
            ownername_r,
            ownername2_l,
            ownername2_r,
            owneraddr_raw_l,
            owneraddr_raw_r,
            owneraddr_cleaned_l,
            owneraddr_cleaned_r,
            ownerzip_canonical_l,
            ownerzip_canonical_r,
            parcel_count_l,
            parcel_count_r,
            total_parcel_count,
            parcel_handles_l,
            parcel_handles_r
        from {MART_TABLE}
        where owner_record_key_l is not null
          and owner_record_key_r is not null
        """
    )


@st.cache_data(show_spinner="Loading owner records...")
def fetch_owner_records() -> pd.DataFrame:
    return query_df(
        f"""
        with matched_owner_keys as (
            select owner_record_key_l as owner_record_key
            from {MART_TABLE}
            where owner_record_key_l is not null

            union

            select owner_record_key_r as owner_record_key
            from {MART_TABLE}
            where owner_record_key_r is not null
        ),

        owner_records as (
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
                ownerzip_canonical,
                canonical_address_key,
                nullif(split_part(canonical_address_key, '|', 2), '') as ownercity,
                nullif(split_part(canonical_address_key, '|', 3), '') as ownerstate,
                nullif(split_part(canonical_address_key, '|', 4), '') as ownercountry,
                parse_status,
                parse_error
            from {OWNERS_TABLE}
        )

        select o.*
        from owner_records o
        join matched_owner_keys k
            on k.owner_record_key = o.owner_record_key
        """
    )


def build_owner_groups(edges: pd.DataFrame) -> dict[str, str]:
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

    for row in edges.itertuples(index=False):
        union(str(row.owner_record_key_l), str(row.owner_record_key_r))

    return {owner_record_key: find(owner_record_key) for owner_record_key in parent}


def most_common(values: pd.Series) -> str:
    clean_values = [str(value) for value in values.dropna() if str(value).strip()]
    if not clean_values:
        return ""

    return Counter(clean_values).most_common(1)[0][0]


def parcel_handles_from_edges(edges: pd.DataFrame) -> dict[str, set[str]]:
    parcel_handles: dict[str, set[str]] = defaultdict(set)

    for row in edges.itertuples(index=False):
        group_key = row.owner_group_key
        for field in ["parcel_handles_l", "parcel_handles_r"]:
            handles = getattr(row, field, None)
            if handles is None:
                continue

            for handle in handles:
                if handle:
                    parcel_handles[group_key].add(str(handle))

    return parcel_handles


@st.cache_data(show_spinner="Building owner groups...")
def load_grouped_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    edges = fetch_mart_edges()
    owners = fetch_owner_records()

    if edges.empty or owners.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    owner_to_group = build_owner_groups(edges)
    edges["owner_group_key"] = edges["owner_record_key_l"].map(owner_to_group)
    owners["owner_group_key"] = owners["owner_record_key"].map(owner_to_group)
    owners = owners[owners["owner_group_key"].notna()].copy()

    parcel_handles = parcel_handles_from_edges(edges)
    edge_counts = edges.groupby("owner_group_key").size()
    match_methods = (
        edges.groupby("owner_group_key")["match_method"]
        .apply(lambda values: ", ".join(sorted(set(values.dropna()))))
        .rename("match_methods")
    )
    group_summary = (
        owners.groupby("owner_group_key")
        .agg(
            owner_records=("owner_record_key", "nunique"),
            ownername_variants=("ownername", "nunique"),
            ownername2_variants=("ownername2", "nunique"),
            address_variants=("owneraddr_raw", "nunique"),
            cleaned_address_variants=("owneraddr_cleaned", "nunique"),
            zip_variants=("ownerzip_canonical", "nunique"),
        )
        .reset_index()
    )
    group_summary["display_ownername"] = group_summary["owner_group_key"].map(
        owners.groupby("owner_group_key")["ownername"].apply(most_common)
    )
    group_summary["display_ownername2"] = group_summary["owner_group_key"].map(
        owners.groupby("owner_group_key")["ownername2"].apply(most_common)
    )
    group_summary["display_address"] = group_summary["owner_group_key"].map(
        owners.groupby("owner_group_key")["owneraddr_cleaned"].apply(most_common)
    )
    group_summary["display_zip"] = group_summary["owner_group_key"].map(
        owners.groupby("owner_group_key")["ownerzip_canonical"].apply(most_common)
    )
    group_summary["matched_edges"] = group_summary["owner_group_key"].map(edge_counts).fillna(0).astype(int)
    group_summary["parcel_count"] = group_summary["owner_group_key"].map(
        lambda group_key: len(parcel_handles.get(group_key, set()))
    )
    group_summary["match_methods"] = group_summary["owner_group_key"].map(match_methods).fillna("")
    group_summary = group_summary.sort_values(
        by=["owner_records", "matched_edges", "display_ownername"],
        ascending=[False, False, True],
    )

    return group_summary, owners, edges


def variant_counts(
    owners: pd.DataFrame,
    group_key: str,
    column: str,
    label: str,
) -> pd.DataFrame:
    group_owners = owners[owners["owner_group_key"] == group_key]
    variants = (
        group_owners[column]
        .fillna("(blank)")
        .replace("", "(blank)")
        .value_counts()
        .rename_axis(label)
        .reset_index(name="owner_records")
    )

    return variants


def group_edges(edges: pd.DataFrame, group_key: str) -> pd.DataFrame:
    group_edge_rows = edges[edges["owner_group_key"] == group_key].copy()
    return group_edge_rows[
        [
            "match_method",
            "review_status",
            "discrepancy_type",
            "match_weight",
            "ownername_l",
            "ownername_r",
            "owneraddr_cleaned_l",
            "owneraddr_cleaned_r",
        ]
    ].sort_values(by="match_weight", ascending=False)


def selected_group_key(group_table: pd.DataFrame) -> str | None:
    if group_table.empty:
        return None

    event = st.dataframe(
        group_table,
        hide_index=True,
        use_container_width=True,
        height=520,
        selection_mode="single-row",
        on_select="rerun",
    )

    selected_rows = event.selection.rows
    if selected_rows:
        return str(group_table.iloc[selected_rows[0]]["owner_group_key"])

    return str(group_table.iloc[0]["owner_group_key"])


def render_variant_table(title: str, data: pd.DataFrame) -> None:
    st.subheader(title)
    st.dataframe(
        data,
        hide_index=True,
        use_container_width=True,
        height=min(320, 42 + 35 * max(len(data), 1)),
    )


def main() -> None:
    st.title("Owner Mart Browser")

    group_summary, owners, edges = load_grouped_data()
    if group_summary.empty:
        st.warning("No matched owner groups found in current.parcel_owner_mart.")
        return

    with st.sidebar:
        st.header("Filters")
        search_text = st.text_input("Search owner/address")
        min_owner_records = st.number_input("Minimum owner records", min_value=2, value=2, step=1)
        min_parcels = st.number_input("Minimum parcels", min_value=0, value=0, step=1)
        if st.button("Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    filtered_groups = group_summary[
        (group_summary["owner_records"] >= min_owner_records)
        & (group_summary["parcel_count"] >= min_parcels)
    ].copy()

    if search_text:
        search = search_text.upper()
        search_columns = [
            "display_ownername",
            "display_ownername2",
            "display_address",
            "display_zip",
        ]
        search_mask = filtered_groups[search_columns].fillna("").apply(
            lambda row: row.astype(str).str.upper().str.contains(search, regex=False).any(),
            axis=1,
        )
        filtered_groups = filtered_groups[search_mask].copy()

    st.caption(
        f"{len(filtered_groups):,} matched owner groups shown from "
        f"{len(group_summary):,} total groups."
    )

    list_col, detail_col = st.columns([0.45, 0.55], gap="large")

    with list_col:
        st.subheader("Matched Owners")
        visible_group_table = filtered_groups[
            [
                "display_ownername",
                "display_ownername2",
                "display_address",
                "display_zip",
                "owner_records",
                "parcel_count",
                "ownername_variants",
                "ownername2_variants",
                "address_variants",
                "owner_group_key",
            ]
        ].reset_index(drop=True)
        group_key = selected_group_key(visible_group_table)

    with detail_col:
        if not group_key:
            st.info("No owner group selected.")
            return

        selected_summary = group_summary[group_summary["owner_group_key"] == group_key].iloc[0]
        st.subheader(selected_summary["display_ownername"] or "(blank owner name)")
        metric_cols = st.columns(4)
        metric_cols[0].metric("Owner Records", int(selected_summary["owner_records"]))
        metric_cols[1].metric("Parcels", int(selected_summary["parcel_count"]))
        metric_cols[2].metric("Name Variants", int(selected_summary["ownername_variants"]))
        metric_cols[3].metric("Address Variants", int(selected_summary["address_variants"]))
        st.caption(f"Owner group key: `{group_key}`")
        st.caption(f"Match methods: {selected_summary['match_methods']}")

        tab_names, tab_name2, tab_addr, tab_clean_addr, tab_records, tab_edges = st.tabs(
            [
                "Owner Names",
                "Owner Name 2",
                "Raw Addresses",
                "Cleaned Addresses",
                "Records",
                "Match Edges",
            ]
        )

        with tab_names:
            render_variant_table(
                "Owner Name Variants",
                variant_counts(owners, group_key, "ownername", "ownername"),
            )

        with tab_name2:
            render_variant_table(
                "Owner Name 2 Variants",
                variant_counts(owners, group_key, "ownername2", "ownername2"),
            )

        with tab_addr:
            render_variant_table(
                "Raw Address Variants",
                variant_counts(owners, group_key, "owneraddr_raw", "owneraddr_raw"),
            )

        with tab_clean_addr:
            render_variant_table(
                "Cleaned Address Variants",
                variant_counts(owners, group_key, "owneraddr_cleaned", "owneraddr_cleaned"),
            )

        with tab_records:
            group_owner_records = owners[owners["owner_group_key"] == group_key][
                [
                    "ownername",
                    "ownername2",
                    "owneraddr_raw",
                    "owneraddr_cleaned",
                    "ownerzip_canonical",
                    "ownercity",
                    "ownerstate",
                    "ownercountry",
                    "parse_status",
                    "owner_record_key",
                ]
            ].sort_values(by=["ownername", "owneraddr_raw", "ownerzip_canonical"])
            st.dataframe(group_owner_records, hide_index=True, use_container_width=True)

        with tab_edges:
            st.dataframe(group_edges(edges, group_key), hide_index=True, use_container_width=True)


if __name__ == "__main__":
    main()
