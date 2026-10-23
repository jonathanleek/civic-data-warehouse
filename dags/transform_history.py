from datetime import datetime, timedelta

from airflow.sdk import DAG

from include.transform_dag import TEMPLATE_SEARCHPATH, build_layer, upstream_assets

doc_md_DAG = """
### cdw_transform_history

Versions every `current` table into `history.record_version`: new and
changed records get a new version, records gone from source get a tombstone.

Runs when `cdw_transform_events` completes (Asset schedule), or manually to re-run this
layer alone. Its Asset schedules
`cdw_transform_qa`.
See documentation/pipeline_architecture/staging_to_current.md.
"""

with DAG(
    "cdw_transform_history",
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    schedule=upstream_assets("history"),
    catchup=False,
    doc_md=doc_md_DAG,
    tags=["transform"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    template_searchpath=TEMPLATE_SEARCHPATH,
) as dag:
    build_layer("history")
