from datetime import datetime, timedelta

from airflow.sdk import DAG

from include.transform_dag import TEMPLATE_SEARCHPATH, build_layer, upstream_assets

doc_md_DAG = """
### cdw_transform_events

Reloads the event tables: `assessment`, `tax_delinquency`, `permit`,
`inspection`, `condemnation`, `service`.

Runs when `cdw_transform_spine` completes (Asset schedule), or manually to re-run this
layer alone. Its Asset schedules
`cdw_transform_history`.
See documentation/pipeline_architecture/staging_to_current.md.
"""

with DAG(
    "cdw_transform_events",
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    schedule=upstream_assets("events"),
    catchup=False,
    doc_md=doc_md_DAG,
    tags=["transform"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    template_searchpath=TEMPLATE_SEARCHPATH,
) as dag:
    build_layer("events")
