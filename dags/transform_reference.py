from datetime import datetime, timedelta

from airflow.sdk import DAG

from include.transform_dag import TEMPLATE_SEARCHPATH, build_layer

doc_md_DAG = """
### cdw_transform_reference

First staging -> current layer: reloads the lookup tables (municipality,
division, zoning_class, use_type, permit_type, inspection_type, service_type)
with namespaced keys from `crosswalk.entity_xref`.

Manually triggered after `staging_table_prep`. Its Asset schedules
`cdw_transform_entities`, which cascades through the remaining layers.
See documentation/pipeline_architecture/staging_to_current.md.
"""

with DAG(
    "cdw_transform_reference",
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    schedule=None,
    catchup=False,
    doc_md=doc_md_DAG,
    tags=["transform"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    template_searchpath=TEMPLATE_SEARCHPATH,
) as dag:
    build_layer("reference")
