from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.prcl_owner_mart import apply_accepted_review_groups, rebuild_parcel_owner_mart
from include.prcl_owner_matching import rebuild_int_prcl_owner_match_candidates
from include.prcl_owner_parser import rebuild_int_prcl_owners

doc_md_DAG = """
### prcl_owner_parsing

Rebuilds `current.int_prcl_owners` from raw parcel owner fields in
`staging.prcl_prcl`, using `usaddress` to parse `owneraddr` into address
components. It then uses Splink to generate probabilistic owner/address
match candidates in `current.int_prcl_owner_match_candidates`, and builds
accepted owner review groups before building `current.parcel_owner_mart`
from high-confidence and human-reviewed matches.
"""


with DAG(
    "prcl_owner_parsing",
    description="Parse parcel owner mailing addresses into int_prcl_owners",
    schedule=None,
    start_date=datetime(2022, 12, 30),
    catchup=False,
    max_active_runs=1,
    tags=["parcel", "owners", "intermediate"],
    doc_md=doc_md_DAG,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
) as dag:
    create_int_prcl_owners = PythonOperator(
        task_id="create_int_prcl_owners",
        python_callable=rebuild_int_prcl_owners,
        op_kwargs={"postgres_conn_id": "cdw-dev"},
    )

    create_int_prcl_owner_match_candidates = PythonOperator(
        task_id="create_int_prcl_owner_match_candidates",
        python_callable=rebuild_int_prcl_owner_match_candidates,
        op_kwargs={"postgres_conn_id": "cdw-dev"},
    )

    create_parcel_owner_mart = PythonOperator(
        task_id="create_parcel_owner_mart",
        python_callable=rebuild_parcel_owner_mart,
        op_kwargs={"postgres_conn_id": "cdw-dev"},
    )

    apply_owner_review_groups = PythonOperator(
        task_id="apply_owner_review_groups",
        python_callable=apply_accepted_review_groups,
        op_kwargs={"postgres_conn_id": "cdw-dev"},
    )

    (
        create_int_prcl_owners
        >> create_int_prcl_owner_match_candidates
        >> apply_owner_review_groups
        >> create_parcel_owner_mart
    )
