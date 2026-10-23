"""Builds the task chain for one staging -> current transform layer.

Each layer DAG (dags/transform_*.py) is: set namespace -> rebuild source views
-> one SQLExecuteQueryOperator per SQL file in include/transform_layers.py, in
order. The last task emits the layer's Asset so the next layer's DAG runs.
Each SQL file runs in one transaction, so a failed task leaves its target table
as it was before the task started.
"""

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Asset, chain

from include.transform_layers import SET_NAMESPACE_SQL, SQL_DIR, layer_files

CONN_ID = "cdw-dev"

# Layer i+1 is scheduled on layer i's Asset.
LAYER_ORDER = ["reference", "entities", "spine", "events", "history", "qa"]
LAYER_ASSETS = {layer: Asset(name=f"cdw_current_{layer}") for layer in LAYER_ORDER}


def upstream_assets(layer: str) -> list[Asset]:
    """The schedule for a layer DAG: the Asset of the layer before it."""
    index = LAYER_ORDER.index(layer)
    return [LAYER_ASSETS[LAYER_ORDER[index - 1]]] if index else []


def task_id_for(sql_file: str) -> str:
    return sql_file.removesuffix(".sql").replace("current/", "").replace("/", "__")


def build_layer(layer: str):
    """Create the layer's tasks in the active DAG context and chain them."""
    tasks = [
        SQLExecuteQueryOperator(
            task_id="set_namespace",
            conn_id=CONN_ID,
            sql=SET_NAMESPACE_SQL,
        )
    ]
    files = layer_files(layer)
    for sql_file in files:
        tasks.append(
            SQLExecuteQueryOperator(
                task_id=task_id_for(sql_file),
                conn_id=CONN_ID,
                sql=sql_file,
                outlets=[LAYER_ASSETS[layer]] if sql_file == files[-1] else [],
            )
        )
    chain(*tasks)
    return tasks


TEMPLATE_SEARCHPATH = [str(SQL_DIR)]
