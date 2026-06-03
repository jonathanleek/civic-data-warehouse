# Detect duplicate DAG IDs across files
# This test ensures: Duplicate DAG IDs can cause one DAG to overwrite another and disappear from the UI.
# References:
# - Airflow DAGs & IDs: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html

import ast
from collections import defaultdict

from conftest import call_name, dag_files


def _module_string_constants(tree):
    constants = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if (
            isinstance(target, ast.Name)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            constants[target.id] = node.value.value
    return constants


def _string_value(node, constants):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return constants.get(node.id)
    return None


def _dag_id_from_call(node: ast.Call, constants):
    for keyword in node.keywords:
        if keyword.arg == "dag_id":
            return _string_value(keyword.value, constants)

    if node.args:
        return _string_value(node.args[0], constants)

    return None


def _find_declared_dag_ids(py_path):
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text, filename=str(py_path))
    except SyntaxError:
        return []

    constants = _module_string_constants(tree)
    dag_ids = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and call_name(node).endswith("DAG"):
            dag_id = _dag_id_from_call(node, constants)
            if dag_id:
                dag_ids.append(dag_id)

        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Call) and call_name(decorator).endswith(
                    "dag"
                ):
                    dag_ids.append(_dag_id_from_call(decorator, constants) or node.name)
                elif isinstance(decorator, ast.Name) and decorator.id == "dag":
                    dag_ids.append(node.name)

    return dag_ids


def test_duplicate_dag_ids_across_files():
    dag_id_to_files = defaultdict(list)

    for py in dag_files():
        for dag_id in _find_declared_dag_ids(py):
            dag_id_to_files[dag_id].append(str(py))

    duplicates = {
        dag_id: files for dag_id, files in dag_id_to_files.items() if len(files) > 1
    }
    assert not duplicates, (
        "Duplicate DAG IDs detected (a later file will overwrite the earlier one in the UI):\n"
        + "\n".join(
            f"  {dag_id}: {', '.join(sorted(files))}"
            for dag_id, files in sorted(duplicates.items())
        )
    )
