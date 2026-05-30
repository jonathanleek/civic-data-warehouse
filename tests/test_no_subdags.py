# Avoid SubDAGs; use TaskGroups instead
# This test ensures: DAGs do not use SubDagOperator or subdag helpers that complicate scheduling.
# Guidance: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#taskgroups

import ast

from conftest import call_name, dag_files


def _import_aliases(tree):
    aliases = {}
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                aliases[alias.asname or alias.name.split(".")[0]] = alias.name
        elif isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                aliases[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    return aliases


def _resolve_alias(name: str, aliases: dict[str, str]) -> str:
    if not name:
        return name

    first, *rest = name.split(".")
    if first not in aliases:
        return name
    return ".".join([aliases[first], *rest])


def _find_subdag_usage(py_path):
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text, filename=str(py_path))
    except SyntaxError:
        return []

    aliases = _import_aliases(tree)
    violations = []

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif node.module:
                names = [f"{node.module}.{alias.name}" for alias in node.names]

            for name in names:
                if "subdag" in name.lower():
                    violations.append((node.lineno, node.col_offset, f"import {name}"))

        if isinstance(node, ast.Call):
            name = _resolve_alias(call_name(node), aliases)
            if name == "SubDagOperator" or name.endswith(".SubDagOperator"):
                violations.append((node.lineno, node.col_offset, f"call {name}"))

    return violations


def test_no_subdags():
    offenders = {}
    for py in dag_files():
        hits = _find_subdag_usage(py)
        if hits:
            offenders[py.name] = hits

    assert not offenders, (
        "Avoid SubDAGs; use TaskGroups or separate DAGs with datasets/assets instead. Offending locations:\n"
        + "\n".join(
            f"  {fname}: "
            + ", ".join(f"line {ln}:{col} {desc}" for ln, col, desc in hits)
            for fname, hits in offenders.items()
        )
    )
