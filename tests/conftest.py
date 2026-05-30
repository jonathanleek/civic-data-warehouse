# Shared fixtures and helpers for Airflow best-practices tests

import ast
from pathlib import Path

import pytest

# Resolve relative to this file so tests work from any working directory
DAGS_DIR = (Path(__file__).parent.parent / "dags").resolve()


def dag_files():
    """Yield .py paths in DAGS_DIR, skipping __init__.py, dotfiles, and underscore-prefixed files."""
    for py in sorted(DAGS_DIR.glob("*.py")):
        if py.name.startswith((".", "_")):
            continue
        yield py


def call_name(node: ast.Call) -> str:
    """Return a best-effort dotted function name for an ast.Call node."""
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        parts = []
        cur = node.func
        while isinstance(cur, ast.Attribute):
            parts.append(cur.attr)
            cur = cur.value
        if isinstance(cur, ast.Name):
            parts.append(cur.id)
        return ".".join(reversed(parts))
    return ""


@pytest.fixture(scope="session")
def generated_dags():
    """Load all DAGs via DagBag and return a {dag_id: dag} mapping."""
    try:
        from airflow.models import DagBag
    except ModuleNotFoundError:
        pytest.skip("Airflow is required for DagBag-based tests")

    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    return bag.dags
