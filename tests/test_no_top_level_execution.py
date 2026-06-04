# Avoid code execution at import time
# This test ensures: Expensive or side-effectful code (print, sleeps, I/O, network) should not run at module import.
# Guidance: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#top-level-python-code

import ast

from conftest import call_name, dag_files

_SIDE_EFFECT_EXACT_CALLS = {
    "open",
    "print",
    "read_csv",
    "read_json",
    "read_parquet",
    "Variable.get",
    "BaseHook.get_connection",
}

_SIDE_EFFECT_PREFIXES = (
    "boto3.",
    "google.",
    "httpx.",
    "os.system",
    "requests.",
    "subprocess.",
)

_SIDE_EFFECT_SUFFIXES = (
    ".BaseHook.get_connection",
    ".Variable.get",
    ".execute",
    ".get_records",
    ".read_csv",
    ".read_json",
    ".read_parquet",
    ".run",
    "sleep",
)


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


def _is_side_effect_call(name: str) -> bool:
    if name in _SIDE_EFFECT_EXACT_CALLS:
        return True
    if any(name.startswith(prefix) for prefix in _SIDE_EFFECT_PREFIXES):
        return True
    if any(name.endswith(suffix) for suffix in _SIDE_EFFECT_SUFFIXES):
        return True
    return False


def _find_top_level_side_effects(py_path):
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text, filename=str(py_path))
    except Exception:
        return []
    aliases = _import_aliases(tree)
    violations = []

    class TopLevelSideEffectVisitor(ast.NodeVisitor):
        def visit_FunctionDef(self, node):
            return

        def visit_AsyncFunctionDef(self, node):
            return

        def visit_ClassDef(self, node):
            return

        def visit_Call(self, node):
            name = _resolve_alias(call_name(node), aliases)
            if _is_side_effect_call(name):
                violations.append((node.lineno, node.col_offset, name))
            self.generic_visit(node)

    visitor = TopLevelSideEffectVisitor()
    for statement in tree.body:
        visitor.visit(statement)

    return violations


def test_no_top_level_execution_side_effects():
    offenders = {}
    for py in dag_files():
        hits = _find_top_level_side_effects(py)
        if hits:
            offenders[py.name] = hits
    assert not offenders, (
        "Avoid side-effectful code at module import time (e.g., print/sleep/I/O/network/subprocess). Offending locations:\n"
        + "\n".join(
            f"  {fname}: "
            + ", ".join(f"line {ln}:{col} call={name}" for ln, col, name in hits)
            for fname, hits in offenders.items()
        )
    )
