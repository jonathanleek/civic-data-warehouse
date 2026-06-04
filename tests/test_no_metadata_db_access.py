# Avoid direct Airflow metadata database access from DAG source
# This test ensures: DAGs do not couple workflow code to Airflow's internal metadata DB schema/session APIs.
# Guidance: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#metadata-db-maintenance

import ast

from conftest import call_name, dag_files

_DISALLOWED_IMPORTS = {
    "airflow.settings.Session",
    "airflow.utils.session.create_session",
    "airflow.utils.session.provide_session",
}

_SESSION_FACTORY_CALLS = {
    "airflow.settings.Session",
    "airflow.utils.session.create_session",
}

_DISALLOWED_DECORATORS = {
    "airflow.utils.session.provide_session",
}


def _import_aliases(tree):
    aliases = {}
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                aliases[alias.asname or alias.name.split(".")[0]] = alias.name
        elif isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                full_name = f"{node.module}.{alias.name}"
                aliases[alias.asname or alias.name] = full_name
    return aliases


def _resolve_alias(name: str, aliases: dict[str, str]) -> str:
    if not name:
        return name

    first, *rest = name.split(".")
    if first not in aliases:
        return name
    return ".".join([aliases[first], *rest])


def _target_names(target):
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, (ast.Tuple, ast.List)):
        names = []
        for element in target.elts:
            names.extend(_target_names(element))
        return names
    return []


def _is_session_factory_call(node, aliases) -> bool:
    return (
        isinstance(node, ast.Call)
        and _resolve_alias(call_name(node), aliases) in _SESSION_FACTORY_CALLS
    )


def _metadata_session_names(tree, aliases):
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and _is_session_factory_call(
            node.value, aliases
        ):
            for target in node.targets:
                names.update(_target_names(target))

        if isinstance(node, ast.AnnAssign) and _is_session_factory_call(
            node.value, aliases
        ):
            names.update(_target_names(node.target))

        if isinstance(node, ast.With):
            for item in node.items:
                if (
                    _is_session_factory_call(item.context_expr, aliases)
                    and item.optional_vars
                ):
                    names.update(_target_names(item.optional_vars))
    return names


def _find_metadata_db_access(py_path):
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text, filename=str(py_path))
    except SyntaxError:
        return []

    aliases = _import_aliases(tree)
    session_names = _metadata_session_names(tree, aliases)
    violations = []

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                full_name = f"{node.module}.{alias.name}"
                if full_name in _DISALLOWED_IMPORTS:
                    violations.append(
                        (node.lineno, node.col_offset, f"import {full_name}")
                    )

        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for decorator in node.decorator_list:
                name = (
                    call_name(decorator)
                    if isinstance(decorator, ast.Call)
                    else call_name(ast.Call(func=decorator, args=[], keywords=[]))
                )
                resolved = _resolve_alias(name, aliases)
                if resolved in _DISALLOWED_DECORATORS:
                    violations.append(
                        (
                            decorator.lineno,
                            decorator.col_offset,
                            f"decorator {resolved}",
                        )
                    )

        if isinstance(node, ast.Call):
            name = _resolve_alias(call_name(node), aliases)
            if name in _SESSION_FACTORY_CALLS:
                violations.append((node.lineno, node.col_offset, f"call {name}"))
            elif name.endswith(".query") and name.split(".", 1)[0] in session_names:
                violations.append((node.lineno, node.col_offset, f"call {name}"))

    return violations


def test_no_direct_airflow_metadata_db_access():
    offenders = {}
    for py in dag_files():
        hits = _find_metadata_db_access(py)
        if hits:
            offenders[py.name] = hits

    assert not offenders, (
        "Avoid direct Airflow metadata database access from DAG code; use Airflow APIs, context, "
        "XComs, Variables, Connections, or external application tables instead. Offending locations:\n"
        + "\n".join(
            f"  {fname}: "
            + ", ".join(f"line {ln}:{col} {desc}" for ln, col, desc in hits)
            for fname, hits in offenders.items()
        )
    )
