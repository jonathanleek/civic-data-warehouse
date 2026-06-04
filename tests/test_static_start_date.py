# Avoid dynamic start_date values in DAG definitions
# This test ensures: DAG schedules are based on stable, deterministic start dates.
# Guidance: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#top-level-python-code

import ast

from conftest import call_name, dag_files

_DYNAMIC_DATE_CALLS = {
    "date.today",
    "datetime.date.today",
    "datetime.datetime.now",
    "datetime.datetime.today",
    "datetime.datetime.utcnow",
    "datetime.now",
    "datetime.today",
    "datetime.utcnow",
    "pendulum.now",
    "pendulum.today",
    "timezone.utcnow",
    "airflow.utils.dates.days_ago",
    "days_ago",
}


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


def _string_key(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _contains_dynamic_date_call(node, aliases) -> bool:
    for child in ast.walk(node):
        if isinstance(child, ast.Call):
            name = _resolve_alias(call_name(child), aliases)
            if name in _DYNAMIC_DATE_CALLS:
                return True
    return False


def _dict_has_dynamic_start_date(node, aliases) -> bool:
    if not isinstance(node, ast.Dict):
        return False

    for key, value in zip(node.keys, node.values):
        if _string_key(key) == "start_date" and _contains_dynamic_date_call(
            value, aliases
        ):
            return True
    return False


def _module_dict_assignments(tree):
    assignments = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if isinstance(target, ast.Name) and isinstance(node.value, ast.Dict):
            assignments[target.id] = node.value
    return assignments


def _find_dynamic_start_dates(py_path):
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text, filename=str(py_path))
    except SyntaxError:
        return []

    aliases = _import_aliases(tree)
    dict_assignments = _module_dict_assignments(tree)
    violations = []

    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == "start_date":
            if _contains_dynamic_date_call(node.value, aliases):
                violations.append(
                    (node.lineno, node.col_offset, "dynamic start_date keyword")
                )

        if isinstance(node, ast.keyword) and node.arg == "default_args":
            if _dict_has_dynamic_start_date(node.value, aliases):
                violations.append(
                    (node.lineno, node.col_offset, "dynamic start_date in default_args")
                )
            elif isinstance(node.value, ast.Name):
                default_args = dict_assignments.get(node.value.id)
                if default_args and _dict_has_dynamic_start_date(default_args, aliases):
                    violations.append(
                        (
                            node.lineno,
                            node.col_offset,
                            f"dynamic start_date in default_args variable '{node.value.id}'",
                        )
                    )

    return violations


def test_start_date_is_static():
    offenders = {}
    for py in dag_files():
        hits = _find_dynamic_start_dates(py)
        if hits:
            offenders[py.name] = hits

    assert not offenders, (
        "DAG start_date values must be static and deterministic; avoid datetime.now(), "
        "pendulum.now(), days_ago(), and similar moving dates. Offending locations:\n"
        + "\n".join(
            f"  {fname}: "
            + ", ".join(f"line {ln}:{col} {desc}" for ln, col, desc in hits)
            for fname, hits in offenders.items()
        )
    )
