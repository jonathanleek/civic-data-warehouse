# Avoid hardcoded secrets and connection strings in DAG source
# This test ensures: Credentials stay in Airflow connections, variables, or external secret backends.
# Guidance: https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html

import ast
import re

from conftest import dag_files

_SECRET_NAME_PATTERN = re.compile(
    r"(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|client[_-]?secret)",
    re.IGNORECASE,
)

_CONNECTION_STRING_PATTERN = re.compile(
    r"\b(?:postgres(?:ql)?|mysql|mssql|redshift|snowflake|mongodb(?:\+srv)?|redis|amqp|http|https)://[^\s'\"]+:[^\s'\"]+@",
    re.IGNORECASE,
)

_SECRET_VALUE_PATTERN = re.compile(
    r"\b(?:AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|xox[baprs]-[A-Za-z0-9-]{10,}|sk-[A-Za-z0-9]{20,})\b"
)

_PLACEHOLDER_VALUES = {
    "",
    "changeme",
    "dummy",
    "example",
    "fake",
    "mock",
    "none",
    "placeholder",
    "redacted",
    "test",
    "todo",
}

_SAFE_SECRET_NAME_SUFFIXES = (
    "_conn_id",
    "_connection_id",
    "_variable_key",
    "_var_key",
    "_secret_name",
)


def _string_value(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _target_names(target):
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, ast.Attribute):
        return [target.attr]
    if isinstance(target, (ast.Tuple, ast.List)):
        names = []
        for element in target.elts:
            names.extend(_target_names(element))
        return names
    return []


def _looks_like_secret_name(name: str) -> bool:
    lowered = name.lower()
    if lowered.endswith(_SAFE_SECRET_NAME_SUFFIXES):
        return False
    return bool(_SECRET_NAME_PATTERN.search(lowered))


def _looks_like_secret_value(value: str) -> bool:
    stripped = value.strip()
    if stripped.lower() in _PLACEHOLDER_VALUES:
        return False
    return bool(
        _CONNECTION_STRING_PATTERN.search(stripped)
        or _SECRET_VALUE_PATTERN.search(stripped)
    )


def _is_sensitive_assignment(node):
    if not isinstance(node, (ast.Assign, ast.AnnAssign)):
        return []

    value_node = node.value
    value = _string_value(value_node)
    if value is None:
        return []

    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
    names = [name for target in targets for name in _target_names(target)]
    violations = []

    for name in names:
        if (
            _looks_like_secret_name(name)
            and value.strip().lower() not in _PLACEHOLDER_VALUES
        ):
            violations.append((node.lineno, node.col_offset, f"{name}=<literal>"))

    if _looks_like_secret_value(value):
        label = names[0] if names else "literal"
        violations.append(
            (node.lineno, node.col_offset, f"{label}=<credential-like literal>")
        )

    return violations


def _find_hardcoded_secrets(py_path):
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text, filename=str(py_path))
    except SyntaxError:
        return []

    violations = []
    for node in ast.walk(tree):
        violations.extend(_is_sensitive_assignment(node))
    return violations


def test_no_hardcoded_secrets_in_dags():
    offenders = {}
    for py in dag_files():
        hits = _find_hardcoded_secrets(py)
        if hits:
            offenders[py.name] = hits

    assert not offenders, (
        "Avoid hardcoded secrets or credential-bearing connection strings in DAG source; "
        "use Airflow connections, variables, or a secrets backend instead. Offending locations:\n"
        + "\n".join(
            f"  {fname}: "
            + ", ".join(f"line {ln}:{col} {desc}" for ln, col, desc in hits)
            for fname, hits in offenders.items()
        )
    )
