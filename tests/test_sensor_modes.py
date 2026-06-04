# Avoid long-running sensors in poke mode
# This test ensures: Sensors that wait for minutes or hours release worker slots while waiting.
# Guidance: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html

import ast
import os

from conftest import call_name, dag_files

LONG_POKE_INTERVAL_SECONDS = int(
    os.getenv("AIRFLOW_LONG_SENSOR_POKE_INTERVAL_SECONDS", "300")
)
LONG_TIMEOUT_SECONDS = int(os.getenv("AIRFLOW_LONG_SENSOR_TIMEOUT_SECONDS", "1800"))


def _literal_value(node):
    if isinstance(node, ast.Constant):
        return node.value
    return None


def _keyword_value(call: ast.Call, keyword_name: str):
    for keyword in call.keywords:
        if keyword.arg == keyword_name:
            return _literal_value(keyword.value)
    return None


def _is_sensor_call(call: ast.Call) -> bool:
    name = call_name(call)
    return name == "Sensor" or name.endswith("Sensor")


def _is_long_wait_sensor(call: ast.Call) -> bool:
    poke_interval = _keyword_value(call, "poke_interval")
    timeout = _keyword_value(call, "timeout")

    if (
        isinstance(poke_interval, (int, float))
        and poke_interval >= LONG_POKE_INTERVAL_SECONDS
    ):
        return True
    if isinstance(timeout, (int, float)) and timeout >= LONG_TIMEOUT_SECONDS:
        return True
    return False


def _uses_reschedule_or_deferrable(call: ast.Call) -> bool:
    mode = _keyword_value(call, "mode")
    deferrable = _keyword_value(call, "deferrable")

    return mode == "reschedule" or deferrable is True


def _find_long_poke_mode_sensors(py_path):
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text, filename=str(py_path))
    except SyntaxError:
        return []

    violations = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _is_sensor_call(node):
            if _is_long_wait_sensor(node) and not _uses_reschedule_or_deferrable(node):
                violations.append((node.lineno, node.col_offset, call_name(node)))
    return violations


def test_long_running_sensors_release_worker_slots():
    offenders = {}
    for py in dag_files():
        hits = _find_long_poke_mode_sensors(py)
        if hits:
            offenders[py.name] = hits

    assert not offenders, (
        "Long-running sensors should use `mode='reschedule'` or `deferrable=True` "
        "so they do not hold worker slots while waiting. Offending locations:\n"
        + "\n".join(
            f"  {fname}: "
            + ", ".join(f"line {ln}:{col} call={name}" for ln, col, name in hits)
            for fname, hits in offenders.items()
        )
    )
