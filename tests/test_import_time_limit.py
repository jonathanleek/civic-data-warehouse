# Avoid slow imports due to heavy logic
# This test ensures: Heavy logic at DAG parse time can delay the scheduler; move work into tasks.
# Guidance: https://docs.astronomer.io/learn/dynamically-generating-dags

import importlib.util
import os
import sys
import time

import pytest
from conftest import dag_files

pytestmark = pytest.mark.skipif(
    os.getenv("AIRFLOW_RUN_IMPORT_TIMING_TESTS") != "1",
    reason="DAG import timing tests execute DAG files; set AIRFLOW_RUN_IMPORT_TIMING_TESTS=1 to run them",
)

# Default per-file budget in seconds; can be overridden for CI via env
DEFAULT_LIMIT_S = float(os.getenv("AIRFLOW_DAG_IMPORT_TIME_LIMIT_S", "1.0"))


def _timed_import(py_path) -> float:
    """Import a module from a path with a unique name and return elapsed seconds."""
    name = f"_dagmod_{py_path.stem}_{int(time.time() * 1000000)}"
    spec = importlib.util.spec_from_file_location(name, str(py_path))
    module = importlib.util.module_from_spec(spec)
    start = time.perf_counter()
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    elapsed = time.perf_counter() - start
    sys.modules.pop(name, None)
    return elapsed


def test_import_time_limit_per_file():
    slow = []
    for py in dag_files():
        try:
            elapsed = _timed_import(py)
            if elapsed > DEFAULT_LIMIT_S:
                slow.append((py.name, elapsed))
        except Exception as e:
            slow.append((py.name, f"import error: {e}"))
    assert not slow, (
        "DAG import too slow or error-prone. Per-file limit is "
        f"{DEFAULT_LIMIT_S:.2f}s. Offenders:\n"
        + "\n".join(f"  {name}: {elapsed}" for name, elapsed in slow)
    )
