# Per-statement import profiling for DAG files
# This test ensures: When a DAG file is slow to import, the specific top-level
# statement(s) responsible are identified with line numbers and timing.
# Guidance: https://docs.astronomer.io/learn/dynamically-generating-dags

import ast
import importlib.util
import os
import sys
import textwrap
import time

import pytest
from conftest import dag_files

pytestmark = pytest.mark.skipif(
    os.getenv("AIRFLOW_RUN_PARSE_PROFILING_TESTS") != "1",
    reason="DAG parse profiling executes DAG files; set AIRFLOW_RUN_PARSE_PROFILING_TESTS=1 to run it",
)

# Per-statement threshold in seconds
DEFAULT_STATEMENT_LIMIT_S = float(os.getenv("AIRFLOW_STATEMENT_TIME_LIMIT_S", "0.5"))

# Overall file threshold — if the file imports fast, skip per-statement reporting
DEFAULT_FILE_LIMIT_S = float(os.getenv("AIRFLOW_DAG_IMPORT_TIME_LIMIT_S", "1.0"))


def _snippet(source_lines, lineno, end_lineno, max_lines=3):
    """Return a short code snippet for the statement starting at lineno."""
    start = lineno - 1  # 0-indexed
    end = min(end_lineno, start + max_lines)
    lines = source_lines[start:end]
    snippet = "\n".join(lines)
    if end_lineno > end:
        snippet += "\n    ..."
    return snippet


def _profile_file(py_path):
    """Instrument and exec a DAG file, returning per-statement timings.

    Returns a list of (lineno, end_lineno, elapsed) for every top-level
    statement, or None if the file cannot be parsed / executed.
    """
    source = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(source, filename=str(py_path))
    except SyntaxError:
        return None

    # We'll build a new module body that wraps each original statement
    # with timing instrumentation.  The approach:
    #   _t0 = time.perf_counter()
    #   <original statement>
    #   _timings.append((lineno, end_lineno, time.perf_counter() - _t0))
    new_body = []

    # Inject: import time as _time_mod; _timings = []
    setup = ast.parse(
        "import time as _time_mod\n_timings = []",
        mode="exec",
    ).body
    new_body.extend(setup)

    for node in tree.body:
        lineno = node.lineno
        end_lineno = getattr(node, "end_lineno", lineno) or lineno

        # _t0 = _time_mod.perf_counter()
        before = ast.parse("_t0 = _time_mod.perf_counter()", mode="exec").body[0]

        # _timings.append((lineno, end_lineno, _time_mod.perf_counter() - _t0))
        record_src = (
            f"_timings.append(({lineno}, {end_lineno}, _time_mod.perf_counter() - _t0))"
        )
        after = ast.parse(record_src, mode="exec").body[0]

        new_body.append(before)
        new_body.append(node)
        new_body.append(after)

    tree.body = new_body
    ast.fix_missing_locations(tree)

    code = compile(tree, filename=str(py_path), mode="exec")

    # Execute in an isolated module namespace
    mod_name = f"_profmod_{py_path.stem}_{int(time.time() * 1000000)}"
    spec = importlib.util.spec_from_file_location(mod_name, str(py_path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module

    try:
        exec(code, module.__dict__)  # noqa: S102
        timings = module.__dict__.get("_timings", [])
    except Exception:
        return None
    finally:
        sys.modules.pop(mod_name, None)

    return timings


def test_dag_parse_profiling():
    """Flag specific top-level statements that exceed the per-statement time budget."""
    slow_statements = []

    for py in dag_files():
        timings = _profile_file(py)
        if timings is None:
            continue  # parse/exec error — other tests catch this

        total = sum(t[2] for t in timings)
        if total <= DEFAULT_FILE_LIMIT_S:
            continue  # file is fast overall, no need to report

        source_lines = py.read_text(encoding="utf-8", errors="ignore").splitlines()

        for lineno, end_lineno, elapsed in timings:
            if elapsed > DEFAULT_STATEMENT_LIMIT_S:
                snippet = _snippet(source_lines, lineno, end_lineno)
                slow_statements.append(
                    f"  {py.name}:{lineno} ({elapsed:.3f}s)\n"
                    f"    {textwrap.indent(snippet, '    ').strip()}"
                )

    assert not slow_statements, (
        "Slow top-level statements detected during DAG import. "
        f"Per-statement limit: {DEFAULT_STATEMENT_LIMIT_S:.2f}s, "
        f"file threshold: {DEFAULT_FILE_LIMIT_S:.2f}s.\n" + "\n".join(slow_statements)
    )
