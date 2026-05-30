# Limit static task creation in loops; prefer dynamic task mapping
# This test ensures: Loop-generated tasks do not create large static DAGs at parse time.
# Guidance: https://docs.astronomer.io/learn/dynamic-tasks

import ast
import os

from conftest import call_name, dag_files

_OPERATOR_HINTS = {"Operator", "Sensor"}
STATIC_TASK_CREATION_LIMIT = int(os.getenv("AIRFLOW_STATIC_TASK_CREATION_LIMIT", "50"))


def _is_task_decorator(decorator) -> bool:
    if isinstance(decorator, ast.Call):
        name = call_name(decorator)
    else:
        name = call_name(ast.Call(func=decorator, args=[], keywords=[]))

    return name == "task" or name.startswith("task.") or name.endswith(".task")


def _taskflow_function_names(tree):
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            if any(_is_task_decorator(decorator) for decorator in node.decorator_list):
                names.add(node.name)
    return names


def _looks_like_task_creation(
    call: ast.Call, taskflow_function_names: set[str]
) -> bool:
    name = call_name(call)
    if not name:
        return False
    if any(hint in name for hint in _OPERATOR_HINTS):
        return True
    if name in taskflow_function_names:
        return True
    return False


def _literal_int(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, int):
        return node.value
    return None


def _range_length(node: ast.Call):
    values = [_literal_int(arg) for arg in node.args]
    if any(value is None for value in values):
        return None

    try:
        return len(range(*values))
    except ValueError:
        return None


def _estimated_iter_count(node, constants):
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return len(node.elts)
    if isinstance(node, ast.Dict):
        return len(node.keys)
    if isinstance(node, ast.Name):
        return constants.get(node.id)
    if isinstance(node, ast.Call):
        name = call_name(node)
        if name == "range":
            return _range_length(node)
        if name == "enumerate" and node.args:
            return _estimated_iter_count(node.args[0], constants)
        if name == "zip" and node.args:
            counts = [_estimated_iter_count(arg, constants) for arg in node.args]
            if all(count is not None for count in counts):
                return min(counts)
    return None


def _module_iterable_counts(tree):
    constants = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if isinstance(target, ast.Name):
            count = _estimated_iter_count(node.value, constants)
            if count is not None:
                constants[target.id] = count
    return constants


class _StaticTaskLoopVisitor(ast.NodeVisitor):
    def __init__(self, taskflow_function_names: set[str], constants: dict[str, int]):
        self.taskflow_function_names = taskflow_function_names
        self.constants = constants
        self.violations = []
        self.loop_counts = []

    def visit_FunctionDef(self, node):
        if any(_is_task_decorator(decorator) for decorator in node.decorator_list):
            return
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node):
        if any(_is_task_decorator(decorator) for decorator in node.decorator_list):
            return
        self.generic_visit(node)

    def visit_For(self, node: ast.For):
        self.loop_counts.append(_estimated_iter_count(node.iter, self.constants))
        self.generic_visit(node)
        self.loop_counts.pop()

    def visit_While(self, node: ast.While):
        self.loop_counts.append(None)
        self.generic_visit(node)
        self.loop_counts.pop()

    def visit_Call(self, node: ast.Call):
        name = call_name(node)
        if self.loop_counts and _looks_like_task_creation(
            node, self.taskflow_function_names
        ):
            if all(count is not None for count in self.loop_counts):
                estimated_count = 1
                for count in self.loop_counts:
                    estimated_count *= count
                if estimated_count > STATIC_TASK_CREATION_LIMIT:
                    self.violations.append(
                        (
                            node.lineno,
                            node.col_offset,
                            name,
                            f"estimated loop-created tasks={estimated_count}",
                        )
                    )
            elif len(self.loop_counts) >= 2:
                self.violations.append(
                    (
                        node.lineno,
                        node.col_offset,
                        name,
                        "nested dynamic loop-created task count is unknown",
                    )
                )
        self.generic_visit(node)


def _find_excessive_static_task_creation(py_path):
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text, filename=str(py_path))
    except Exception:
        return []
    visitor = _StaticTaskLoopVisitor(
        _taskflow_function_names(tree), _module_iterable_counts(tree)
    )
    visitor.visit(tree)
    return visitor.violations


def test_static_task_creation_from_loops_is_limited():
    offenders = {}
    for py in dag_files():
        hits = _find_excessive_static_task_creation(py)
        if hits:
            offenders[py.name] = hits
    assert not offenders, (
        f"Avoid creating more than {STATIC_TASK_CREATION_LIMIT} static tasks from parse-time loops; "
        "use dynamic task mapping instead. Offending locations:\n"
        + "\n".join(
            f"  {fname}: "
            + ", ".join(
                f"line {ln}:{col} call={name} ({reason})"
                for ln, col, name, reason in hits
            )
            for fname, hits in offenders.items()
        )
    )
