# Avoid use of globals() to register DAGs
# This test ensures: Using globals() to inject DAGs hides structure, confuses linters, and makes imports fragile.
# Guidance: prefer returning/collecting DAG objects explicitly or using module-level variables without globals().
# See also: https://docs.astronomer.io/learn/dynamically-generating-dags

import ast

from conftest import dag_files


def _detect_globals_injection(py_path):
    """Return a list of (lineno, col, kind) where the file assigns via globals()[...] = ... or calls globals() in assignment."""
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    violations = []
    try:
        tree = ast.parse(text, filename=str(py_path))
    except Exception:
        return violations

    class Visitor(ast.NodeVisitor):
        def visit_Assign(self, node: ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Subscript) and isinstance(tgt.value, ast.Call):
                    call = tgt.value
                    if isinstance(call.func, ast.Name) and call.func.id == "globals":
                        violations.append(
                            (
                                node.lineno,
                                node.col_offset,
                                "assign_to_globals_subscript",
                            )
                        )
            self.generic_visit(node)

        def visit_Call(self, node: ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == "globals":
                violations.append((node.lineno, node.col_offset, "call_globals"))
            self.generic_visit(node)

    Visitor().visit(tree)

    # De-duplicate: if both assign_to_globals_subscript and call_globals on same line, keep the assign variant
    dedup = {}
    for lineno, col, kind in violations:
        prev = dedup.get((lineno, col))
        if prev == "call_globals" and kind == "assign_to_globals_subscript":
            dedup[(lineno, col)] = kind
        elif prev is None:
            dedup[(lineno, col)] = kind
    out = [(ln, c, k) for (ln, c), k in sorted(dedup.items())]
    return out


def test_no_globals_injection():
    offenders = {}
    for py in dag_files():
        hits = _detect_globals_injection(py)
        if hits:
            offenders[py.name] = hits

    assert not offenders, (
        "Avoid injecting DAGs with globals(); declare DAGs explicitly instead. Offending locations:\n"
        + "\n".join(
            f"  {fname}: "
            + ", ".join(f"line {ln}:{col} ({kind})" for ln, col, kind in hits)
            for fname, hits in offenders.items()
        )
    )
