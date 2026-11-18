# SPDX-License-Identifier: Apache-2.0
"""Write a gated copy of the exact worker file. The input is never replaced."""
import argparse
import ast
from pathlib import Path


def gated_copy(original):
    tree = ast.parse(original)
    workers = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "NPUWorker"]
    if len(workers) != 1:
        raise ValueError("Expected one NPUWorker class")
    methods = [node for node in workers[0].body if isinstance(node, ast.FunctionDef)
               and node.name == "initialize_from_config"]
    if len(methods) != 1:
        raise ValueError("Expected one NPUWorker.initialize_from_config")
    matches = [node for node in methods[0].body if isinstance(node, ast.Expr)
               and isinstance(node.value, ast.Call)
               and isinstance(node.value.func, ast.Name)
               and node.value.func.id == "ensure_kv_transfer_initialized"]
    if len(matches) != 1 or "before_kv_init(self)" in original:
        raise ValueError("Expected one unmodified top-level KV initialization call")
    lines = original.splitlines(keepends=True)
    target = matches[0].lineno - 1
    indent = lines[target][:matches[0].col_offset]
    extra = (
        "from os import environ as _ks_order_environ",
        'if _ks_order_environ.get("KS_ORDER_GATE_DIR"):',
        "    from ks_order_gate import before_kv_init",
        "    before_kv_init(self)",
    )
    lines[target:target] = [indent + l + "\n" for l in extra]
    body = "".join(lines)
    compile(body, "worker.py", "exec")
    return body


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    contents = gated_copy(args.source.read_text(encoding="utf8"))
    with args.output.open("x", encoding="utf8", newline="\n") as file:
        file.write(contents)
