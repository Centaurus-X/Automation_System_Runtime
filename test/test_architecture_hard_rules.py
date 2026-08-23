# -*- coding: utf-8 -*-

"""Executable hard-rule gate for the complete shipped Python codebase."""

import ast
import os
import sys


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
SCAN_ROOTS = ("src", "test", "_tools")


def _python_files():
    result = []
    for root_name in SCAN_ROOTS:
        root = os.path.join(PROJECT_ROOT, root_name)
        for current_root, directory_names, file_names in os.walk(root):
            directory_names[:] = sorted(
                name for name in directory_names if name != "__pycache__")
            for file_name in sorted(file_names):
                if file_name.endswith(".py"):
                    result.append(os.path.join(current_root, file_name))
    return result


def _function_has_annotations(node):
    arguments = list(node.args.posonlyargs)
    arguments.extend(node.args.args)
    arguments.extend(node.args.kwonlyargs)
    if node.args.vararg is not None:
        arguments.append(node.args.vararg)
    if node.args.kwarg is not None:
        arguments.append(node.args.kwarg)
    return bool(
        node.returns is not None
        or any(argument.annotation is not None for argument in arguments)
    )


def _node_violation(node):
    if isinstance(node, ast.ClassDef):
        return "class"
    if isinstance(node, ast.Lambda):
        return "lambda"
    if isinstance(node, ast.AnnAssign):
        return "annotated assignment"
    if isinstance(node, ast.MatMult):
        return "@ operator"
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        if node.decorator_list:
            return "decorator"
        if _function_has_annotations(node):
            return "type annotation"
    if isinstance(node, ast.Import):
        if any(alias.name == "typing" for alias in node.names):
            return "typing import"
    if isinstance(node, ast.ImportFrom):
        if node.module == "typing":
            return "typing import"
        if node.module == "__future__" and any(
                alias.name == "annotations" for alias in node.names):
            return "future annotations import"
        if node.module == "types" and any(
                alias.name == "SimpleNamespace" for alias in node.names):
            return "SimpleNamespace import"
        if node.module == "dataclasses":
            return "dataclasses import"
    return ""


def run_test():
    violations = []
    for path in _python_files():
        with open(path, "r", encoding="utf-8") as handle:
            tree = ast.parse(handle.read(), filename=path)
        for node in ast.walk(tree):
            violation = _node_violation(node)
            if violation:
                relative = os.path.relpath(path, PROJECT_ROOT)
                violations.append((relative, getattr(node, "lineno", 0), violation))
    assert not violations, "architecture hard-rule violations: {}".format(
        violations[:50])
    print("PASS architecture hard rules across src, test and _tools")


if __name__ == "__main__":
    run_test()
