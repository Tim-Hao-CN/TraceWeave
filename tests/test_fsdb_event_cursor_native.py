"""Portable tests of the production raw-tick resume and sibling cleanup code."""
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.fixture(scope="module")
def cursor_probe(tmp_path_factory):
    compiler = shutil.which("g++")
    if compiler is None:
        pytest.skip("C++ compiler unavailable")
    root = Path(__file__).resolve().parents[1]
    executable = tmp_path_factory.mktemp("event_cursor") / "probe"
    subprocess.run([compiler, "-std=c++11", "-Wall", "-Wextra", "-Werror",
                    "-I", str(root / "native/fsdb"),
                    str(root / "tests/native/fsdb_event_cursor_test.cpp"),
                    "-o", str(executable)], check=True, capture_output=True, text=True)
    return executable


@pytest.mark.parametrize("scenario", [
    "positions", "nonzero_origin", "siblings", "throw_free", "throw_free_before",
    "unload", "throw_unload", "create", "second_create", "throw_create",
    "seek", "wrong_tick", "throw_seek", "get", "throw_get", "next", "throw_next",
])
def test_native_event_cursor_resume_and_cleanup(cursor_probe, scenario):
    subprocess.run([str(cursor_probe), scenario], check=True, timeout=10)
