from __future__ import annotations

"""Shared pytest environment bootstrap for Spark-based tests.

This file configures runtime prerequisites before tests import Spark code:
- JAVA_HOME for local Spark JVM startup.
- SPARK_CONF_DIR for repository Spark settings.
- PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON to enforce same Python executable
    on driver and workers (avoids version mismatch errors).
"""

import os
import re
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest


ROOT_DIR = Path(__file__).resolve().parents[1]
VENV_PYTHON = ROOT_DIR / ".venv" / "bin" / "python"

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def _java_major(java_home: Path) -> int | None:
    java_bin = java_home / "bin" / "java"
    if not java_bin.exists():
        return None
    try:
        out = subprocess.run(
            [str(java_bin), "-version"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    header = (out.stderr or out.stdout).splitlines()[0] if (out.stderr or out.stdout) else ""
    match = re.search(r'"(\d+)(?:\.\d+)?', header)
    if not match:
        return None
    return int(match.group(1))


def _pick_java_home() -> Path | None:
    current = os.environ.get("JAVA_HOME")
    if current:
        current_path = Path(current)
        major = _java_major(current_path)
        if major is not None and 11 <= major <= 21:
            return current_path

    candidates = [
        Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
        Path("/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"),
        Path("/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"),
    ]
    for c in candidates:
        major = _java_major(c)
        if major is not None and 11 <= major <= 21:
            return c

    try:
        proc = subprocess.run(
            ["/usr/libexec/java_home", "-v", "11+"],
            check=False,
            capture_output=True,
            text=True,
        )
        home = proc.stdout.strip()
        if home:
            c = Path(home)
            major = _java_major(c)
            if major is not None and 11 <= major <= 21:
                return c
    except OSError:
        pass

    return None


java_home = _pick_java_home()
if java_home:
    os.environ["JAVA_HOME"] = str(java_home)
    os.environ["PATH"] = f"{java_home / 'bin'}:{os.environ.get('PATH', '')}"
os.environ["SPARK_CONF_DIR"] = str(ROOT_DIR / "spark_conf")
os.environ.setdefault("SPARK_SQL_USE_ISOLATED_METASTORE", "1")
os.environ.setdefault("SPARK_SQL_METASTORE_RUN_ID", f"pytest_{uuid4().hex[:8]}")
# Always pin Spark worker and driver Python to the same interpreter as pytest.
# This avoids minor-version mismatch errors when .venv and system Python differ.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def root_dir() -> Path:
    """Expose repository root path to tests and other fixtures."""
    return ROOT_DIR
