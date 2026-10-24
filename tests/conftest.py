"""Shared fixtures. test_local.py is a script, not a pytest module."""
import os
import sys

import pytest
from pyspark.sql import SparkSession

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

# Workers must use this interpreter. On Windows, `python` on PATH is the Store stub.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

collect_ignore = ["test_local.py"]


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("telco-fraud-rules")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.adaptive.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
