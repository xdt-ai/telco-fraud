# Databricks notebook source
# Shared setup: widgets, config, import path, table-name helper. Pulled in with %run ./_common
import json
import os
import sys

dbutils.widgets.text("catalog", "telco_fraud")
dbutils.widgets.text("schema", "fraud")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))  # notebooks/ -> project root
if os.path.join(ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(ROOT, "src"))

with open(os.path.join(ROOT, "config", "fraud_config.json")) as fh:
    cfg = json.load(fh)

VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/raw"


def T(name: str) -> str:
    """Fully qualified UC table name."""
    return f"{CATALOG}.{SCHEMA}.{name}"


print(f"catalog={CATALOG} schema={SCHEMA} industry={cfg['industry']}")
