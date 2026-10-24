# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · (Optional) Lakebase: serve gold features at OLTP latency
# MAGIC Removes the "copy gold back to the operational DB" batch job: a Lakebase (managed Postgres)
# MAGIC instance gets **continuous synced tables** fed from the Delta gold tables via Change Data Feed.
# MAGIC The real-time scorer and the app then do point lookups by key in Postgres.
# MAGIC
# MAGIC **Needs:** a workspace with Lakebase enabled (not available on every tier / Free Edition).
# MAGIC Skip this step otherwise — the app works off the SQL warehouse.
# MAGIC
# MAGIC The Lakebase SDK surface has been moving; if a call below doesn't match your `databricks-sdk`
# MAGIC version, do the same thing in the UI: *Compute → Lakebase → Create instance*, then on each gold
# MAGIC table *Create → Synced table* (mode: **Continuous**).

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk psycopg[binary]
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (DatabaseInstance, SyncedDatabaseTable, SyncedTableSchedulingPolicy,
                                             SyncedTableSpec)

w = WorkspaceClient()
INSTANCE = "telco-fraud-oltp"
PG_CATALOG = f"{CATALOG}_pg"  # UC catalog that mirrors the Postgres database

if INSTANCE not in [i.name for i in w.database.list_database_instances()]:
    w.database.create_database_instance_and_wait(DatabaseInstance(name=INSTANCE, capacity="CU_1"))
print(w.database.get_database_instance(INSTANCE).state)

# COMMAND ----------

from databricks.sdk.service.database import DatabaseCatalog

try:
    w.database.create_database_catalog(DatabaseCatalog(name=PG_CATALOG, database_instance_name=INSTANCE,
                                                       database_name="fraud", create_database_if_not_exists=True))
except Exception as e:
    print(f"catalog: {e}")

SYNCS = {"gold_subscriber_risk_profile": ["sub_id"], "gold_fraud_cases": ["case_id"]}
for table, pk in SYNCS.items():
    try:
        w.database.create_synced_database_table(SyncedDatabaseTable(
            name=f"{PG_CATALOG}.public.{table}",
            spec=SyncedTableSpec(source_table_full_name=T(table), primary_key_columns=pk,
                                 scheduling_policy=SyncedTableSchedulingPolicy.CONTINUOUS)))
        print(f"sync created: {table}")
    except Exception as e:
        print(f"{table}: {e}")

# COMMAND ----------

# MAGIC %md ### Point lookup from Postgres — what a real-time scorer would call per transaction

# COMMAND ----------

import uuid

import psycopg

inst = w.database.get_database_instance(INSTANCE)
cred = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[INSTANCE])
user = spark.sql("SELECT current_user()").first()[0]

with psycopg.connect(host=inst.read_write_dns, dbname="fraud", user=user, password=cred.token,
                     sslmode="require") as conn:
    row = conn.execute('SELECT * FROM public.gold_subscriber_risk_profile WHERE sub_id = %s', (42,)).fetchone()
    print(row)
