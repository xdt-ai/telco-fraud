# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the Unity Catalog catalog, schema and landing volume.
# MAGIC If you can't create catalogs (e.g. Free Edition / restricted metastore), set the `catalog`
# MAGIC widget to one you own (often `workspace`) and this notebook will only create the schema.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:  # no CREATE CATALOG privilege -> reuse an existing catalog
    print(f"Skipping catalog creation ({type(e).__name__}); using existing catalog {CATALOG}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'Telco fraud detection accelerator'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw COMMENT 'Landing zone for source extracts'")
display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}"))
