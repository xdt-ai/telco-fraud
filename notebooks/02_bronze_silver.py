# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze → Silver
# MAGIC **Bronze:** Auto Loader ingests landing files incrementally (`availableNow`), adding lineage columns.
# MAGIC **Silver:** typed, de-duplicated, validated tables with Change Data Feed on (needed later for
# MAGIC continuous Lakebase sync).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCES = ["subscribers", "cdr", "transactions", "device_events"]

for src in SOURCES:
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{VOLUME}/_schemas/{src}")
        .load(f"{VOLUME}/{src}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .writeStream
        .option("checkpointLocation", f"{VOLUME}/_checkpoints/bronze_{src}")
        .trigger(availableNow=True)
        .toTable(T(f"bronze_{src}"))
        .awaitTermination())
    print(f"bronze_{src}: {spark.table(T(f'bronze_{src}')).count():,} rows")

# COMMAND ----------

# MAGIC %md ### Silver: keep latest copy of each key, enforce basic quality rules

# COMMAND ----------

SILVER = {
    # table: (primary key, quality filter)
    "subscribers": ("sub_id", "sub_id IS NOT NULL AND msisdn IS NOT NULL"),
    "cdr": ("cdr_id", "sub_id IS NOT NULL AND ts IS NOT NULL AND duration_s > 0"),
    "transactions": ("txn_id", "sub_id IS NOT NULL AND ts IS NOT NULL AND amount >= 0"),
    "device_events": ("event_id", "sub_id IS NOT NULL AND ts IS NOT NULL"),
}

for name, (pk, rule) in SILVER.items():
    bronze = spark.table(T(f"bronze_{name}"))
    bad = bronze.where(f"NOT ({rule})").count()
    latest = Window.partitionBy(pk).orderBy(F.col("_ingested_at").desc())
    silver = (bronze.where(rule)
              .withColumn("_rn", F.row_number().over(latest)).where("_rn = 1")
              .drop("_rn", "_source_file"))
    (silver.write.mode("overwrite").option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true").saveAsTable(T(f"silver_{name}")))
    print(f"silver_{name}: {spark.table(T(f'silver_{name}')).count():,} rows ({bad} rejected)")
