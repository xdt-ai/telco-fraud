# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Drift monitoring
# MAGIC Fraudsters adapt as soon as they get caught, so recall decays. This notebook tracks daily recall
# MAGIC and precision of the rules vs the combined engine and flags days below `monitoring.recall_floor`.
# MAGIC Schedule it daily; when it fails the job, retrain (step 04) or retune rule weights in the config.
# MAGIC For production, also attach a Lakehouse Monitoring (data quality) monitor to `gold_txn_scored`.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F

floor = cfg["monitoring"]["recall_floor"]
flagged = lambda col: F.col(col) != "auto_approve"  # noqa: E731

mon = (spark.table(T("gold_txn_scored"))
       .groupBy(F.to_date("ts").alias("day"))
       .agg(F.sum("is_fraud").alias("fraud"),
            F.sum((flagged("rule_decision") & (F.col("is_fraud") == 1)).cast("int")).alias("tp_rules"),
            F.sum((flagged("decision") & (F.col("is_fraud") == 1)).cast("int")).alias("tp_final"),
            F.sum(flagged("decision").cast("int")).alias("alerts"))
       .withColumn("recall_rules", F.round(F.col("tp_rules") / F.col("fraud"), 3))
       .withColumn("recall_final", F.round(F.col("tp_final") / F.col("fraud"), 3))
       .withColumn("precision_final", F.round(F.col("tp_final") / F.col("alerts"), 3))
       .withColumn("breach", F.col("recall_final") < floor)
       .withColumn("checked_at", F.current_timestamp()))

mon.write.mode("overwrite").saveAsTable(T("gold_model_monitoring"))
display(spark.table(T("gold_model_monitoring")).orderBy("day"))

# COMMAND ----------

breaches = spark.table(T("gold_model_monitoring")).where("breach AND fraud >= 3").count()
if breaches:
    # Fails the job task -> job email/Slack notification fires.
    raise RuntimeError(f"{breaches} day(s) with recall below {floor}. Retrain or retune rules.")
print("Recall above floor on all days.")
