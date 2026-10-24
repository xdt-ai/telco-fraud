# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: features + rule-based risk engine
# MAGIC For every transaction, joins the network (CDR), SIM and CRM context into one row, fires the
# MAGIC configured rules and routes it: `auto_approve` / `review` / `auto_block`.
# MAGIC
# MAGIC | Rule | Signal | Catches |
# MAGIC |---|---|---|
# MAGIC | impossible_travel | last cell → IP location speed > max_kmh | ATO from abroad |
# MAGIC | cell_ip_mismatch | last cell country ≠ IP country | ATO, proxying |
# MAGIC | rapid_tower_hops | > N distinct cells in 30 min | SIM box / cloned SIM |
# MAGIC | roaming_premium | premium-rate calls while roaming | IRSF |
# MAGIC | device_emulation | emulator flag | bot / scripted takeover |
# MAGIC | recent_sim_swap | SIM swap < 24 h before | SIM-swap fraud |
# MAGIC | high_value_order | device order ≥ threshold | device fraud |

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F

from fraudlib.features import apply_rules, build_features, route

feat = build_features(spark.table(T("silver_transactions")), spark.table(T("silver_cdr")),
                      spark.table(T("silver_device_events")), spark.table(T("silver_subscribers")), cfg)
risk = apply_rules(feat, cfg).withColumn("rule_decision", route("rule_score", cfg))

(risk.write.mode("overwrite").option("overwriteSchema", "true")
     .option("delta.enableChangeDataFeed", "true").saveAsTable(T("gold_txn_risk")))
spark.sql(f"ALTER TABLE {T('gold_txn_risk')} ALTER COLUMN txn_id SET NOT NULL")
spark.sql(f"COMMENT ON TABLE {T('gold_txn_risk')} IS "
          "'One row per transaction with network/SIM/CRM features, rule flags (r_*), rule_score 0-100 and rule_decision.'")

# COMMAND ----------

# MAGIC %md ### How good are the rules alone? (labels are known because the data is synthetic)

# COMMAND ----------

g = spark.table(T("gold_txn_risk"))
display(g.groupBy("rule_decision").agg(F.count("*").alias("txns"), F.sum("is_fraud").alias("fraud"),
                                       F.round(F.avg("is_fraud") * 100, 2).alias("fraud_pct")))
display(g.select(F.explode("rule_reasons").alias("rule"), "is_fraud").groupBy("rule")
         .agg(F.count("*").alias("fired"), F.round(F.avg("is_fraud") * 100, 1).alias("precision_pct"))
         .orderBy(F.desc("fired")))

# COMMAND ----------

# MAGIC %md ### Online profile per subscriber (what the real-time engine looks up; synced to Lakebase in step 07)

# COMMAND ----------

from pyspark.sql.window import Window

w = Window.partitionBy("sub_id").orderBy(F.col("ts").desc())
profile = (g.withColumn("rn", F.row_number().over(w)).where("rn = 1")
            .select("sub_id", "region", "plan", "tenure_months",
                    F.col("ts").alias("last_txn_ts"), "last_cell_country", "mins_since_sim_swap",
                    "distinct_cells_30m", "premium_roam_calls_24h", F.col("rule_score").alias("last_rule_score")))
(profile.write.mode("overwrite").option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true").saveAsTable(T("gold_subscriber_risk_profile")))
spark.sql(f"ALTER TABLE {T('gold_subscriber_risk_profile')} ALTER COLUMN sub_id SET NOT NULL")
