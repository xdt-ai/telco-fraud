# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Case queue + KPI tables
# MAGIC * `gold_fraud_cases` — one case per transaction routed to `review` or `auto_block`, with priority and SLA.
# MAGIC * `case_decisions` — written by the analyst workbench app (append-only audit log).
# MAGIC * `gold_daily_kpis`, `gold_region_risk` — feed the executive/management views and Genie.
# MAGIC
# MAGIC Financial impact uses the ~3.4× fully-loaded cost per fraud dollar (chargebacks, replacements,
# MAGIC investigation overhead) cited in the source article — change `LOSS_MULTIPLIER` to your own figure.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

LOSS_MULTIPLIER = 3.4
sla_h = cfg["routing"]["review_sla_hours"]

spark.sql(f"""
CREATE OR REPLACE TABLE {T('gold_fraud_cases')}
TBLPROPERTIES (delta.enableChangeDataFeed = true) AS
SELECT
  concat('C', substr(txn_id, 2))                       AS case_id,
  txn_id, sub_id, region, home_city, plan, txn_type, channel, amount, ip_country,
  ts                                                    AS opened_at,
  ts + make_interval(0, 0, 0, 0, {sla_h}, 0, 0)         AS sla_due_at,
  decision                                              AS engine_decision,
  final_score, rule_score, round(ml_prob, 4)            AS ml_prob, rule_reasons,
  CASE WHEN final_score >= 85 THEN 'P1' WHEN final_score >= 60 THEN 'P2' ELSE 'P3' END AS priority,
  CASE WHEN decision = 'auto_block' THEN 'blocked_pending_confirm' ELSE 'open' END     AS status,
  is_fraud                                              AS label_is_fraud
FROM {T('gold_txn_scored')}
WHERE decision IN ('review', 'auto_block')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T('case_decisions')} (
  case_id STRING NOT NULL, decided_at TIMESTAMP, analyst STRING,
  decision STRING COMMENT 'approve | deny | escalate', notes STRING,
  ai_recommendation STRING, ai_rationale STRING
) TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Append-only analyst decisions from the workbench app'
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {T('gold_daily_kpis')} AS
SELECT
  date(ts)                                                         AS day,
  count(*)                                                         AS txns,
  sum(CASE WHEN decision = 'review' THEN 1 ELSE 0 END)             AS cases_review,
  sum(CASE WHEN decision = 'auto_block' THEN 1 ELSE 0 END)         AS cases_blocked,
  sum(is_fraud)                                                    AS fraud_txns,
  sum(CASE WHEN is_fraud = 1 AND decision <> 'auto_approve' THEN 1 ELSE 0 END) AS fraud_caught,
  sum(CASE WHEN is_fraud = 0 AND decision <> 'auto_approve' THEN 1 ELSE 0 END) AS false_positives,
  round(sum(CASE WHEN is_fraud = 1 AND decision <> 'auto_approve' THEN amount ELSE 0 END) * {LOSS_MULTIPLIER}, 2) AS loss_prevented_eur,
  round(sum(CASE WHEN is_fraud = 1 AND decision =  'auto_approve' THEN amount ELSE 0 END) * {LOSS_MULTIPLIER}, 2) AS loss_leaked_eur,
  sum(CASE WHEN ml_only_catch AND is_fraud = 1 THEN 1 ELSE 0 END)  AS fraud_caught_by_ml_only
FROM {T('gold_txn_scored')}
GROUP BY date(ts)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {T('gold_region_risk')} AS
SELECT region, home_city,
  count(*) AS txns,
  sum(is_fraud) AS fraud_txns,
  sum(CASE WHEN array_contains(rule_reasons, 'recent_sim_swap') THEN 1 ELSE 0 END) AS sim_swap_alerts,
  sum(CASE WHEN array_contains(rule_reasons, 'roaming_premium') THEN 1 ELSE 0 END) AS irsf_alerts,
  round(avg(final_score), 2) AS avg_score
FROM {T('gold_txn_scored')}
GROUP BY region, home_city
""")

for t, c in {"gold_fraud_cases": "Analyst case queue, one row per flagged transaction",
             "gold_daily_kpis": "Daily fraud KPIs: volumes, catch rate, false positives, EUR impact (3.4x loaded cost)",
             "gold_region_risk": "Fraud and alert counts by region and city"}.items():
    spark.sql(f"COMMENT ON TABLE {T(t)} IS '{c}'")

display(spark.table(T("gold_daily_kpis")).orderBy("day"))
