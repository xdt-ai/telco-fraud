# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · ML layer: train, register, score
# MAGIC Gradient boosting on the engineered features + rule flags, trained on the first `train_days`
# MAGIC and tested on the rest (time split, never random — fraud drifts). Logged to MLflow and registered
# MAGIC in Unity Catalog with the alias `champion`.
# MAGIC
# MAGIC **Final score = max(rule_score, ML probability × 100).** ML can escalate what rules miss,
# MAGIC but can't quietly suppress a rule hit — easier to defend to compliance.
# MAGIC
# MAGIC In production labels arrive late (chargebacks, customer claims, analyst decisions). Here the
# MAGIC synthetic `is_fraud` stands in for them.

# COMMAND ----------

# MAGIC %pip install -q "scikit-learn>=1.4" "mlflow>=2.16"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import mlflow
import pandas as pd
from mlflow.models import infer_signature
from pyspark.sql import functions as F

from fraudlib.features import route
from fraudlib.model import MODEL_COLS, evaluate, final_score, time_split, to_matrix, train

mlflow.set_registry_uri("databricks-uc")
MODEL_NAME = T("fraud_gbt")

pdf = spark.table(T("gold_txn_risk")).select("txn_id", "ts", "is_fraud", "rule_score", *MODEL_COLS).toPandas()
tr, te, cutoff = time_split(pdf, cfg["ml"]["train_days"])
print(f"train < {cutoff:%Y-%m-%d}: {len(tr):,} rows ({tr.is_fraud.sum()} fraud) | test: {len(te):,} ({te.is_fraud.sum()} fraud)")

# COMMAND ----------

user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/telco_fraud")

with mlflow.start_run(run_name="gbt_rules_plus_features") as run:
    clf = train(tr)
    prob = clf.predict_proba(to_matrix(te))[:, 1]
    metrics = evaluate(te["is_fraud"].values, te["rule_score"].values, prob, cfg)
    mlflow.log_params({"train_cutoff": str(cutoff.date()), "n_features": len(MODEL_COLS), **cfg["routing"]})
    mlflow.log_metrics(metrics)
    X_sample = to_matrix(tr.head(200))
    info = mlflow.sklearn.log_model(clf, artifact_path="model", input_example=X_sample.head(5),
                                    signature=infer_signature(X_sample, clf.predict_proba(X_sample)[:, 1]),
                                    registered_model_name=MODEL_NAME)

display(pd.DataFrame([metrics]).T.rename(columns={0: "value"}))

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()
client.set_registered_model_alias(MODEL_NAME, "champion", info.registered_model_version)
print(f"{MODEL_NAME} v{info.registered_model_version} -> @champion")

# COMMAND ----------

# MAGIC %md ### Batch-score every transaction with the champion and write `gold_txn_scored`

# COMMAND ----------

champion = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@champion")
pdf["ml_prob"] = champion.predict_proba(to_matrix(pdf))[:, 1]
pdf["final_score"] = final_score(pdf["rule_score"].values, pdf["ml_prob"].values)
pdf["model_version"] = int(info.registered_model_version)

scores = spark.createDataFrame(pdf[["txn_id", "ml_prob", "final_score", "model_version"]])
scored = (spark.table(T("gold_txn_risk")).join(scores, "txn_id")
          .withColumn("decision", route("final_score", cfg))
          .withColumn("ml_only_catch", (F.col("rule_decision") == "auto_approve") & (F.col("decision") != "auto_approve")))
(scored.write.mode("overwrite").option("overwriteSchema", "true")
       .option("delta.enableChangeDataFeed", "true").saveAsTable(T("gold_txn_scored")))

display(spark.table(T("gold_txn_scored")).groupBy("decision")
        .agg(F.count("*").alias("txns"), F.sum("is_fraud").alias("fraud"), F.sum(F.col("ml_only_catch").cast("int")).alias("ml_only")))
