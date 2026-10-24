# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate synthetic source extracts
# MAGIC Simulates four source systems (CRM, network CDRs, device/SIM platform, digital channels) dropping
# MAGIC parquet files into the landing volume. Each run writes a new batch, so Auto Loader in step 02 picks
# MAGIC up only new files. Change `seed` in the config to get a different batch.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import time

from fraudlib.datagen import generate

data = generate(cfg)
batch = time.strftime("%Y%m%d_%H%M%S")
for name, pdf in data.items():
    path = f"{VOLUME}/{name}"
    os.makedirs(path, exist_ok=True)
    pdf.to_parquet(f"{path}/batch_{batch}.parquet", index=False)
    print(f"{name:15s} {len(pdf):>10,} rows -> {path}")

# COMMAND ----------

t = data["transactions"]
print(f"Fraud transactions injected: {t.is_fraud.sum():,} of {len(t):,} ({t.is_fraud.mean():.2%})")
