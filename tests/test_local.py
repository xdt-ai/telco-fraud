"""Local end-to-end smoke test: datagen -> Spark features/rules -> ML. Run: python tests/test_local.py"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from fraudlib.datagen import generate  # noqa: E402
from fraudlib.features import apply_rules, build_features, route  # noqa: E402
from fraudlib.model import evaluate, time_split, to_matrix, train  # noqa: E402

cfg = json.load(open(os.path.join(ROOT, "config", "fraud_config.json")))
cfg["data_gen"]["n_subscribers"] = 4000

spark = (SparkSession.builder.master("local[4]").config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
d = generate(cfg)
sdf = {k: spark.createDataFrame(v) for k, v in d.items()}

risk = apply_rules(build_features(sdf["transactions"], sdf["cdr"], sdf["device_events"],
                                  sdf["subscribers"], cfg), cfg).withColumn("rule_decision", route("rule_score", cfg))
assert risk.count() == len(d["transactions"]), "row count changed in feature build"
risk.groupBy("rule_decision").agg(F.count("*").alias("n"), F.sum("is_fraud").alias("fraud")).show()
(risk.select(F.explode("rule_reasons").alias("rule"), "is_fraud").groupBy("rule")
     .agg(F.count("*").alias("fired"), F.round(F.avg("is_fraud"), 3).alias("precision")).show())

pdf = risk.drop("rule_reasons").toPandas().sort_values("txn_id").reset_index(drop=True)
tr, te, cutoff = time_split(pdf, cfg["ml"]["train_days"])
clf = train(tr)
prob = clf.predict_proba(to_matrix(te))[:, 1]
m = evaluate(te["is_fraud"].values, te["rule_score"].values, prob, cfg)
print("cutoff", cutoff, "train", len(tr), "test", len(te), "test fraud", int(te.is_fraud.sum()))
print(json.dumps({k: round(v, 3) for k, v in m.items()}, indent=1))
assert m["rules_recall"] > 0.5 and m["final_recall"] >= m["rules_recall"]
print("OK")
