"""Risk engine: per-transaction features, rule flags, score and routing (PySpark)."""
from __future__ import annotations

import re

from pyspark.sql.window import Window

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

RULES = ["impossible_travel", "cell_ip_mismatch", "rapid_tower_hops", "roaming_premium",
         "device_emulation", "recent_sim_swap", "high_value_order"]
FEATURE_COLS = ["amount", "travel_km", "travel_kmh", "distinct_cells_30m", "premium_roam_calls_24h",
                "mins_since_sim_swap", "cdr_count_24h", "is_emulator_i", "ip_foreign_i", "tenure_months",
                "new_device_i", "hour_of_day"]


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    dlat, dlon = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return F.lit(2 * 6371.0) * F.asin(F.sqrt(a))


def build_features(txn: DataFrame, cdr: DataFrame, dev: DataFrame, subs: DataFrame, cfg: dict) -> DataFrame:
    r, home = cfg["rules"], cfg["home_country"]
    travel_s = r["impossible_travel"]["lookback_min"] * 60
    hops_s = r["rapid_tower_hops"]["window_min"] * 60
    roam_s = r["roaming_premium"]["window_hours"] * 3600
    swap_s = r["recent_sim_swap"]["window_hours"] * 3600
    horizon = max(travel_s, hops_s, roam_s)
    prem_rx = "^(" + "|".join(re.escape(p) for p in cfg["premium_prefixes"]) + ")"

    t = txn.withColumn("txn_secs", F.unix_timestamp("ts"))
    # first time this device is seen on the account (and not the account's very first txn)
    first_dev = F.min("ts").over(Window.partitionBy("sub_id", "device_id"))
    first_sub = F.min("ts").over(Window.partitionBy("sub_id"))
    t = (t.withColumn("new_device_i", ((F.col("ts") == first_dev) & (F.col("ts") > first_sub)).cast("int"))
          .withColumn("hour_of_day", F.hour("ts")))

    # --- CDR context: range join on sub_id within the look-back horizon ---
    c = cdr.select("sub_id", F.unix_timestamp("ts").alias("cdr_secs"), "cell_id", "cell_lat",
                   "cell_lon", "country", "dest_number", "roaming")
    j = (t.select("txn_id", "sub_id", "txn_secs").join(c, "sub_id")
         .where((F.col("cdr_secs") <= F.col("txn_secs")) &
                (F.col("cdr_secs") >= F.col("txn_secs") - horizon))
         .withColumn("dt", F.col("txn_secs") - F.col("cdr_secs")))
    in_travel = F.col("dt") <= travel_s
    last = F.struct("cdr_secs", "cell_lat", "cell_lon", "country")
    cdr_ctx = j.groupBy("txn_id").agg(
        F.max_by(F.when(in_travel, last), F.when(in_travel, F.col("cdr_secs"))).alias("last_cdr"),
        F.countDistinct(F.when(F.col("dt") <= hops_s, F.col("cell_id"))).alias("distinct_cells_30m"),
        F.sum(F.when((F.col("dt") <= roam_s) & F.col("roaming") & F.col("dest_number").rlike(prem_rx), 1)
              .otherwise(0)).alias("premium_roam_calls_24h"),
        F.sum(F.when(F.col("dt") <= 86400, 1).otherwise(0)).alias("cdr_count_24h"),
    )

    # --- SIM swaps in the window before the transaction ---
    swaps = (t.select("txn_id", "sub_id", "txn_secs")
             .join(dev.where(F.col("event_type") == "sim_swap")
                   .select("sub_id", F.unix_timestamp("ts").alias("swap_secs")), "sub_id")
             .where(F.col("swap_secs").between(F.col("txn_secs") - swap_s, F.col("txn_secs")))
             .groupBy("txn_id")
             .agg((F.min(F.col("txn_secs") - F.col("swap_secs")) / 60).alias("mins_since_sim_swap")))

    f = (t.join(cdr_ctx, "txn_id", "left").join(swaps, "txn_id", "left")
         .join(subs.select("sub_id", "region", "home_city", "plan", "tenure_months"), "sub_id", "left")
         .fillna({"distinct_cells_30m": 0, "premium_roam_calls_24h": 0, "cdr_count_24h": 0}))

    hours = F.greatest((F.col("txn_secs") - F.col("last_cdr.cdr_secs")) / 3600, F.lit(1 / 60))
    f = (f.withColumn("travel_km", F.coalesce(
            haversine_km(F.col("last_cdr.cell_lat"), F.col("last_cdr.cell_lon"), F.col("ip_lat"), F.col("ip_lon")),
            F.lit(0.0)))
          .withColumn("travel_kmh", F.when(F.col("last_cdr").isNotNull(), F.col("travel_km") / hours).otherwise(0.0))
          .withColumn("last_cell_country", F.col("last_cdr.country"))
          .withColumn("is_emulator_i", F.col("is_emulator").cast("int"))
          .withColumn("ip_foreign_i", (F.col("ip_country") != home).cast("int"))
          .withColumn("mins_since_sim_swap", F.coalesce("mins_since_sim_swap", F.lit(99999.0))))

    flags = {
        "impossible_travel": (F.col("travel_kmh") > r["impossible_travel"]["max_kmh"]) & (F.col("travel_km") > 100),
        "cell_ip_mismatch": F.col("last_cell_country").isNotNull() & (F.col("last_cell_country") != F.col("ip_country")),
        "rapid_tower_hops": F.col("distinct_cells_30m") > r["rapid_tower_hops"]["max_cells"],
        "roaming_premium": F.col("premium_roam_calls_24h") >= r["roaming_premium"]["min_premium_calls"],
        "device_emulation": F.col("is_emulator"),
        "recent_sim_swap": F.col("mins_since_sim_swap") <= swap_s / 60,
        "high_value_order": (F.col("txn_type") == "device_order") & (F.col("amount") >= r["high_value_order"]["min_amount"]),
    }
    for name, cond in flags.items():
        f = f.withColumn(f"r_{name}", F.coalesce(cond, F.lit(False)))
    return f.drop("last_cdr", "txn_secs")


def apply_rules(f: DataFrame, cfg: dict) -> DataFrame:
    r = cfg["rules"]
    score = sum(F.col(f"r_{n}").cast("int") * r[n]["weight"] for n in RULES)
    reasons = F.filter(F.array(*[F.when(F.col(f"r_{n}"), F.lit(n)) for n in RULES]), lambda x: x.isNotNull())
    return (f.withColumn("rule_score", F.least(score, F.lit(100)))
             .withColumn("rule_reasons", reasons))


def route(score_col: str, cfg: dict) -> Column:
    rt = cfg["routing"]
    return (F.when(F.col(score_col) >= rt["block_at"], "auto_block")
             .when(F.col(score_col) >= rt["review_at"], "review")
             .otherwise("auto_approve"))
