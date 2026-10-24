"""One hand-built case that should fire, and one that should not, for every rule."""
import json
import os
from datetime import datetime, timedelta

import pytest
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

from fraudlib.features import RULES, apply_rules, build_features

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CFG = json.load(open(os.path.join(ROOT, "config", "fraud_config.json")))
BASE = datetime(2026, 9, 10, 12, 0, 0)
MADRID = (40.4168, -3.7038)
LAGOS = (6.5244, 3.3792)

TXN = StructType([
    StructField("txn_id", StringType()),
    StructField("sub_id", LongType()),
    StructField("ts", TimestampType()),
    StructField("txn_type", StringType()),
    StructField("amount", DoubleType()),
    StructField("ip_country", StringType()),
    StructField("ip_lat", DoubleType()),
    StructField("ip_lon", DoubleType()),
    StructField("device_id", StringType()),
    StructField("is_emulator", BooleanType()),
])
CDR = StructType([
    StructField("sub_id", LongType()),
    StructField("ts", TimestampType()),
    StructField("cell_id", StringType()),
    StructField("cell_lat", DoubleType()),
    StructField("cell_lon", DoubleType()),
    StructField("country", StringType()),
    StructField("dest_number", StringType()),
    StructField("roaming", BooleanType()),
])
DEV = StructType([
    StructField("sub_id", LongType()),
    StructField("event_type", StringType()),
    StructField("ts", TimestampType()),
])
SUB = StructType([
    StructField("sub_id", LongType()),
    StructField("region", StringType()),
    StructField("home_city", StringType()),
    StructField("plan", StringType()),
    StructField("tenure_months", IntegerType()),
])


def _df(spark, rows, schema):
    return spark.createDataFrame(rows or [], schema)


def _txn(txn_id, sub_id, **kw):
    row = {
        "txn_id": txn_id, "sub_id": sub_id, "ts": BASE, "txn_type": "topup", "amount": 20.0,
        "ip_country": "ES", "ip_lat": MADRID[0], "ip_lon": MADRID[1],
        "device_id": f"dev-{sub_id}", "is_emulator": False,
    }
    row.update(kw)
    return row


def _cdr(sub_id, minutes_before, cell_id="MAD-001", lat=MADRID[0], lon=MADRID[1],
         country="ES", dest="+34600000000", roaming=False):
    return {
        "sub_id": sub_id, "ts": BASE - timedelta(minutes=minutes_before), "cell_id": cell_id,
        "cell_lat": lat, "cell_lon": lon, "country": country, "dest_number": dest, "roaming": roaming,
    }


def _subs(sub_ids):
    return [{"sub_id": s, "region": "Centro", "home_city": "Madrid", "plan": "postpaid_basic",
             "tenure_months": 24} for s in sub_ids]


def _flags(spark, txns, cdrs=None, devs=None):
    scored = apply_rules(build_features(
        _df(spark, txns, TXN), _df(spark, cdrs, CDR), _df(spark, devs, DEV),
        _df(spark, _subs(sorted({t["sub_id"] for t in txns})), SUB), CFG), CFG)
    return {r["txn_id"]: r.asDict() for r in scored.select("txn_id", *[f"r_{n}" for n in RULES]).collect()}


def _impossible_travel(spark):
    return _flags(spark, [
        _txn("fire", 1, ip_country="NG", ip_lat=LAGOS[0], ip_lon=LAGOS[1]),
        _txn("quiet", 2),
    ], [
        _cdr(1, 60, lat=MADRID[0], lon=MADRID[1], country="ES"),
        _cdr(2, 60),
    ])


def _cell_ip_mismatch(spark):
    return _flags(spark, [
        _txn("fire", 1, ip_country="FR"),
        _txn("quiet", 2, ip_country="ES"),
    ], [
        _cdr(1, 30, country="ES"),
        _cdr(2, 30, country="ES"),
    ])


def _rapid_tower_hops(spark):
    limit = CFG["rules"]["rapid_tower_hops"]["max_cells"]
    cdrs = [_cdr(1, i + 1, cell_id=f"HOP-{i}") for i in range(limit + 1)]
    cdrs += [_cdr(2, i + 1, cell_id=f"OK-{i}") for i in range(limit)]
    return _flags(spark, [_txn("fire", 1), _txn("quiet", 2)], cdrs)


def _roaming_premium(spark):
    need = CFG["rules"]["roaming_premium"]["min_premium_calls"]
    prefix = CFG["premium_prefixes"][0]
    cdrs = [_cdr(1, 60 * (i + 1), cell_id=f"RM-{i}", country="NG", lat=LAGOS[0], lon=LAGOS[1],
                 dest=f"{prefix}100000{i}", roaming=True) for i in range(need)]
    cdrs += [_cdr(2, 60, cell_id="RM-Q", country="NG", lat=LAGOS[0], lon=LAGOS[1],
                  dest=f"{prefix}1999999", roaming=True)]
    return _flags(spark, [
        _txn("fire", 1, ip_country="NG", ip_lat=LAGOS[0], ip_lon=LAGOS[1]),
        _txn("quiet", 2, ip_country="NG", ip_lat=LAGOS[0], ip_lon=LAGOS[1]),
    ], cdrs)


def _device_emulation(spark):
    return _flags(spark, [
        _txn("fire", 1, is_emulator=True),
        _txn("quiet", 2, is_emulator=False),
    ])


def _recent_sim_swap(spark):
    return _flags(spark, [_txn("fire", 1), _txn("quiet", 2)], devs=[
        {"sub_id": 1, "event_type": "sim_swap", "ts": BASE - timedelta(minutes=30)},
        {"sub_id": 2, "event_type": "device_change", "ts": BASE - timedelta(minutes=30)},
    ])


def _high_value_order(spark):
    minimum = float(CFG["rules"]["high_value_order"]["min_amount"])
    return _flags(spark, [
        _txn("fire", 1, txn_type="device_order", amount=minimum),
        _txn("quiet", 2, txn_type="topup", amount=minimum + 1000),
    ])


CASES = {
    "impossible_travel": _impossible_travel,
    "cell_ip_mismatch": _cell_ip_mismatch,
    "rapid_tower_hops": _rapid_tower_hops,
    "roaming_premium": _roaming_premium,
    "device_emulation": _device_emulation,
    "recent_sim_swap": _recent_sim_swap,
    "high_value_order": _high_value_order,
}


def test_case_builders_cover_every_rule():
    assert set(CASES) == set(RULES)


@pytest.mark.parametrize("rule", RULES)
def test_rule_positive_and_negative(spark, rule):
    flags = CASES[rule](spark)
    assert flags["fire"][f"r_{rule}"] is True
    assert flags["quiet"][f"r_{rule}"] is False
