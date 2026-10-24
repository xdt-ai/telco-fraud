"""Synthetic telco data: subscribers, CDRs, device events, transactions.

Pure numpy/pandas so it can be unit-tested off-cluster. Fraud is injected as
account-takeover chains: SIM swap -> (tower hops | premium roaming calls) ->
high-value order from a foreign IP, sometimes from an emulated device.
Legit noise (travellers, legit SIM swaps, big legit orders) creates realistic
false positives for the rule engine.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

CITIES = {  # name: (lat, lon, region)
    "Madrid": (40.4168, -3.7038, "Centro"),
    "Barcelona": (41.3874, 2.1686, "Nordeste"),
    "Valencia": (39.4699, -0.3763, "Este"),
    "Sevilla": (37.3891, -5.9845, "Sur"),
    "Zaragoza": (41.6488, -0.8891, "Nordeste"),
    "Bilbao": (43.2630, -2.9350, "Norte"),
    "Malaga": (36.7213, -4.4214, "Sur"),
}
CITY_P = [0.30, 0.20, 0.12, 0.10, 0.10, 0.10, 0.08]
FRAUD_ORIGINS = {"NG": (6.5244, 3.3792), "RO": (44.4268, 26.1025),
                 "BR": (-23.5505, -46.6333), "VN": (21.0278, 105.8342)}
TRAVEL_DEST = {"GB": (51.5072, -0.1276), "FR": (48.8566, 2.3522), "DE": (52.5200, 13.4050)}
TXN_TYPES = ["topup", "plan_change", "address_change", "device_order", "sim_swap_request"]
TXN_P = [0.45, 0.20, 0.10, 0.20, 0.05]
CHANNELS = ["app", "web", "store", "call_center"]


def _rand_ts(rng, start, days, size):
    return start + pd.to_timedelta(rng.uniform(0, days * 86400, size), unit="s")


def generate(cfg: dict) -> dict[str, pd.DataFrame]:
    g = cfg["data_gen"]
    rng = np.random.default_rng(g["seed"])
    n, days = g["n_subscribers"], g["n_days"]
    start = pd.Timestamp(g["start_date"])
    premium = cfg["premium_prefixes"]

    # ---------- subscribers ----------
    city = rng.choice(list(CITIES), n, p=CITY_P)
    lat = np.array([CITIES[c][0] for c in city])
    lon = np.array([CITIES[c][1] for c in city])
    subs = pd.DataFrame({
        "sub_id": np.arange(1, n + 1),
        "msisdn": [f"+346{x:08d}" for x in rng.integers(0, 10**8, n)],
        "home_city": city,
        "region": [CITIES[c][2] for c in city],
        "home_lat": lat, "home_lon": lon,
        "plan": rng.choice(["prepaid", "postpaid_basic", "postpaid_premium"], n, p=[.3, .5, .2]),
        "tenure_months": rng.integers(1, 120, n),
        "activated_at": start - pd.to_timedelta(rng.integers(30, 3600, n), unit="D"),
    })
    is_fraud_victim = rng.random(n) < g["fraud_rate"]
    is_traveller = (~is_fraud_victim) & (rng.random(n) < 0.03)

    # ---------- baseline CDRs ----------
    per_sub = days * g["cdr_per_sub_per_day"]
    idx = np.repeat(np.arange(n), per_sub)
    m = idx.size
    cdr = pd.DataFrame({
        "sub_id": idx + 1,
        "ts": _rand_ts(rng, start, days, m),
        "cell_lat": lat[idx] + rng.normal(0, 0.04, m),
        "cell_lon": lon[idx] + rng.normal(0, 0.04, m),
        "country": "ES",
        "dest_number": np.where(rng.random(m) < 0.97,
                                [f"+34{x:09d}" for x in rng.integers(6 * 10**8, 10**9, m)],
                                rng.choice(["+33612345678", "+447700900123", "+4915112345678"], m)),
        "duration_s": rng.exponential(120, m).astype(int) + 1,
    })
    cdr["cell_id"] = [f"{c[:3].upper()}-{k:03d}" for c, k in
                      zip(city[idx], rng.integers(0, 60, m))]

    # travellers: 3-6 days abroad, CDRs roam there (no fraud, but foreign IPs)
    trips = {}
    for s in np.where(is_traveller)[0]:
        dest = rng.choice(list(TRAVEL_DEST))
        t0 = start + pd.Timedelta(days=int(rng.integers(0, days - 6)))
        t1 = t0 + pd.Timedelta(days=int(rng.integers(3, 7)))
        trips[s + 1] = (dest, t0, t1)
        sel = (cdr.sub_id == s + 1) & (cdr.ts >= t0) & (cdr.ts < t1)
        dlat, dlon = TRAVEL_DEST[dest]
        k = sel.sum()
        cdr.loc[sel, "country"] = dest
        cdr.loc[sel, "cell_lat"] = dlat + rng.normal(0, .03, k)
        cdr.loc[sel, "cell_lon"] = dlon + rng.normal(0, .03, k)
        cdr.loc[sel, "cell_id"] = [f"{dest}-R{x:03d}" for x in rng.integers(0, 40, k)]

    # ---------- baseline transactions ----------
    n_txn = rng.poisson(g["txn_per_sub"], n)
    tidx = np.repeat(np.arange(n), n_txn)
    k = tidx.size
    ttype = rng.choice(TXN_TYPES, k, p=TXN_P)
    amount = np.select(
        [ttype == "topup", ttype == "device_order", ttype == "plan_change"],
        [rng.choice([10, 15, 20, 30, 50], k), rng.gamma(2.0, 250, k).round(2), rng.uniform(5, 40, k).round(2)],
        0.0)
    txn = pd.DataFrame({
        "sub_id": tidx + 1,
        "ts": _rand_ts(rng, start, days, k),
        "txn_type": ttype,
        "amount": amount,
        "channel": rng.choice(CHANNELS, k, p=[.45, .3, .15, .1]),
        "ip_country": "ES",
        "ip_lat": lat[tidx] + rng.normal(0, .05, k),
        "ip_lon": lon[tidx] + rng.normal(0, .05, k),
        "device_id": [f"dev-{s + 1}" if r > 0.03 else f"dev-n{s + 1}-{i}" for i, (s, r) in enumerate(zip(tidx, rng.random(k)))],
        "is_emulator": rng.random(k) < 0.003,
        "is_fraud": 0,
    })
    # travellers transact from abroad during the trip
    for sid, (dest, t0, t1) in trips.items():
        sel = (txn.sub_id == sid) & (txn.ts >= t0) & (txn.ts < t1)
        dlat, dlon = TRAVEL_DEST[dest]
        txn.loc[sel, ["ip_country", "ip_lat", "ip_lon"]] = [dest, dlat, dlon]

    # ---------- baseline device events ----------
    legit_swaps = np.where((~is_fraud_victim) & (rng.random(n) < 0.02))[0]
    dev = pd.DataFrame({
        "sub_id": np.concatenate([legit_swaps + 1, rng.integers(1, n + 1, n // 2)]),
        "event_type": ["sim_swap"] * len(legit_swaps) + ["device_change"] * (n // 2),
        "channel": "store",
    })
    dev["ts"] = _rand_ts(rng, start, days, len(dev))

    # ---------- fraud injection (account-takeover chains) ----------
    f_cdr, f_txn, f_dev = [], [], []
    for s in np.where(is_fraud_victim)[0]:
        sid = s + 1
        tf = start + pd.Timedelta(seconds=float(rng.uniform(2 * 86400, (days - 1) * 86400)))
        if rng.random() < 0.6:  # takeovers skew to night-time, when victims are asleep
            tf = tf.normalize() + pd.Timedelta(hours=float(rng.uniform(0, 6)))
        if rng.random() < 0.75:  # not every takeover goes through a SIM swap
            f_dev.append({"sub_id": sid, "event_type": "sim_swap",
                          "channel": rng.choice(["call_center", "web"]),
                          "ts": tf - pd.Timedelta(minutes=float(rng.uniform(10, 300)))})
        if rng.random() < 0.5:  # rapid tower hops (SIM-box / cloned SIM pattern)
            for j in range(6):
                f_cdr.append({"sub_id": sid, "ts": tf - pd.Timedelta(minutes=25 - 4 * j),
                              "cell_lat": lat[s] + rng.normal(0, .08), "cell_lon": lon[s] + rng.normal(0, .08),
                              "country": "ES", "dest_number": f"+34{rng.integers(6e8, 1e9)}",
                              "duration_s": int(rng.integers(5, 40)), "cell_id": f"HOP-{sid}-{j}"})
        if rng.random() < 0.35:  # IRSF: premium-rate calls while roaming
            oc = rng.choice(list(FRAUD_ORIGINS))
            for j in range(3):
                f_cdr.append({"sub_id": sid, "ts": tf - pd.Timedelta(hours=float(rng.uniform(1, 20))),
                              "cell_lat": FRAUD_ORIGINS[oc][0], "cell_lon": FRAUD_ORIGINS[oc][1],
                              "country": oc, "dest_number": f"{rng.choice(premium)}{rng.integers(1e6, 1e7)}",
                              "duration_s": int(rng.integers(600, 3600)), "cell_id": f"{oc}-R{j:03d}"})
        foreign = rng.random() < 0.55
        oc = rng.choice(list(FRAUD_ORIGINS))
        ilat, ilon = FRAUD_ORIGINS[oc] if foreign else (lat[s] + rng.normal(0, .05), lon[s] + rng.normal(0, .05))
        for j, (tt, amt) in enumerate([("address_change", 0.0),
                                       ("device_order", float(rng.uniform(400, 1800)))]):
            f_txn.append({"sub_id": sid, "ts": tf + pd.Timedelta(minutes=3 * j), "txn_type": tt,
                          "amount": round(amt, 2), "channel": rng.choice(["web", "app"]),
                          "ip_country": oc if foreign else "ES", "ip_lat": ilat, "ip_lon": ilon,
                          "device_id": f"dev-x{rng.integers(1e6)}", "is_emulator": bool(rng.random() < 0.3),
                          "is_fraud": 1})

    cdr = pd.concat([cdr, pd.DataFrame(f_cdr)], ignore_index=True)
    txn = pd.concat([txn, pd.DataFrame(f_txn)], ignore_index=True)
    dev = pd.concat([dev, pd.DataFrame(f_dev)], ignore_index=True)

    cdr["roaming"] = cdr["country"] != cfg["home_country"]
    cdr = cdr.sort_values("ts").reset_index(drop=True)
    cdr.insert(0, "cdr_id", np.arange(1, len(cdr) + 1))
    txn = txn.sort_values("ts").reset_index(drop=True)
    txn.insert(0, "txn_id", [f"T{i:08d}" for i in range(1, len(txn) + 1)])
    dev = dev.sort_values("ts").reset_index(drop=True)
    dev.insert(0, "event_id", np.arange(1, len(dev) + 1))
    for df in (cdr, txn, dev):
        df["ts"] = pd.to_datetime(df["ts"]).astype("datetime64[us]")
    subs["activated_at"] = subs["activated_at"].astype("datetime64[us]")
    txn["is_emulator"] = txn["is_emulator"].astype(bool)
    return {"subscribers": subs, "cdr": cdr, "transactions": txn, "device_events": dev}
