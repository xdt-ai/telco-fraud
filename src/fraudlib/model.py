"""ML layer on top of the rules: gradient boosting on engineered features + rule flags."""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, precision_score, recall_score

from .features import FEATURE_COLS, RULES

MODEL_COLS = FEATURE_COLS + [f"r_{r}" for r in RULES]


def to_matrix(pdf: pd.DataFrame) -> pd.DataFrame:
    X = pdf[MODEL_COLS].copy()
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce").astype("float64")
    return X.fillna(0.0)


def time_split(pdf: pd.DataFrame, train_days: int):
    cutoff = pdf["ts"].min().normalize() + pd.Timedelta(days=train_days)
    return pdf[pdf["ts"] < cutoff], pdf[pdf["ts"] >= cutoff], cutoff


def train(train_pdf: pd.DataFrame) -> HistGradientBoostingClassifier:
    clf = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
                                         class_weight="balanced", random_state=42)
    return clf.fit(to_matrix(train_pdf), train_pdf["is_fraud"].astype(int))


def final_score(rule_score: np.ndarray, ml_prob: np.ndarray) -> np.ndarray:
    """ML can escalate a transaction but never silently override a rule hit."""
    return np.maximum(rule_score, np.round(ml_prob * 100)).astype(int)


def evaluate(y: np.ndarray, rule_score: np.ndarray, ml_prob: np.ndarray, cfg: dict) -> dict:
    review_at = cfg["routing"]["review_at"]
    rules_flag = rule_score >= review_at
    final_flag = final_score(rule_score, ml_prob) >= review_at
    return {
        "rules_recall": recall_score(y, rules_flag, zero_division=0),
        "rules_precision": precision_score(y, rules_flag, zero_division=0),
        "rules_alerts": int(rules_flag.sum()),
        "ml_pr_auc": average_precision_score(y, ml_prob),
        "final_recall": recall_score(y, final_flag, zero_division=0),
        "final_precision": precision_score(y, final_flag, zero_division=0),
        "final_alerts": int(final_flag.sum()),
    }
