# AGENTS.md — context for AI coding assistants (Cursor, Claude Code, Copilot)

## What this project is
Telco fraud detection accelerator on Databricks: synthetic source data → Auto Loader bronze → silver
(dedupe/DQ) → gold features + 7 configurable rules → gradient-boosting ML layer (MLflow, UC registry)
→ case queue + KPIs → drift monitoring → Streamlit Databricks App (executive / management / analyst
views + LLM investigation agent) → Genie space → optional Lakebase sync. Deployed as a Databricks
Asset Bundle (`databricks.yml`). Owner: XDT Studio.

## Layout
- `config/fraud_config.json` — single source of truth for rules, weights, thresholds, data size. Industry logic lives here.
- `src/fraudlib/` — pure, testable library. `datagen.py` (pandas/numpy), `features.py` (PySpark), `model.py` (sklearn).
- `notebooks/` — Databricks **source-format** notebooks (`# Databricks notebook source`, `# COMMAND ----------`, `# MAGIC`). Each starts with `%run ./_common` which defines `cfg`, `CATALOG`, `SCHEMA`, `VOLUME`, `T()`.
- `app/` — Databricks App. Reads via `databricks-sql-connector` using the app's service principal; writes only to `case_decisions`.
- `tests/test_local.py` — end-to-end local run on PySpark 4 + Java 17.

## Rules for changes
1. Business logic goes in `src/fraudlib/`, never inline in notebooks. Notebooks orchestrate and display.
2. No hard-coded catalog/schema/thresholds. Use `T("table")` and `cfg[...]`.
3. Keep notebooks in source format; don't convert to `.ipynb`.
4. New rule = add to `RULES` and `flags` in `features.py` + weight in config + a row in the 03 notebook's rule table.
5. New model feature = add to `FEATURE_COLS`; it must be computable from data available **before** the transaction (no leakage; `is_fraud` is a label, never a feature).
6. Time-based train/test split only. Never random split.
7. Final score stays `max(rule_score, ml_prob*100)` unless explicitly asked: ML may escalate, never suppress a rule hit.
8. Gold tables used by Lakebase keep Change Data Feed on and a NOT NULL primary key.
9. App SQL uses named parameters (`:p0`), never f-string user input into SQL.
10. Never put `msisdn` or `device_id` in LLM prompts or Genie answers.
11. After changing `src/` run `python tests/test_local.py`; it must print `OK`.

## Known soft spots
- `07_lakebase_sync.py` uses a Lakebase SDK surface that has changed across versions; verify against the installed `databricks-sdk`.
- Synthetic fraud is cleaner than real fraud; metrics are optimistic.
- Management forecast is a linear trend placeholder.
- Agent "RAG" is a stuffed `playbook.md`, not retrieval.

## Backlog (good next tasks)
- Convert 02–03 to a Lakeflow Declarative Pipeline with expectations.
- Model Serving endpoint for `fraud_gbt@champion` + online lookup from Lakebase.
- Agent with UC-function tools + Knowledge Assistant over real policy docs.
- Retraining from `case_decisions` labels; champion/challenger promotion in 06.
- Unit tests per rule (pytest) with hand-built edge cases.
