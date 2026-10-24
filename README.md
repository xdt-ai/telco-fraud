# Telco Fraud Detection on Databricks — step by step

A from-scratch build of the pattern in Databricks' *Fraud Analyst AI Coworker* post (Oct 2026):
medallion pipeline → rule-based risk engine → ML layer → case queue → analyst workbench app with an
AI investigation agent → Genie for plain-language questions → optional Lakebase for OLTP-speed lookups.
Everything is driven by `config/fraud_config.json`, so the same skeleton retargets to fintech/insurance.

```
config/fraud_config.json      rules, weights, thresholds, data-gen size
src/fraudlib/                 datagen.py (pandas) · features.py (PySpark rules) · model.py (sklearn)
notebooks/00–07               the pipeline, one notebook per step
app/                          Streamlit Databricks App + playbook.md for the agent
genie/genie_space_setup.md    tables, instructions, sample + trusted queries
tests/test_local.py           runs the whole engine locally on PySpark
databricks.yml                Asset Bundle: 7-task job + the app
```

## Architecture

```
 CRM ─┐                                                    ┌─ gold_txn_scored ──┐
 CDRs ─┼─▶ Volume (parquet) ─▶ bronze_* ─▶ silver_* ─▶ features + rules ─▶ ML ─┤                    ├─▶ App (3 views + AI agent)
 SIM  ─┤   (Auto Loader)      lineage    dedupe/DQ    gold_txn_risk     @champion└─ gold_fraud_cases ──┤─▶ Genie space
 Web  ─┘                                              │                           gold_daily_kpis    └─▶ Lakebase (synced, optional)
                                                       └─ gold_subscriber_risk_profile ─────────────────▶ Lakebase (online lookup)
```

Routing: `final_score = max(rule_score, ml_prob × 100)` → `<30 auto_approve`, `30–69 review`, `≥70 auto_block`.

---

## Step 0 — Prerequisites
- Databricks workspace with Unity Catalog + serverless (Free Edition works for steps 1–6; Lakebase needs a paid tier).
- Databricks CLI ≥ 0.250: `databricks auth login --host https://<your-workspace>`
- A SQL warehouse ID (Compute → SQL Warehouses → your warehouse → copy ID).
- A chat endpoint. Default is `databricks-meta-llama-3-3-70b-instruct` (pay-per-token). Check *Serving* for what your workspace has.

## Step 1 — Test locally (optional, 1 min)
```bash
python -m venv .venv && . .venv/bin/activate
pip install pyspark==4.0.1 scikit-learn pandas pyarrow   # needs Java 17+
python tests/test_local.py
```
Expected on 4k subscribers: rules ≈ 93% recall / 70% precision; rules + ML ≈ 100% recall with similar precision.

## Step 2 — Deploy the bundle
```bash
databricks bundle validate --var="warehouse_id=<id>"
databricks bundle deploy   --var="warehouse_id=<id>"
```
No permission to create catalogs? Add `--var="catalog=workspace"` and change `FRAUD_CATALOG` in `app/app.yaml`.

## Step 3 — Run the pipeline
```bash
databricks bundle run fraud_pipeline --var="warehouse_id=<id>"
```
| Task | What it does | Output |
|---|---|---|
| 00_setup | catalog, schema, landing volume | `raw` volume |
| 01_generate_data | 20k subscribers, ~1.8M CDRs, ~100k transactions, ~2% injected ATO chains | parquet batches |
| 02_bronze_silver | Auto Loader (`availableNow`) → bronze with lineage; silver dedupe + DQ, CDF on | `bronze_*`, `silver_*` |
| 03_gold_risk_engine | range-join CDR/SIM context per txn, 7 rules, score, route | `gold_txn_risk`, `gold_subscriber_risk_profile` |
| 04_train_score_model | time-split GBT, MLflow, UC registry `@champion`, batch score | `fraud_gbt`, `gold_txn_scored` |
| 05_cases_and_kpis | case queue with SLA/priority, decisions log, KPI tables | `gold_fraud_cases`, `case_decisions`, `gold_daily_kpis`, `gold_region_risk` |
| 06_monitoring | daily recall rules vs final; **fails the job** if below floor | `gold_model_monitoring` |

Re-run step 01 with a new `seed` to simulate a new day's drop; Auto Loader only ingests new files.

## Step 4 — Grant the app's service principal access
Find it under *Compute → Apps → telco-fraud-workbench → Authorization*, then in a SQL editor:
```sql
GRANT USE CATALOG ON CATALOG telco_fraud TO `<app-sp-id>`;
GRANT USE SCHEMA, SELECT ON SCHEMA telco_fraud.fraud TO `<app-sp-id>`;
GRANT MODIFY ON TABLE telco_fraud.fraud.case_decisions TO `<app-sp-id>`;
```

## Step 5 — Start the app
```bash
databricks bundle run fraud_workbench --var="warehouse_id=<id>"
```
- **Executive:** € prevented/leaked, catch rate, ML-only catches, regional risk, Genie link.
- **Management:** open cases, SLA breaches, naive 30-day forecast, daily recall.
- **Analyst:** queue → case detail + account history → **Investigate** (LLM reads case, features, history and `playbook.md`, returns approve/deny/escalate + rationale) → decision written to `case_decisions` with analyst email and the AI's recommendation (audit trail for AI-vs-human agreement).

## Step 6 — Genie space
Follow `genie/genie_space_setup.md`, then paste the space URL into `GENIE_SPACE_URL` in `app/app.yaml` and redeploy.

## Step 7 — Lakebase (optional, paid tier)
Run `notebooks/07_lakebase_sync.py` interactively. It creates a Postgres instance, continuous synced
tables from `gold_subscriber_risk_profile` and `gold_fraud_cases` (via CDF), and does a point lookup.
That's the piece that kills the "copy gold back to the OLTP DB every night" job. The SDK surface for
Lakebase has been changing — if a call errors, do the same in the UI (noted in the notebook).

---

## Where to take it next
1. **Streaming:** turn 02–03 into a Lakeflow Declarative Pipeline with `trigger(processingTime)` so scoring is continuous.
2. **Real-time scoring:** serve `fraud_gbt@champion` on Model Serving with online feature lookup from Lakebase.
3. **Proper RAG agent:** replace the stuffed `playbook.md` with Agent Bricks Knowledge Assistant over your real policy docs, and give the agent tools (UC functions) instead of a pre-built context.
4. **Feedback loop:** use `case_decisions` as labels for retraining — that's the monitor → tune → redeploy loop the article keeps hammering.
5. **Honesty note for clients:** the synthetic fraud is clean by design. Real labels are late, sparse and noisy; expect much lower precision and plan for it.
