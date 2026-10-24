"""Fraud Analyst Workbench — Databricks App (Streamlit).

Three role views over the same governed gold tables:
  Executive  : P&L impact, trend, regional risk, link to the Genie space
  Management : queue health, SLA breaches, naive 30-day forecast, engine performance
  Analyst    : case queue, case detail + account context, AI investigation agent, decision capture
"""
import json
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

CATALOG = os.getenv("FRAUD_CATALOG", "workspace")
SCHEMA = os.getenv("FRAUD_SCHEMA", "fraud")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
LLM_ENDPOINT = os.getenv("SERVING_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
GENIE_URL = os.getenv("GENIE_SPACE_URL", "")


def T(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"


st.set_page_config(page_title="Fraud Workbench", layout="wide")
cfg = Config()


@st.cache_resource
def conn():
    return sql.connect(server_hostname=cfg.host, http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
                       credentials_provider=lambda: cfg.authenticate)


@st.cache_data(ttl=60)
def q(query: str, params: tuple = ()) -> pd.DataFrame:
    with conn().cursor() as c:
        c.execute(query, {f"p{i}": v for i, v in enumerate(params)})
        return c.fetchall_arrow().to_pandas()


def execute(query: str, params: dict) -> None:
    with conn().cursor() as c:
        c.execute(query, params)


def user_email() -> str:
    return st.context.headers.get("X-Forwarded-Email", "unknown")


view = st.sidebar.radio("Role", ["Executive", "Management", "Analyst"])
st.sidebar.caption(f"Signed in as {user_email()}")

# ------------------------------------------------------------------ Executive
if view == "Executive":
    st.title("Fraud impact")
    k = q(f"SELECT * FROM {T('gold_daily_kpis')} ORDER BY day")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Loss prevented (€)", f"{k.loss_prevented_eur.sum():,.0f}")
    c2.metric("Loss leaked (€)", f"{k.loss_leaked_eur.sum():,.0f}")
    c3.metric("Catch rate", f"{k.fraud_caught.sum() / max(k.fraud_txns.sum(), 1):.1%}")
    c4.metric("Caught only by ML", int(k.fraud_caught_by_ml_only.sum()))
    st.line_chart(k.set_index("day")[["loss_prevented_eur", "loss_leaked_eur"]])
    st.subheader("Regional risk")
    st.dataframe(q(f"SELECT * FROM {T('gold_region_risk')} ORDER BY fraud_txns DESC"), hide_index=True,
                 use_container_width=True)
    if GENIE_URL:
        st.link_button("Ask Genie in plain language →", GENIE_URL)

# ------------------------------------------------------------------ Management
elif view == "Management":
    st.title("Queue & engine health")
    cases = q(f"""SELECT c.*, d.decision AS analyst_decision FROM {T('gold_fraud_cases')} c
                  LEFT JOIN (SELECT case_id, max_by(decision, decided_at) AS decision
                             FROM {T('case_decisions')} GROUP BY case_id) d USING (case_id)""")
    open_ = cases[cases.analyst_decision.isna()]
    now = pd.Timestamp(cases.opened_at.max())  # synthetic data: treat the latest event as "now"
    c1, c2, c3 = st.columns(3)
    c1.metric("Open cases", len(open_))
    c2.metric("SLA breached", int((open_.sla_due_at < now).sum()))
    c3.metric("P1 open", int((open_.priority == "P1").sum()))

    k = q(f"SELECT day, cases_review + cases_blocked AS cases FROM {T('gold_daily_kpis')} ORDER BY day")
    x = np.arange(len(k))
    slope, icpt = np.polyfit(x, k.cases.astype(float), 1)
    fut = pd.DataFrame({"day": pd.date_range(pd.Timestamp(k.day.max()) + pd.Timedelta(days=1), periods=30),
                        "forecast": np.maximum(icpt + slope * np.arange(len(k), len(k) + 30), 0)})
    st.subheader("Daily case volume + naive 30-day forecast")
    st.line_chart(pd.concat([k.set_index("day").rename(columns={"cases": "actual"}),
                             fut.set_index("day")]))
    st.caption("Linear trend; swap for ai_forecast() or a Prophet model for staffing decisions.")

    st.subheader("Engine performance (daily recall)")
    st.line_chart(q(f"SELECT day, recall_rules, recall_final FROM {T('gold_model_monitoring')} ORDER BY day")
                  .set_index("day"))

# ------------------------------------------------------------------ Analyst
else:
    st.title("Case queue")
    queue = q(f"""SELECT case_id, priority, opened_at, sla_due_at, final_score, txn_type, amount, ip_country,
                         rule_reasons, status
                  FROM {T('gold_fraud_cases')}
                  WHERE case_id NOT IN (SELECT case_id FROM {T('case_decisions')})
                  ORDER BY priority, final_score DESC LIMIT 200""")
    st.dataframe(queue, hide_index=True, use_container_width=True)
    if queue.empty:
        st.success("Queue empty.")
        st.stop()

    case_id = st.selectbox("Open case", queue.case_id)
    case = q(f"SELECT * FROM {T('gold_fraud_cases')} WHERE case_id = :p0", (case_id,)).iloc[0]
    txn = q(f"SELECT * FROM {T('gold_txn_scored')} WHERE txn_id = :p0", (case.txn_id,)).iloc[0]
    history = q(f"""SELECT ts, txn_type, amount, channel, ip_country, device_id, final_score, decision
                    FROM {T('gold_txn_scored')} WHERE sub_id = :p0 ORDER BY ts DESC LIMIT 15""",
                (int(case.sub_id),))

    left, right = st.columns([3, 2])
    with left:
        st.subheader(f"{case_id} · {case.priority} · score {case.final_score}")
        st.write({"subscriber": int(case.sub_id), "city": case.home_city, "plan": case.plan,
                  "txn": f"{case.txn_type} €{case.amount:,.2f} via {case.channel}",
                  "ip_country": case.ip_country, "rules_fired": list(case.rule_reasons),
                  "ml_prob": float(case.ml_prob), "mins_since_sim_swap": float(txn.mins_since_sim_swap),
                  "travel_kmh": round(float(txn.travel_kmh), 1), "distinct_cells_30m": int(txn.distinct_cells_30m),
                  "premium_roam_calls_24h": int(txn.premium_roam_calls_24h), "new_device": bool(txn.new_device_i)})
        st.subheader("Account history")
        st.dataframe(history, hide_index=True, use_container_width=True)

    with right:
        st.subheader("AI investigation agent")
        if st.button("Investigate"):
            playbook = open(os.path.join(os.path.dirname(__file__), "playbook.md")).read()
            prompt = (f"You are a telecom fraud investigator. Follow this playbook strictly:\n{playbook}\n\n"
                      f"CASE:\n{case.drop(['label_is_fraud']).to_json(default_handler=str)}\n\n"
                      f"FEATURES:\n{txn[['travel_km', 'travel_kmh', 'last_cell_country', 'mins_since_sim_swap', 'distinct_cells_30m', 'premium_roam_calls_24h', 'new_device_i', 'hour_of_day', 'is_emulator']].to_json(default_handler=str)}\n\n"
                      f"ACCOUNT HISTORY:\n{history.to_json(orient='records', date_format='iso')}\n\n"
                      'Reply ONLY with JSON: {"recommendation": "approve|deny|escalate", '
                      '"confidence": 0-1, "rationale": "<=80 words citing evidence and playbook section"}')
            try:
                resp = WorkspaceClient().serving_endpoints.query(
                    name=LLM_ENDPOINT, max_tokens=400, temperature=0.0,
                    messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)])
                raw = resp.choices[0].message.content.strip().removeprefix("```json").removesuffix("```")
                st.session_state[f"ai_{case_id}"] = json.loads(raw)
            except Exception as e:
                st.error(f"Agent call failed: {e}")
        ai = st.session_state.get(f"ai_{case_id}")
        if ai:
            st.info(f"**{ai['recommendation'].upper()}** (confidence {ai.get('confidence', '?')})\n\n{ai['rationale']}")

        st.subheader("Decision")
        with st.form("decide"):
            decision = st.radio("Outcome", ["approve", "deny", "escalate"], horizontal=True,
                                index=["approve", "deny", "escalate"].index(ai["recommendation"]) if ai else 0)
            notes = st.text_area("Notes (required for deny/escalate)")
            if st.form_submit_button("Submit"):
                if decision != "approve" and not notes.strip():
                    st.warning("Add a note: the playbook requires documentation for deny/escalate.")
                else:
                    execute(f"""INSERT INTO {T('case_decisions')}
                                VALUES (:cid, :ts, :who, :dec, :notes, :ai_rec, :ai_why)""",
                            {"cid": case_id, "ts": datetime.now(timezone.utc), "who": user_email(),
                             "dec": decision, "notes": notes,
                             "ai_rec": ai["recommendation"] if ai else None,
                             "ai_why": ai["rationale"] if ai else None})
                    q.clear()
                    st.success(f"{case_id} → {decision}")
                    st.rerun()
