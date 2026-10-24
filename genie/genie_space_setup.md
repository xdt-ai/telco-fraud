# Genie space: "Telco Fraud"

Create it in the UI: **Genie → New → choose a SQL warehouse → add these tables** (all in `workspace.fraud`):

- `gold_daily_kpis`, `gold_region_risk`, `gold_fraud_cases`, `gold_txn_scored`, `gold_model_monitoring`, `case_decisions`

## General instructions (paste into the space)

```
You answer questions about telecom fraud for executives and fraud managers.
- "Fraud" means is_fraud = 1 (gold_txn_scored) or label_is_fraud = 1 (gold_fraud_cases).
- "Caught" means decision IN ('review','auto_block').
- Money is EUR. loss_prevented_eur / loss_leaked_eur already include the 3.4x loaded cost; do not multiply again.
- "SIM swap cases" = rows where array_contains(rule_reasons, 'recent_sim_swap').
- "IRSF" = array_contains(rule_reasons, 'roaming_premium').
- Quarter = calendar quarter of ts / day. Default to the latest 30 days when no period is given.
- Never expose msisdn or device_id in answers.
```

## Sample questions (add as examples, then verify the generated SQL)

1. Which regions had the most SIM-swap cases this quarter?
2. How much loss did we prevent last week vs the week before?
3. What share of fraud was caught only by the ML model?
4. How many open P1 cases are past their SLA?
5. Which rule has the worst precision?
6. On which days did recall drop below 0.7?

## Trusted query example (add under "SQL queries")

```sql
-- Which regions had the most SIM-swap cases this quarter?
SELECT region, count(*) AS sim_swap_cases
FROM workspace.fraud.gold_fraud_cases
WHERE array_contains(rule_reasons, 'recent_sim_swap')
  AND opened_at >= date_trunc('quarter', (SELECT max(opened_at) FROM workspace.fraud.gold_fraud_cases))
GROUP BY region ORDER BY sim_swap_cases DESC;
```
