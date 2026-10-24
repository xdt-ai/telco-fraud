# Cursor prompts: telco-fraud project

Paste these one at a time in Cursor's chat (Agent mode). Wait for each to finish before the next.
Cursor reads `AGENTS.md` and `.cursor/rules/` automatically, but prompt 0 makes sure.

---

## 0. Onboarding (always first)
```
Read AGENTS.md, README.md and config/fraud_config.json. Don't change anything yet.
Summarise in 10 lines: the data flow from notebooks/01 to app/app.py, where the business
logic lives, and the 3 weakest parts of the project.
```

## 1. Local environment
```
Create a Python 3.11 venv with pyspark==4.0.1, scikit-learn, pandas, pyarrow, pytest.
Check Java 17+ is installed. Run tests/test_local.py and show me the metrics table.
If it fails, fix the environment, not the code.
```

## 2. Unit tests per rule
```
Add tests/test_rules.py with pytest. For each rule in RULES (src/fraudlib/features.py),
build a tiny hand-made Spark DataFrame with one case that should fire and one that
shouldn't. Use a session-scoped SparkSession fixture in tests/conftest.py.
Run pytest and fix only the tests, unless you find a real bug. If you do, explain it first.
```

## 3. Explain the code (to learn it / prepare for the client)
```
Explain src/fraudlib/features.py block by block, as if to a data analyst who knows
PySpark basics. For each rule: the business fraud it targets, the Spark logic, and
one way a fraudster could evade it. Write the result to docs/rules_explained.md.
```

## 4. Retarget to another industry
```
Propose (don't apply) how to adapt config/fraud_config.json and src/fraudlib for a
[fintech | insurance | e-commerce] client: which rules stay, which change, which new
signals are needed, and what source data the client must provide.
Answer as a table. After I approve, implement it in a new config file, not by overwriting.
```

## 5. Lakeflow Declarative Pipeline
```
Implement backlog item 1 from AGENTS.md: create pipelines/fraud_dlt.py that rebuilds
notebooks 02 and 03 as a Lakeflow Declarative Pipeline (bronze/silver/gold) with
expectations for the silver quality rules. Reuse src/fraudlib, don't copy logic.
Add the pipeline to databricks.yml as a resource. Don't delete the old notebooks.
```

## 6. Model Serving
```
Implement backlog item 2: add a Model Serving endpoint for fraud_gbt@champion to
databricks.yml, and a notebook 08_realtime_scoring.py that sends 5 sample transactions
and compares the endpoint's answers with gold_txn_scored. Keep the scoring formula.
```

## 7. Real agent with tools
```
Implement backlog item 3: replace the stuffed playbook prompt in app/app.py with an
agent that calls Unity Catalog SQL functions as tools (get_case, get_account_history,
get_rule_definitions). Create the functions in notebooks/09_agent_tools.py.
Never pass msisdn or device_id to the LLM. Keep the human decision step.
```

## 8. Retraining loop
```
Implement backlog item 4: in notebook 04, use case_decisions (deny = 1, approve = 0)
as extra labels when they exist. In notebook 06, add champion/challenger: train a
challenger, promote it to @champion only if its PR-AUC on the latest 7 days is higher.
Log both to MLflow.
```

## 9. Review before a client demo
```
Review the whole repo as a senior Databricks engineer would before a client demo.
List problems by severity: security (SQL injection, PII in prompts, secrets), things
that will break on a fresh workspace, and anything that looks amateur.
Don't fix anything; give me the list with file and line.
```

## 10. Client materials
```
Using docs/client_brief.md and the code, write docs/client_brief_de.md (German, Sie-form)
and docs/client_brief_fr.md (French, vouvoiement) for DACH/Francophone telco clients.
Plain business language, no code. Remove the internal "Talking points" section.
```

---

### Tips
- **Agent mode** for prompts that change files, **Ask mode** for prompts 0, 3 and 9.
- After every change: `python tests/test_local.py` must print `OK`.
- Commit after each prompt that works (`git commit -m "step N: ..."`), so you can roll back.
- If Cursor wants to change the scoring formula `max(rule_score, ml_prob*100)`, say no unless you asked.
