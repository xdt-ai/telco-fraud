# Real-time fraud detection for telecom operators — client brief

## The problem, in one paragraph
A phone number is now the key to a customer's bank, email and two-factor codes. A fraudster who
takes over an account (often via a SIM swap) can order expensive devices and drain linked accounts
within minutes. Most operators detect this in overnight batches, using signals scattered across
6–7 systems that don't talk to each other. By the time the alert fires, the money is gone.

## What we build
One platform that brings network, SIM, CRM and digital-channel data together and scores every
transaction as it happens:

1. **Collects** call records, SIM events, account changes and orders into one governed place.
2. **Checks** each transaction against clear, auditable rules (e.g. "SIM swapped 2 hours ago and the
   order comes from another country").
3. **Adds a machine-learning model** that catches patterns the rules miss, without overriding them.
4. **Routes** each transaction: approve automatically, send to an analyst, or block.
5. **Gives analysts a workbench** with the full case context and an AI assistant that recommends a
   decision and explains why, based on your own fraud playbook. The analyst always decides.
6. **Lets managers ask questions in plain language** ("Which regions had the most SIM-swap cases this
   quarter?") and get answers from governed data.
7. **Watches itself:** daily checks warn when detection quality drops, because fraudsters adapt fast.

## What you get
- Working pipeline in your Databricks workspace, with code you own.
- Rules and thresholds in one configuration file your team can tune.
- Dashboards for executives (€ prevented / leaked), managers (queue, SLA) and analysts (cases).
- Audit trail of every decision, human and AI.

## What we need from you
- Access to sample extracts: call records, SIM/device events, CRM changes, orders.
- Historical confirmed fraud cases (even a few hundred) to train and measure the model.
- Your current fraud policy / playbook.
- One fraud analyst and one data owner, a few hours per week.

## How we'd run it
| Phase | Duration | Outcome |
|---|---|---|
| 1. Discovery & data check | 1–2 weeks | Data map, gaps, success metrics agreed |
| 2. Pilot on your data | 3–4 weeks | Rules + model running on history; measured vs current system |
| 3. Workbench & AI assistant | 2–3 weeks | Analysts using it on a subset of cases |
| 4. Hardening | 2+ weeks | Monitoring, retraining loop, handover |

## Honest limits
- The demo uses synthetic data and looks better than reality will. Real results depend on your data quality and how quickly confirmed fraud labels arrive.
- This **augments** your existing fraud system; it doesn't replace it on day one.
- The AI assistant recommends; it does not decide. Humans stay accountable.
- Some components (e.g. the real-time database layer) depend on your Databricks tier.

---

## Talking points for the meeting (internal — remove before sending)
- **Open with their pain, not the tech:** "How long between a SIM swap and your first alert today?"
- **Demo order:** Executive view (€) → one blocked case in the Analyst view → click *Investigate* → Genie question. Five minutes, no code.
- **Why one platform:** no nightly copy job between analytics and operations, one governance model, one audit trail.
- **Why rules + ML:** rules are explainable to compliance; ML catches what rules miss; ML can never quietly un-flag a rule hit.
- **Likely objections:**
  - *"We already have a fraud system."* → Good, keep it. We cover the gaps: cross-channel correlation and fast adaptation.
  - *"AI making decisions?"* → It doesn't. It drafts a recommendation; every decision is logged with who made it.
  - *"GDPR?"* → Data stays in your workspace under Unity Catalog; phone numbers and device IDs never go to the LLM.
  - *"Cost?"* → Start with a fixed-scope pilot on historical data; decide on production only with measured results.
- **Don't promise** specific detection rates before seeing their data.
