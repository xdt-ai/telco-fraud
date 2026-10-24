# Fraud Operations Playbook (demo)

## 1. Deny immediately
- SIM swap < 6 h before a device order AND IP country differs from last cell country.
- Rapid tower hops (> 4 cells / 30 min) combined with a new device.
- Premium-rate calls while roaming (IRSF) on an account with no travel history.

## 2. Escalate to Tier 2
- Order ≥ €800 with exactly one strong signal (SIM swap, impossible travel, emulator) and no history of that behaviour.
- Any case where the AI agent's confidence < 0.6.
- Postpaid premium customers with tenure > 5 years (relationship risk): never deny without Tier 2.

## 3. Approve
- Foreign IP that matches the roaming cell country (customer travelling).
- High-value order from a known device, domestic IP, no SIM swap in 24 h.

## 4. Documentation
- Deny and escalate require a written note citing the evidence.
- Escalations: SLA 4 h from case open. P1 cases are worked first.
