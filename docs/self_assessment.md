# Self-Assessment Report

**Project:** Tennis Player Development Platform
**Period:** January 2026 – May 2026
**Status:** In progress

---

## What Was Built

A production-grade Lakehouse platform on Databricks Premium that:
- Ingests 3 data sources via batch (Auto Loader) and streaming
  (Lakeflow Connect + Kafka)
- Extracts structured statistics from unstructured SwingVision
  screenshots using Claude API Vision
- Transforms data through a full Medallion architecture (Bronze →
  Silver → Gold) managed by dbt with 50+ automated tests
- Surfaces insights via a Streamlit coach dashboard with
  LLM-generated session recommendations
- Detects semantic inconsistencies between coaching notes and
  match statistics using an LLM quality auditor

---

## Metrics

| Metric | Target | Actual | Status |
|---|---|---|---|
| LLM extraction field-level accuracy | >90% | TBD (eval set too small) | 🟡 Pending |
| Pipeline success rate | >95% | ~100% (4/4 sessions) | ✅ |
| dbt tests passing | 100% | 100% (50+ tests) | ✅ |
| dbt full build time | <5 min | ~90s | ✅ |
| Real stakeholder feedback | 3 parents | 0 (not yet collected) | 🔴 Pending |

---

## What Went Well

1. **AI extraction pipeline** — Claude API Vision reliably extracts
   structured statistics from SwingVision screenshots with >0.85
   confidence on all 4 real sessions.

2. **dbt Silver/Gold layer** — All 13 models pass 50+ automated
   tests. The Player Development Score formula is fully documented
   and defensible.

3. **End-to-end streaming** — Typeform → Make.com → Confluent Kafka
   → Lakeflow Connect → Databricks works end-to-end. One real
   student intake form submission processed.

4. **Domain expertise integration** — Coaching knowledge is embedded
   in model design: 7-day note-to-match linkage window, development
   score component weights, SwingVision filename convention.

---

## What I Would Do Differently

1. **Write data contracts earlier** — Refactored Silver layer
   mid-project when Bronze schema changed. Contracts from day one
   would have prevented this.

2. **Set dbt target schema to neutral value** — silver_gold naming
   inconsistency caused confusion throughout Gold layer development.

3. **Collect real stakeholder feedback earlier** — Dashboard was
   built before showing it to parents. Earlier feedback would have
   changed the metrics displayed.

---

## Known Limitations

See `/docs/known_limitations.md` for full list.

---

## Monthly Databricks Cost

Estimated: <$5/month at current data volume on Serverless tier.
Full cost breakdown to be added after 30 days of operation.
