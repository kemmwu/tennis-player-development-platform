# Known Limitations

This document honestly lists what was not built or is below production
standard in this project. These are engineering maturity decisions,
not oversights.

## Data Limitations

**1. Small real data volume**
Only 4 real students with 9 sessions of SwingVision data.
The majority of data is synthetic. Real coaching insights are limited
to the 4 students with actual screenshots.

**2. Opponent UTR not captured**
SwingVision does not record opponent UTR. The opponent_strength_weight
in the development score defaults to 1.0 (neutral) for all matches.
The formula is correct but the data to power it is not yet available.

**3. No multi-tenancy**
The platform supports only one coach. A production version would need
per-coach data isolation, role-based access control, and separate
student namespaces.

## Engineering Limitations

**4. Schema naming inconsistency**
dbt models land in silver_gold instead of gold due to profile
configuration. Manually created tables are in gold. Both schemas
are in use. See Decision 13.

**5. No real-time streaming volume**
The Kafka + Lakeflow Connect pipeline is correctly configured but
has processed only a handful of test submissions. It has not been
stress-tested at production event volume.

**6. HITL loop not fully closed**
The extraction eval set exists and corrections are saved. However
the monthly accuracy evaluation notebook has not been run with
enough approved corrections to produce meaningful accuracy metrics.

**7. Streamlit on Community Cloud**
The coach app and HITL app are hosted on Streamlit Community Cloud
(free tier) rather than Databricks Apps as originally designed.
Databricks Apps would provide tighter integration with Unity Catalog
and better security for production use.

## What Would Be Different in Production

- Multi-tenancy with per-coach catalogs
- Databricks Apps instead of Streamlit Community Cloud
- Automated prompt improvement based on eval set accuracy trends
- Mobile-friendly coach interface for on-court use
- Integration with tournament scheduling systems for automatic
  match date population
