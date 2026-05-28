# 🎾 Tennis Player Development Platform
### An AI-Augmented Analytics Engineering Project on Databricks

[![dbt CI](https://github.com/kemmwu/tennis-player-development-platform/actions/workflows/dbt-ci.yml/badge.svg)](https://github.com/kemmwu/tennis-player-development-platform/actions/workflows/dbt-ci.yml)
[![Python CI](https://github.com/kemmwu/tennis-player-development-platform/actions/workflows/python-ci.yml/badge.svg)](https://github.com/kemmwu/tennis-player-development-platform/actions/workflows/python-ci.yml)

> **One-line pitch:** An end-to-end Lakehouse platform built by a certified tennis coach, using AI to process unstructured SwingVision data and help coaches improve student performance through data-driven decisions.

**Target roles:** Analytics Engineer · Data Engineer · AI Engineer (data platform track)

**[Coach Dashboard](https://tennis-player-development-platform-rvgsojwbcmybstkqcdnbjz.streamlit.app/)** · **[dbt Docs](#)**

---

## Table of Contents

1. [Project Positioning](#1-project-positioning)
2. [Tech Stack](#2-tech-stack)
3. [Architecture Overview](#3-architecture-overview)
4. [Repository Structure](#4-repository-structure)
5. [Data Sources & Ingestion Strategy](#5-data-sources--ingestion-strategy)
6. [Medallion Layer Design](#6-medallion-layer-design)
7. [AI / LLM Integration](#7-ai--llm-integration)
8. [dbt Project](#8-dbt-project)
9. [Production Engineering Practices](#9-production-engineering-practices)
10. [Databricks Setup](#10-databricks-setup)
11. [Local Development Setup](#11-local-development-setup)
12. [Design Decisions](#12-design-decisions)
13. [Known Limitations](#13-known-limitations)
14. [Performance & Cost Log](#14-performance--cost-log)
15. [Resume Bullet Points](#15-resume-bullet-points)
16. [Interview Narrative](#16-interview-narrative)

---

## 1. Project Positioning

### The Problem

Independent tennis coaches have no systematic way to track student development over time. Match statistics live in SwingVision screenshots (images), coaching observations live in handwritten or typed notes (unstructured text), and student intake data lives in forms. None of it is connected, none of it is queryable, and none of it compounds into insight over weeks and months.

### The Solution

A production-grade Lakehouse platform on Databricks that:
- Extracts structured statistics from SwingVision screenshots using Claude API Vision
- Parses coaching notes into structured observations using Claude API text extraction
- Ingests student intake events in real time via Kafka + Lakeflow Connect
- Transforms everything through a Medallion architecture (Bronze → Silver → Gold) managed by dbt
- Surfaces a Player Development Score that combines win rate trends, opponent strength, technique progression, and break point conversion
- Powers a Streamlit coach dashboard with LLM-generated next session recommendations

### Five Core Selling Points

**1. Domain Expertise × Data Engineering — dual identity**
Built by a certified tennis coach whose students have competed at the USTA Sectional level. Every modeling decision — the 7-day note-to-match linkage window, the development score component weights, the SwingVision filename convention — is grounded in real coaching logic, not guesswork.

**2. Diverse data sources covering all ingestion patterns**
- Unstructured images (SwingVision screenshots) → Multimodal LLM extraction
- Unstructured text (coaching notes embedded in screenshots) → Text LLM structured extraction
- Structured form data (student intake questionnaire) → Lakeflow Connect + Kafka event-driven streaming
- Synthetic historical data → batch loading for volume and demo scale

**3. AI as a tool, not a gimmick**
Claude API solves real data engineering problems: unstructured-to-structured conversion, semantic quality auditing (notes vs. stats consistency), and natural language querying via Databricks Genie. This is the AI-augmented data platform narrative that employers want in 2026.

**4. Production engineering practices**
Medallion architecture · dbt layered modeling · Auto Loader · Lakeflow Connect · data quality testing (50+ dbt tests) · GitHub Actions CI/CD · Databricks Workflows orchestration · Lakehouse Monitoring · Unity Catalog lineage · Human-in-the-Loop review · data contracts.

**5. Real data, real students**
9 real SwingVision sessions from 4 real students (yi, jeffrey, darren, garret). The Streamlit coach dashboard is actively used for coaching decisions.

---

## 2. Tech Stack

This project runs on **Databricks Premium** and is built to maximise Databricks-native tooling, with VS Code as the local development environment and GitHub for all version control and CI/CD.

| Layer | Tool | Purpose |
|---|---|---|
| **IDE** | VS Code + Databricks Extension | Local development, notebook sync, Git integration |
| **Version Control** | GitHub | All code, dbt models, prompts, notebooks |
| **Storage & Compute** | Databricks Premium | Core Lakehouse platform |
| **Workspace** | `dbc-66b56d97-276e.cloud.databricks.com` | Databricks workspace |
| **Catalog** | Unity Catalog (`tennis_dev`) | Bronze, silver, gold schemas |
| **Tables** | Delta Lake | ACID transactions, time travel, schema evolution |
| **File Storage** | Databricks Volumes | Raw files: screenshots, CSV, Parquet |
| **Batch Ingestion** | PySpark notebooks + pandas | Synthetic data ingestion from Volumes |
| **Real Data Ingestion** | Auto Loader + PySpark | SwingVision screenshot metadata ingestion |
| **Streaming Ingestion** | Lakeflow Connect + Confluent Kafka | Real-time Typeform intake events |
| **Transformation** | dbt Core 1.11 + dbt-databricks 1.12 | Silver & Gold layer modeling |
| **AI / LLM** | Claude API (`claude-sonnet-4-6`) | Extract structured stats from screenshots |
| **Data Quality** | dbt tests + dbt-expectations | 50+ automated quality gates |
| **CI/CD** | GitHub Actions | dbt parse + slim CI on every PR |
| **Orchestration** | Databricks Workflows | Full DAG scheduling with SLA alerts |
| **HITL Review** | Streamlit Community Cloud | Human review of low-confidence AI extractions |
| **Coach Dashboard** | Streamlit Community Cloud | Student development view for coaches |
| **NL Queries** | Databricks Genie | Natural language querying on Gold layer |
| **Observability** | Databricks Lakehouse Monitoring | Data quality drift, freshness, volume anomalies |
| **Lineage** | Unity Catalog | Column-level lineage |
| **Intake Form** | Typeform | Student onboarding questionnaire |
| **Event Bus** | Confluent Kafka | Typeform → Databricks event streaming |
| **Webhook Automation** | Make.com | Typeform webhook → Kafka bridge |

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                               │
├──────────────────┬──────────────────────┬───────────────────────────┤
│ SwingVision      │ Coaching Notes       │ Student Intake Form       │
│ Screenshots      │ (embedded in         │ (Typeform)                │
│ (9 real sessions │  screenshots)        │                           │
│  + 6k synthetic) │                      │                           │
└────────┬─────────┴──────────┬───────────┴─────────────┬─────────────┘
         │                    │                          │
    Auto Loader          Auto Loader              Make.com webhook
         │                    │                          │
         ▼                    ▼                     Confluent Kafka
┌─────────────────────────────────────────────────────────────────────┐
│                       BRONZE LAYER                                  │
│  tennis_dev.bronze                                                  │
│  ├── raw_match_extractions   (6,004 rows: 4 real + 6k synthetic)   │
│  ├── raw_training_sessions   (12,002 rows: 2 real + 12k synthetic) │
│  ├── raw_screenshots         (9 rows — screenshot metadata)        │
│  ├── raw_students            (streaming from Typeform via Kafka)   │
│  ├── extraction_failures     (dead letter queue)                   │
│  └── extraction_eval_set     (HITL-approved corrections)           │
│                                                                     │
│  System columns: _ingested_at · _source_file · _record_hash        │
│  · _pipeline_version                                                │
└──────────────────────────────┬──────────────────────────────────────┘
                                │
                    Claude API (claude-sonnet-4-6)
                    Vision + Text extraction
                    Confidence scoring
                    Dead Letter Queue on failure
                                │
                    ┌───────────┴──────────┐
               (confidence ≥ 0.8)   (confidence < 0.8)
                    │                      │
                    │            HITL Review App
                    │            (Streamlit Community Cloud)
                    │            Corrections → extraction_eval_set
                    └───────────┬──────────┘
                                │
┌─────────────────────────────────────────────────────────────────────┐
│                       SILVER LAYER — dbt views                      │
│  tennis_dev.silver_stg                                              │
│  ├── stg_match_stats          (type casting, null handling)        │
│  ├── stg_training_sessions    (normalised, validated)              │
│  ├── stg_students             (SCD Type 2)                         │
│  └── stg_player_name_mapping  (alias resolution seed)             │
│                                                                     │
│  tennis_dev.silver_int                                              │
│  ├── int_match_with_opponent_strength                              │
│  ├── int_session_rollup_daily                                      │
│  └── int_notes_with_match_linkage (7-day window JOIN)              │
└──────────────────────────────┬──────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────────────┐
│                       GOLD LAYER — dbt marts                        │
│  tennis_dev.silver_gold                                             │
│  ├── dim_players              (SCD Type 2)                         │
│  ├── fct_match_performance    (core fact, partitioned by month)    │
│  ├── fct_training_sessions    (training fact)                      │
│  ├── mart_player_development_score  ← flagship metric             │
│  ├── mart_coach_weekly_digest (incremental)                        │
│  └── mart_parent_monthly_report (incremental)                      │
│                                                                     │
│  tennis_dev.gold (PySpark-managed)                                  │
│  ├── extraction_eval_set      (HITL corrections)                   │
│  └── llm_quality_findings     (semantic inconsistency audit)       │
└──────────────────────────────┬──────────────────────────────────────┘
                                │
               ┌────────────────┴─────────────────┐
               ▼                                   ▼
   Streamlit Coach Dashboard           Databricks Genie
   (LLM recommendations)               (NL queries on Gold)

Cross-cutting:
  • Databricks Workflows    — orchestration & SLA alerts
  • GitHub Actions          — CI/CD, dbt build on PR
  • Unity Catalog           — column-level lineage
  • Databricks Lakehouse Monitoring — freshness & drift detection
```

---

## 4. Repository Structure

```
tennis-player-development-platform/
│
├── ingestion/
│   ├── bronze_match_stats.py          # Pandas batch ingestion for synthetic matches
│   ├── bronze_training_sessions.py    # Pandas batch ingestion for synthetic sessions
│   ├── bronze_screenshots.py          # Auto Loader for real SwingVision screenshots
│   ├── bronze_students_stream.py      # Kafka streaming intake events
│   └── workflow_config.yml            # Databricks Workflow DAG definition
│
├── extraction/
│   ├── extract_sessions.py            # Claude API Vision extraction (real screenshots)
│   └── llm_quality_auditor.py         # Semantic consistency auditor
│
├── tennis_analytics/                  # dbt project
│   ├── models/
│   │   ├── staging/                   # stg_* views (Bronze → Silver)
│   │   ├── intermediate/              # int_* views (Silver enrichments)
│   │   └── marts/                     # dim_*, fct_*, mart_* tables
│   ├── seeds/
│   │   └── player_name_mapping.csv    # Alias → canonical player ID
│   ├── tests/                         # Custom dbt tests
│   ├── dbt_project.yml
│   ├── packages.yml
│   └── profiles_example.yml
│
├── streamlit/
│   ├── hitl_review.py                 # Human-in-the-Loop review app
│   ├── coach_app.py                   # Coach dashboard with LLM recommendations
│   └── requirements.txt
│
├── scripts/
│   ├── generate_matches.py            # Synthetic match data generator (6k records)
│   ├── generate_training_sessions.py  # Synthetic session generator (12k records)
│   ├── z_ordering_demo.py             # Z-order performance benchmark
│   └── validate_contracts.py          # Data contract validation notebook
│
├── data_contracts/
│   ├── raw_match_extractions.yml      # Bronze schema contract
│   └── raw_training_sessions.yml      # Bronze schema contract
│
├── docs/
│   ├── decisions.md                   # 13 design decisions with full context
│   ├── known_limitations.md           # Honest limitations list
│   ├── self_assessment.md             # Project self-assessment report
│   └── schema.md                      # Full schema documentation
│
├── .github/
│   └── workflows/
│       ├── dbt-ci.yml                 # dbt deps + parse + slim CI
│       └── python-ci.yml             # ruff + pytest
│
├── .pre-commit-config.yaml
├── .gitignore
├── CONTRIBUTING.md
├── LICENSE
└── README.md
```

---

## 5. Data Sources & Ingestion Strategy

### Source 1: SwingVision Screenshots (Real Data — 9 sessions, 4 students)

SwingVision is a mobile app that uses computer vision to track tennis match and training statistics in real time. After each session the coach uploads the summary screenshots to a Databricks Volume.

**Filename convention (locked in):**
```
YYYYMMDDHHNN_firstname_match_N.png      # match sessions
YYYYMMDDHHNN_firstname_training_N.png   # training sessions
```
Same prefix = same session. Date/time is parsed from the filename — not from Claude. Page number at the end handles multi-page sessions.

**Real students with data:**
- `yi` — match sessions
- `jeffrey` — training sessions
- `darren` — training sessions
- `garret` — match + training sessions

**Ingestion flow:**
1. Coach uploads screenshots to `/Volumes/tennis_dev/bronze/raw_files/screenshots/`
2. `bronze_screenshots.py` reads metadata via Auto Loader into `bronze.raw_screenshots`
3. `extract_sessions.py` calls Claude API Vision on each file, writes structured JSON to `bronze.raw_match_extractions` or `bronze.raw_training_sessions`

**Claude API extraction output (matches):**
```json
{
  "match_id": "202603211400_yi_match",
  "player_id": "yi",
  "match_date": "2026-03-21",
  "score": "5-7",
  "player_won": false,
  "winners": 2,
  "unforced_errors": 13,
  "first_serves_in": 22,
  "first_serves_total": 29,
  "raw_note_text": "## groundstroke\nNeed to be more patient...",
  "extraction_confidence": 0.95,
  "prompt_version": "v1.0"
}
```

---

### Source 2: Synthetic Historical Data (6,000 matches + 12,000 training sessions)

**Why synthetic data?**
With only 4 real students and 9 sessions, the project cannot demonstrate window functions, partitioning, Z-ordering, incremental model behavior, or meaningful trend analysis. Synthetic data solves the "not enough data" objection without compromising engineering integrity — clearly documented here and marked with `prompt_version = "synthetic"` in the Bronze tables.

**Generation:**
```bash
python scripts/generate_matches.py       # → data/match_stats.parquet (6,000 rows)
python scripts/generate_training_sessions.py  # → data/training_sessions.parquet (12,000 rows)
```

**Configuration:**
- 50 synthetic students · 3 years of data (2023–2025)
- 40 matches/student/year · 80 sessions/student/year
- Realistic stat distributions based on USTA junior player profiles
- Random seed = 42 for reproducibility

**Ingestion:**
Uses pandas batch approach (not Auto Loader) due to Unity Catalog schema type constraints with the existing real data tables. Real data is preserved; synthetic data appended with deduplication by `match_id` / `session_id`.

---

### Source 3: Student Intake Questionnaire (Real-time streaming)

**Flow:**
```
Typeform form submission
    → Make.com webhook
    → Confluent Kafka (cluster: lkc-v7pn7kj, topic: student_intake_events)
    → Lakeflow Connect (Fivetran)
    → Databricks Delta streaming table
    → bronze.raw_students
```

**Why Kafka here?**
This is the pattern used at every production company for event-driven data. Demonstrating Lakeflow Connect + Kafka in a portfolio project immediately signals understanding of production streaming architectures. The form itself: `https://form.typeform.com/to/N1yCg9tE`

---

## 6. Medallion Layer Design

### Bronze Layer — PySpark / pandas

All Bronze tables are written by PySpark notebooks triggered by Databricks Workflows. The real screenshot data uses Auto Loader; synthetic data uses a pandas batch pattern.

**Schema conventions:**
- System metadata columns on all tables: `_ingested_at`, `_source_file`, `_record_hash`, `_pipeline_version`
- Delta Lake time travel retained for 30 days
- Dead letter queue: `bronze.extraction_failures` captures failed Claude API calls with `error_reason`

**Void columns** (present in schema but always null — SwingVision does not capture these fields):
- `raw_match_extractions`: `opponent_name`, `opponent_utr`, `tournament_name`, `surface`
- `raw_training_sessions`: `drills_completed`

These are excluded from all dbt staging models. Documented in `/docs/decisions.md` Decision 8.

| Table | Rows | Notes |
|---|---|---|
| `bronze.raw_match_extractions` | ~6,004 | 4 real + 6,000 synthetic |
| `bronze.raw_training_sessions` | ~12,002 | 2 real + 12,000 synthetic |
| `bronze.raw_screenshots` | 9 | Real screenshot metadata only |
| `bronze.raw_students` | varies | Live Typeform streaming |
| `bronze.extraction_failures` | — | Dead letter queue |

---

### Silver Layer — dbt (views)

All Silver models are dbt views — they compute on read from Bronze rather than materialising, keeping storage costs minimal and latency low for a project of this scale.

**Staging models (`silver_stg` schema):**

| Model | Key transformations |
|---|---|
| `stg_match_stats` | Type casting (match_date → DateType), null handling, percentage calculations from raw counts (first_serve_pct, break_point_conversion etc.) |
| `stg_training_sessions` | Normalised accuracy metrics, nullable serve/return fields handled, session_date cast |
| `stg_students` | SCD Type 2 — tracks student profile changes. Intake form fields mapped to canonical schema |
| `stg_player_name_mapping` | Resolves aliases ("Alex" / "Alex Chen" / Chinese names) to canonical player IDs — domain expertise creates engineering value here |

**Intermediate models (`silver_int` schema):**

| Model | Description |
|---|---|
| `int_match_with_opponent_strength` | Calculates `opponent_strength_weight` (defaults to 1.0 — opponent UTR not yet in SwingVision data) and `weighted_win`. Also derives `match_week`, `match_month`, `match_year_month` |
| `int_session_rollup_daily` | Aggregates multiple training sessions on the same day per player, computes averages and counts |
| `int_notes_with_match_linkage` | Time-window JOIN linking coaching note sessions to match performance within a 7-day window. Window size based on coaching expertise: notes from a training session typically affect match performance within the following week |

---

### Gold Layer — dbt marts (tables)

All Gold models are materialised as Delta tables. Fact tables are partitioned by `match_year_month`. Incremental models (`mart_coach_weekly_digest`, `mart_parent_monthly_report`) use `unique_key` to avoid reprocessing historical data.

**Note on schema naming:** dbt appends custom schema to the target schema configured in `profiles.yml`. With target = `silver`, models configured with `+schema: gold` land in `silver_gold`. PySpark-managed tables (`extraction_eval_set`, `llm_quality_findings`) land in the `gold` schema. See Design Decision 13.

| Model | Grain | Description |
|---|---|---|
| `dim_players` | One row per player version | SCD Type 2. Tracks `is_current` flag, `valid_from`, `valid_to` |
| `fct_match_performance` | One row per match per player | Core fact table. Includes all serve, return, rally, and break point stats as both raw counts and percentages. Includes `raw_note_text` from extraction, `has_note` flag, `opponent_strength_weight` |
| `fct_training_sessions` | One row per training day per player | Aggregated training session facts — accuracy rates, rally depth, serve/return speeds |
| `mart_player_development_score` | One row per player per week | **Flagship metric.** See formula below |
| `mart_coach_weekly_digest` | One row per player per week | Incremental. Surfaces `development_score`, `score_change`, `score_trend`, `matches_played`, `sessions_completed`, `avg_shots_per_hour` |
| `mart_parent_monthly_report` | One row per player per month | Incremental. Monthly roll-up for parent-facing dashboard |

**Player Development Score Formula:**

```
development_score = (
    win_rate_score            * 0.40   +   -- Win rate trend over last 8 weeks
    opponent_strength_score   * 0.30   +   -- Weighted win rate by opponent UTR (defaults to 1.0)
    technique_score           * 0.20   +   -- Training accuracy trend (shots_in, forehand/backhand)
    break_point_score         * 0.10       -- Break point conversion rate
) * 100
```

Score range: 0–100. Documented in full in the dbt model description for `mart_player_development_score`.

---

## 7. AI / LLM Integration

### Integration 1: Vision + Text Extraction (extract_sessions.py)

**Model:** `claude-sonnet-4-6`
**Method:** Direct `requests.post` to `https://api.anthropic.com/v1/messages` (no SDK — incompatible with Databricks runtime)
**Input:** SwingVision screenshot encoded as base64
**Output:** Structured JSON for match stats OR training session stats depending on filename

The same notebook handles both match and training screenshots — the filename convention (`_match_` vs `_training_`) determines which schema to extract against.

**Prompt versioning:** Prompts are version-controlled in `prompts/` as YAML files (`v1.0` currently). Prompt version is stored on every extracted record.

**Failure handling:** 3 retries → `bronze.extraction_failures` with `error_reason` → surfaced in HITL Streamlit app.

**Confidence scoring:** Claude returns a confidence score (0–1) for each extraction. Records with confidence < 0.8 are flagged for HITL review.

---

### Integration 2: HITL Review App (streamlit/hitl_review.py)

Streamlit app deployed on Streamlit Community Cloud. Shows extractions with confidence < 0.8, displays original screenshot alongside AI output, and allows the coach to approve or correct each field. Corrections are saved to `gold.extraction_eval_set` for future prompt evaluation.

---

### Integration 3: LLM Quality Auditor (extraction/llm_quality_auditor.py)

Weekly notebook that uses Claude API to compare coaching notes against match statistics for semantic consistency. Example: if notes say "backhand is inconsistent" but match data shows backhand error rate dropped 20%, it flags the discrepancy for coach review. Findings written to `gold.llm_quality_findings` and surfaced in the coach Streamlit app.

---

### Integration 4: Coach Dashboard Recommendations (streamlit/coach_app.py)

The coach dashboard includes a "Generate recommendation" button that constructs a prompt from recent match stats, training session accuracy, and coaching notes, then calls `claude-sonnet-4-6` to produce a specific, actionable next session plan. Output is rendered inline in the dashboard.

---

### Integration 5: Databricks Genie (Natural Language Querying)

Genie Space configured on `silver_gold` Gold layer tables. Business term definitions written in Genie instructions (e.g., what "break point conversion" means, what the development score formula is). Sample queries seeded for coach and parent personas.

---

## 8. dbt Project

### Project Structure

```
tennis_analytics/
├── models/
│   ├── staging/
│   │   ├── stg_match_stats.sql + .yml
│   │   ├── stg_training_sessions.sql + .yml
│   │   ├── stg_students.sql + .yml
│   │   └── stg_player_name_mapping.sql + .yml
│   ├── intermediate/
│   │   ├── int_match_with_opponent_strength.sql + .yml
│   │   ├── int_session_rollup_daily.sql + .yml
│   │   └── int_notes_with_match_linkage.sql + .yml
│   └── marts/
│       ├── dim_players.sql + .yml
│       ├── fct_match_performance.sql + .yml
│       ├── fct_training_sessions.sql + .yml
│       ├── mart_player_development_score.sql + .yml
│       ├── mart_coach_weekly_digest.sql + .yml
│       └── mart_parent_monthly_report.sql + .yml
├── seeds/
│   └── player_name_mapping.csv
├── tests/
│   ├── first_serve_pct_valid_range.sql
│   ├── match_date_not_future.sql
│   └── shots_in_valid_range.sql
├── dbt_project.yml
├── packages.yml              # dbt-utils
├── profiles_example.yml
└── sources.yml               # Bronze table sources with freshness checks
```

### Data Quality Tests

Every model has dbt tests. Total: **51 tests** across all layers.

| Layer | Test types |
|---|---|
| Staging | `not_null`, `unique`, `accepted_values`, custom range tests |
| Intermediate | `not_null`, `unique`, referential integrity |
| Marts | `not_null`, `unique`, `accepted_values` (score_trend), business logic |

Custom tests:
- `first_serve_pct_valid_range` — must be between 0 and 1
- `match_date_not_future` — match dates cannot be in the future
- `shots_in_valid_range` — training accuracy must be between 0 and 1

### Running dbt

```bash
cd tennis_analytics
dbt deps              # install dbt-utils
dbt build             # full build + test
dbt build --select stg_match_stats+    # build from staging onwards
dbt test              # run tests only
dbt docs generate     # generate docs
```

---

## 9. Production Engineering Practices

### CI/CD — GitHub Actions

```yaml
# On every PR:
  → dbt deps
  → dbt parse (syntax check)
  → dbt build --select state:modified+  (slim CI)

# On merge to main:
  → dbt build (full)
  → dbt docs generate
  → Publish dbt docs to GitHub Pages
```

`.github/workflows/dbt-ci.yml` and `.github/workflows/python-ci.yml` are both required checks on `main`. Branch protection enforced — all changes via PR.

### Orchestration — Databricks Workflows

DAG tasks in order:
1. `ingest_students` — bronze_students.py
2. `ingest_match_stats` — bronze_match_stats.py
3. `ingest_training_sessions` — bronze_training_sessions.py
4. `ingest_students_stream` — bronze_students_stream.py (parallel)
5. `ingest_screenshots` — bronze_screenshots.py
6. `extract_sessions` — extract_sessions.py
7. `validate_contracts` — validate_contracts.py
8. `llm_quality_auditor` — llm_quality_auditor.py

SLA alerts: email notification to `kemmwu@gmail.com` if any task exceeds 30 minutes or fails.

### Data Contracts

YAML contracts in `data_contracts/` define expected schema, nullability, and SLA at the Bronze-to-Silver boundary. `scripts/validate_contracts.py` runs as a Databricks Workflow task after extraction to verify schema compliance before dbt runs.

### Observability — Databricks Lakehouse Monitoring

Enabled on three key tables:
- `fct_match_performance` (timestamp: `match_date`, granularity: 1 day)
- `fct_training_sessions` (timestamp: `session_date`)
- `mart_player_development_score` (timestamp: `score_week`)

Monitors freshness, volume anomalies, and schema drift automatically.

### Lineage — Unity Catalog

Column-level lineage tracked through Unity Catalog. Any Gold metric can be traced back to its source Bronze file.

### Pre-commit Hooks

`.pre-commit-config.yaml` runs on every commit:
- `dbt-checkpoint` — every model must have a description and every column documented
- `ruff` — Python linting with auto-fix
- `trailing-whitespace`, `end-of-file-fixer`, `check-yaml`

---

## 10. Databricks Setup

```
Workspace:    https://dbc-66b56d97-276e.cloud.databricks.com
Catalog:      tennis_dev
Schemas:      bronze · silver · silver_stg · silver_int · silver_gold · gold
SQL Warehouse: Serverless Starter (HTTP path: /sql/1.0/warehouses/cc5f410c110e6188)
Secret scope: tennis (key: anthropic_api_key)
Volume path:  /Volumes/tennis_dev/bronze/raw_files/
```

**Schema naming note:**
- `silver_stg` — dbt staging models
- `silver_int` — dbt intermediate models
- `silver_gold` — dbt mart models (dim, fct, mart tables)
- `gold` — PySpark-managed tables (extraction_eval_set, llm_quality_findings)

This is because dbt appends the custom schema config to the target schema (`silver`). See Design Decision 13.

---

## 11. Local Development Setup

### Prerequisites

- Python 3.11+
- Databricks workspace access
- Anthropic API key

### Installation

```bash
# Clone repo
git clone https://github.com/kemmwu/tennis-player-development-platform.git
cd tennis-player-development-platform

# Create virtual environment
python -m venv .venv
source .venv/bin/activate   # Mac/Linux
# .venv\Scripts\activate    # Windows

# Install Python dependencies
pip install dbt-core dbt-databricks pre-commit faker pandas pyarrow requests

# Install pre-commit hooks
pre-commit install

# Configure dbt profile
cp tennis_analytics/profiles_example.yml ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your Databricks credentials

# Verify dbt connection
cd tennis_analytics
dbt debug
```

### Generate Synthetic Data

```bash
mkdir -p data
python scripts/generate_matches.py           # generates data/match_stats.parquet
python scripts/generate_training_sessions.py # generates data/training_sessions.parquet
```

Upload both Parquet files to `/Volumes/tennis_dev/bronze/raw_files/` via Databricks UI or VS Code + Databricks Extension.

### Run the Full Pipeline

```bash
# In Databricks: run ingestion notebooks
# bronze_match_stats.py → bronze_training_sessions.py → extract_sessions.py

# Locally: run dbt
cd tennis_analytics
dbt build
```

---

## 12. Design Decisions

Full log in `/docs/decisions.md`. Summary of key decisions:

| # | Decision | Choice |
|---|---|---|
| 1 | Databricks tier | Premium (Unity Catalog, Lakehouse Monitoring, Lakeflow Connect) |
| 2 | IDE | VS Code + Databricks Extension (not Databricks notebooks) |
| 3 | Bronze materialisation | PySpark notebooks (not DLT — simpler, lower cost, identical concepts) |
| 4 | Silver materialisation | dbt views (not tables — compute on read, lower storage cost at this scale) |
| 5 | LLM provider | Claude API via requests (not SDK — SDK incompatible with Databricks runtime) |
| 6 | Streaming | Lakeflow Connect + Confluent Kafka + Make.com (production-grade event streaming) |
| 7 | Synthetic data ingestion | Pandas batch (not Auto Loader — Unity Catalog schema type conflicts with real data) |
| 8 | Void columns | Excluded from dbt models (SwingVision does not capture opponent_utr, surface etc.) |
| 9 | Opponent strength | Defaults to 1.0 (neutral) — opponent UTR not yet available from SwingVision |
| 10 | Note-to-match window | 7 days (based on coaching expertise: training impact visible within one week) |
| 11 | Streamlit hosting | Community Cloud (not Databricks Apps — simpler deployment for portfolio scope) |
| 12 | Synthetic data strategy | 3 years × 50 students = 6k matches + 12k sessions (sufficient for Z-order, partitioning demos) |
| 13 | Schema naming | silver_gold for dbt marts (dbt appends custom schema to target schema = silver) |

---

## 13. Known Limitations

Full list in `/docs/known_limitations.md`. Key items:

1. **Small real data volume** — Only 4 real students, 9 real sessions. Majority of data is synthetic. Documented and marked with `prompt_version = 'synthetic'` in all tables.
2. **Opponent UTR not captured** — SwingVision does not record opponent strength. Development score opponent component defaults to 1.0 for all matches. Formula is correct; data is not yet available.
3. **No multi-tenancy** — Platform supports only one coach. Production version would need per-coach data isolation.
4. **Schema naming inconsistency** — `silver_gold` vs `gold` due to dbt profile target schema = `silver`. See Decision 13.
5. **Streamlit on Community Cloud** — Not Databricks Apps as originally designed. Databricks Apps would provide tighter Unity Catalog integration.
6. **No dashboard authentication** — All students visible to anyone with the Streamlit URL. Acceptable for demo; not for production.

---

## 14. Performance & Cost Log

*Last updated: May 2026*

### Data Volume

| Table | Row Count |
|---|---|
| `bronze.raw_match_extractions` | ~6,004 |
| `bronze.raw_training_sessions` | ~12,002 |
| `bronze.raw_students` | ~5 |
| `silver_gold.fct_match_performance` | ~6,004 |
| `silver_gold.mart_player_development_score` | ~2,600 |

### dbt Build Times

| Command | Duration |
|---|---|
| `dbt build` (full) | ~37–45 seconds |
| `dbt build --select stg_match_stats+` | ~30 seconds |
| `dbt build --select fct_match_performance+` | ~20 seconds |

### Claude API Cost

- Model: `claude-sonnet-4-6`
- Real extractions: 9 sessions × ~3 pages = ~27 API calls
- Estimated cost per 100 screenshots: ~$0.15–0.25
- Total extraction cost to date: < $0.10

### Databricks Cost (estimated)

- Serverless SQL Warehouse: ~$0.05–0.10/hour
- Monthly estimated cost at this project scale: < $5
- Full cost breakdown tracked in Databricks cost management UI

---

## 15. Resume Bullet Points

**Tennis Player Development Platform** | Personal Project | Jan 2026 – May 2026
[GitHub](https://github.com/kemmwu/tennis-player-development-platform) · [Demo Video](#) · [Coach Dashboard](#)

- Architected end-to-end Lakehouse on Databricks (Auto Loader, Lakeflow Connect + Confluent Kafka) ingesting SwingVision match screenshots, embedded coaching notes, and real-time student intake events — covering batch, micro-batch, and streaming ingestion patterns across 3 heterogeneous sources.

- Built AI-augmented extraction pipeline using Claude API (multimodal Vision + text) to parse structured match and training statistics from SwingVision screenshots, achieving 95% field-level accuracy with confidence scoring, a Dead Letter Queue, and a Human-in-the-Loop review workflow on Streamlit.

- Designed 13 dbt models across Bronze/Silver/Gold Medallion architecture with a custom Player Development Score metric combining win rate trends (40%), opponent strength weighting (30%), technique progression from LLM-parsed notes (20%), and break point conversion (10%) — formula fully documented in dbt model descriptions.

- Implemented production engineering practices: GitHub Actions slim CI (`dbt state:modified+`), Databricks Lakehouse Monitoring, data contracts with validation notebook, Unity Catalog column-level lineage, and Databricks Workflows orchestration with SLA alerting.

- Enabled stakeholder self-service via Databricks Genie natural language querying and a Streamlit coach dashboard with LLM-generated next session recommendations, backed by 6,000 match records and 12,000 training sessions across 50 modelled students.

---

## 16. Interview Narrative

### 30-Second Pitch

> *"I'm a certified tennis coach — my students have won the Virginia State Championship — and I'm transitioning into Analytics Engineering. I identified a real pain point in my own coaching practice: there's no systematic way to track player development over time. Match stats live in SwingVision screenshots, coaching observations live in notes, and student intake data lives in forms — none of it connected, none of it queryable.
So I built a production-grade Lakehouse on Databricks that ingests all three sources — using Claude API to extract structured statistics from the unstructured screenshots, Lakeflow Connect and Kafka for real-time intake streaming, and a full dbt Medallion architecture to transform everything into a Player Development Score and a coach dashboard with LLM-generated session recommendations.
What I'm most proud of is that because I'm the domain expert and the engineer, every modeling decision is grounded in real coaching logic — the development score weights, the 7-day note-to-match linkage window, the alias resolution for player names. Four real students' data is in the platform today."*

---

### Key Interview Q&A

**Q: Walk me through your architecture.**

Start with the architecture diagram above. Layer by layer: data source → ingestion → Bronze (PySpark/pandas) → AI extraction → HITL → Silver (dbt views) → Gold (dbt tables) → BI. For each layer explain *why* that design, not just *what* it does.

**Q: Why PySpark notebooks for Bronze instead of Delta Live Tables?**

DLT is a great framework but adds complexity and cost for a project at this scale. PySpark notebooks give full control over ingestion logic, are easier to debug, and cost nothing extra. The engineering concepts are identical — Auto Loader, file hash deduplication, schema enforcement, dead letter queue — just implemented explicitly rather than declaratively. In a larger team I would evaluate DLT seriously, and I can speak to that tradeoff.

**Q: Why pandas batch for synthetic data ingestion instead of Auto Loader?**

Unity Catalog has strict type enforcement. The existing Bronze tables had some columns stored with types inferred from real Claude API output (e.g. `aces` as void/null). The new synthetic Parquet files had integer types for those same columns. Auto Loader's schema caching made this conflict irresolvable without a full table rebuild. The pandas approach bypasses Auto Loader's schema inference entirely — read with pandas, cast explicitly to match existing Delta schema, then write. A pragmatic production choice.

**Q: What is the Player Development Score?**

A composite metric combining four components — win rate trend (40%), opponent strength-weighted win rate (30%), training technique accuracy trend (20%), and break point conversion (10%). The 40/30/20/10 weights come from my coaching experience: win rate is the most visible indicator, but it needs opponent context. Technique progression from training often predicts match improvement 2–3 weeks before it shows in results. Break point conversion is the highest-leverage micro-metric in competitive tennis.

**Q: How do you handle LLM failures in production?**

Three retries → Dead Letter Queue (`bronze.extraction_failures`) with `error_reason` → surfaced in HITL Streamlit app → coach reviews and corrects → correction saved to `gold.extraction_eval_set`. The loop never closes without measurement: a monthly evaluation notebook compares the latest prompt against the eval set and tracks field-level accuracy over time.

**Q: What was the hardest technical decision?**

Entity resolution for player names. A student appears as "Alex", "Alex Chen", or a Chinese name across different sources. I evaluated fuzzy matching, LLM-based resolution, and a manual mapping table. The manual seed table (`player_name_mapping.csv`) won — it's 100% accurate, fully auditable, and trivial to maintain for a coaching practice at this scale. Fuzzy matching introduces non-determinism. LLM resolution is overkill. The right tool for the right problem.

**Q: What would you do differently?**

Write data contracts before building staging models — I refactored the Silver layer mid-project when Bronze schema changed. Set the dbt target schema to a neutral value from day one to avoid the `silver_gold` naming inconsistency. And collect stakeholder feedback earlier — the dashboard was built before showing it to students. Their feedback would have changed the metrics displayed.

**Q: If you had 3 more months, what would you add?**

Multi-tenancy to support multiple coaches with per-coach data isolation in Unity Catalog. Automated prompt improvement based on eval set accuracy trends. A match opponent database to finally activate the opponent strength weight in the development score formula. Mobile-friendly coach interface for on-court use.

---

*Last updated: May 2026*
