# Tennis Player Development Platform
### An AI-Augmented Analytics Engineering Project on Databricks

> **One-line pitch:** An end-to-end Lakehouse platform built by a certified tennis coach, using AI to process unstructured SwingVision data and help coaches improve student performance through data-driven decisions.

---

## Table of Contents

1. [Project Positioning](#1-project-positioning)
2. [Tech Stack](#2-tech-stack)
3. [Architecture Overview](#3-architecture-overview)
4. [Data Sources & Ingestion Strategy](#4-data-sources--ingestion-strategy)
5. [Medallion Layer Design](#5-medallion-layer-design)
6. [AI / LLM Integration](#6-ai--llm-integration)
7. [Production Engineering Practices](#7-production-engineering-practices)
8. [Deliverables](#8-deliverables)
9. [Design Decisions Log](#9-design-decisions-log)
10. [Resume Bullet Points](#10-resume-bullet-points)
11. [Interview Narrative](#11-interview-narrative)

---

## 1. Project Positioning

### The Problem

Independent tennis coaches have no systematic way to track student development over time. Match statistics live in SwingVision screenshots (unstructured images), coaching observations live in handwritten or typed notes (unstructured text), and student intake data lives in forms. None of it is connected, none of it is queryable, and none of it compounds into insight over weeks and months.

### Five Core Selling Points

**1. Domain Expertise × Data Engineering — dual identity**
Built by a certified tennis coach whose students have competed at the USTA Sectional level. Every modeling decision — the 7-day note-to-match linkage window, the development score component weights, the player name alias resolution — is grounded in real coaching logic, not guesswork. In interviews you are both subject matter expert and engineer at the same time.

**2. Diverse data sources covering all ingestion patterns**
- Unstructured images (SwingVision screenshots) → Multimodal LLM extraction
- Unstructured text (coaching notes embedded in screenshots) → Text LLM structured extraction
- Structured form data (student intake questionnaire) → Lakeflow Connect + Kafka event-driven streaming
- Synthetic historical data → batch loading for volume and scale demonstration

**3. AI as a tool, not a gimmick**
Claude API solves real data engineering problems: unstructured-to-structured conversion, semantic quality auditing (coaching notes vs match statistics consistency), and natural language querying via Databricks Genie. This is the AI-augmented data platform narrative that employers want in 2026.

**4. Production engineering practices**
Medallion architecture · dbt layered modeling · Auto Loader · Lakeflow Connect · 50+ data quality tests · GitHub Actions CI/CD · Databricks Workflows orchestration · Lakehouse Monitoring · Unity Catalog lineage · Human-in-the-Loop review · data contracts · pre-commit hooks.

**5. Real data, real students**
9 real SwingVision sessions from 4 real students actively coached (yi, jeffrey, darren, garret). The Streamlit coach dashboard is used for real coaching decisions. This is something 95% of portfolio projects cannot claim.

---

## 2. Tech Stack

This project runs on **Databricks Premium** and is built to maximise Databricks-native tooling, with VS Code as the local development environment and GitHub for all version control and CI/CD.

| Layer | Tool | Purpose |
|---|---|---|
| **IDE** | VS Code + Databricks Extension | Local development, notebook sync, Git integration |
| **Version Control** | GitHub + GitHub Pull Requests extension | All code, dbt models, prompts, notebooks — PR workflow in VS Code |
| **Storage & Compute** | Databricks Premium | Core Lakehouse platform |
| **Workspace** | `dbc-66b56d97-276e.cloud.databricks.com` | Databricks workspace host |
| **Catalog** | Unity Catalog (`tennis_dev`) | Schemas: bronze, silver, silver_stg, silver_int, silver_gold, gold |
| **Tables** | Delta Lake | ACID transactions, time travel, schema evolution |
| **File Storage** | Databricks Volumes | Raw files: screenshots, Parquet, CSV |
| **Real Data Ingestion** | Auto Loader + PySpark | SwingVision screenshot metadata ingestion |
| **Synthetic Data Ingestion** | pandas + PySpark (batch) | Schema-safe ingestion of synthetic Parquet files |
| **Streaming Ingestion** | Lakeflow Connect + Confluent Kafka | Real-time Typeform intake events |
| **Webhook Automation** | Make.com | Typeform webhook → Confluent Kafka bridge |
| **Intake Form** | Typeform | Student onboarding questionnaire |
| **Transformation** | dbt Core 1.11 + dbt-databricks 1.12 | Silver & Gold layer modeling (13 models) |
| **AI / LLM** | Claude API (`claude-sonnet-4-6`) via `requests` | Extract structured stats from SwingVision screenshots |
| **Data Quality** | 50+ dbt tests (built-in + custom) | Multi-layer quality gates across all models |
| **CI/CD** | GitHub Actions | dbt deps + parse + slim CI on every PR |
| **Orchestration** | Databricks Workflows | Full DAG with 8 tasks, SLA alerts |
| **HITL Review** | Streamlit Community Cloud | Human review of low-confidence AI extractions |
| **Coach Dashboard** | Streamlit Community Cloud | Student development view + LLM recommendations |
| **NL Queries** | Databricks Genie | Natural language querying on Gold layer |
| **Observability** | Databricks Lakehouse Monitoring | Freshness, volume, schema drift on 3 key tables |
| **Lineage** | Unity Catalog | Column-level lineage from Bronze to Gold |
| **Pre-commit** | dbt-checkpoint + ruff + pre-commit-hooks | Documentation enforcement + linting |

### Why This Stack?

Every tool above is either **Databricks-native**, **GitHub-native**, or the **industry-standard open-source tool** (dbt) that works across every major cloud warehouse. There is no throwaway tool here. Every item on this list appears in real job descriptions for Analytics Engineer and Data Engineer roles in 2026.

**One deliberate deviation from the original design:**
The HITL Review app and Coach Dashboard are hosted on **Streamlit Community Cloud** rather than Databricks Apps. This was a pragmatic decision: Streamlit Community Cloud is simpler to deploy and debug for a single-developer portfolio project. A production deployment would use Databricks Apps for tighter Unity Catalog integration and better access control.

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                               │
├──────────────────┬──────────────────────┬───────────────────────────┤
│ SwingVision      │ Coaching Notes       │ Student Intake Form       │
│ Screenshots      │ (embedded in         │ (Typeform)                │
│ (9 real +        │  screenshots as      │                           │
│  6k synthetic)   │  raw_note_text)      │                           │
└────────┬─────────┴──────────┬───────────┴──────────────┬────────────┘
         │                    │                           │
    Auto Loader          PySpark batch               Make.com
    (real data)        (synthetic data)              webhook
         │                    │                           │
         ▼                    ▼                    Confluent Kafka
┌─────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER  (tennis_dev.bronze)                │
│  raw_match_extractions  (6,004 rows: 4 real + 6k synthetic)        │
│  raw_training_sessions  (12,002 rows: 2 real + 12k synthetic)      │
│  raw_screenshots        (9 rows — screenshot metadata)             │
│  raw_students           (Typeform events via Kafka streaming)      │
│  extraction_failures    (dead letter queue)                        │
│                                                                     │
│  System columns on all tables:                                     │
│  _ingested_at · _source_file · _record_hash · _pipeline_version   │
└──────────────────────────────┬──────────────────────────────────────┘
                                │
               Claude API (claude-sonnet-4-6)
               Vision + Text extraction
               Confidence score per record
               3x retry → Dead Letter Queue on failure
                                │
                    ┌───────────┴──────────┐
               confidence ≥ 0.8        confidence < 0.8
                    │                       │
                    │              HITL Review App
                    │              (Streamlit Community Cloud)
                    │              Coach approves/corrects
                    │              → gold.extraction_eval_set
                    └───────────┬──────────┘
                                │
┌─────────────────────────────────────────────────────────────────────┐
│               SILVER LAYER — dbt views                              │
│  tennis_dev.silver_stg                                             │
│  ├── stg_match_stats         (cast types, pct calculations)        │
│  ├── stg_training_sessions   (normalised accuracy metrics)         │
│  ├── stg_students            (SCD Type 2)                          │
│  └── stg_player_name_mapping (alias → canonical player ID seed)   │
│                                                                     │
│  tennis_dev.silver_int                                             │
│  ├── int_match_with_opponent_strength (strength_weight = 1.0)     │
│  ├── int_session_rollup_daily         (daily aggregation)          │
│  └── int_notes_with_match_linkage     (7-day window JOIN)          │
└──────────────────────────────┬──────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────────────┐
│               GOLD LAYER — dbt mart tables                          │
│  tennis_dev.silver_gold   (dbt-managed)                            │
│  ├── dim_players                (SCD Type 2)                       │
│  ├── fct_match_performance      (partitioned by match_year_month)  │
│  ├── fct_training_sessions                                         │
│  ├── mart_player_development_score  ← flagship metric             │
│  ├── mart_coach_weekly_digest       (incremental)                  │
│  └── mart_parent_monthly_report     (incremental)                  │
│                                                                     │
│  tennis_dev.gold   (PySpark-managed)                               │
│  ├── extraction_eval_set        (HITL-approved corrections)        │
│  └── llm_quality_findings       (semantic audit findings)          │
└──────────────────────────────┬──────────────────────────────────────┘
                                │
               ┌────────────────┴──────────────────┐
               ▼                                    ▼
   Streamlit Coach Dashboard            Databricks Genie
   Community Cloud                      NL queries on Gold
   LLM recommendations                  "Did my serve improve?"

Cross-cutting:
  • Databricks Workflows (8-task DAG) — orchestration & SLA alerts
  • GitHub Actions                    — CI/CD, dbt slim CI on PR
  • Unity Catalog                     — column-level lineage
  • Databricks Lakehouse Monitoring   — freshness & drift on 3 tables
  • Data contracts (YAML)             — Bronze-to-Silver validation
```

---

## 4. Data Sources & Ingestion Strategy

### Source 1: SwingVision Screenshots (Unstructured Images)

SwingVision is a mobile app that uses computer vision to generate real-time statistics during tennis sessions. After each session, the app produces a multi-page screenshot summary showing serve percentages, winners, errors, rally length distributions, and embedded coaching notes.

**Filename convention (established in week 3, locked in for the project lifetime):**
```
YYYYMMDDHHNN_firstname_match_N.png      # match session, page N
YYYYMMDDHHNN_firstname_training_N.png   # training session, page N
```

The session ID is constructed from the filename prefix. Date and time are parsed from the filename — never from Claude — to avoid LLM date hallucination. Multiple pages with the same prefix belong to the same session.

**Real students with data:** yi · jeffrey · darren · garret (9 total sessions)

**Ingestion flow:**
1. Coach uploads screenshots to `/Volumes/tennis_dev/bronze/raw_files/screenshots/`
2. `bronze_screenshots.py` reads file metadata via Auto Loader into `bronze.raw_screenshots`
3. `extract_sessions.py` calls Claude API Vision on each file, routes output to `bronze.raw_match_extractions` or `bronze.raw_training_sessions` based on filename

**Void columns — important production note:**
SwingVision does not capture opponent information or surface type. The following columns exist in the schema (populated by earlier design) but are always null in real data:
- `opponent_name`, `opponent_utr`, `tournament_name`, `surface` (match table)
- `drills_completed` (training table)

These are explicitly excluded from all dbt staging models. The opponent strength component of the development score defaults to 1.0 (neutral weight) until opponent data is available.

---

### Source 2: Synthetic Historical Data (6,000 matches + 12,000 training sessions)

**Why synthetic data is necessary and acceptable:**
With only 4 real students and 9 sessions, the platform cannot demonstrate window functions, partitioning, Z-ordering, incremental model behavior, or meaningful trend analysis. Synthetic data solves this without compromising integrity — it is clearly marked with `prompt_version = 'synthetic'` in every Bronze record.

**Generation scripts:**
```bash
python scripts/generate_matches.py           # 50 students × 40 matches/year × 3 years
python scripts/generate_training_sessions.py # 50 students × 80 sessions/year × 3 years
```

Random seed = 42 for reproducibility. Date range: 2023-01-01 to 2025-12-31.

**Ingestion approach — pandas batch (not Auto Loader):**
The original design specified Auto Loader for all ingestion. During development, Unity Catalog's strict type enforcement caused irresolvable schema conflicts between real data (some columns stored as void/null types from early extractions) and synthetic Parquet files (integer columns). The solution was to use pandas to read the Parquet files, apply explicit type casts matching the existing Delta schema, then write with `overwriteSchema=True`. This is documented in Design Decision 7.

---

### Source 3: Student Intake Questionnaire (Real-time Streaming)

**End-to-end flow:**
```
Typeform submission (form: https://form.typeform.com/to/N1yCg9tE)
    → Make.com webhook automation
    → Confluent Kafka (cluster: lkc-v7pn7kj, topic: student_intake_events)
    → Databricks Lakeflow Connect (Fivetran)
    → bronze.raw_students (Delta streaming table)
```

**Why keep Kafka here:**
Even at low event volume (a coaching practice onboards 2-3 students per month), this architecture is the pattern used at every production company for event-driven data. Demonstrating Lakeflow Connect + Kafka immediately signals understanding of production streaming — not just batch pipelines. The engineering is identical regardless of event volume.

---

## 5. Medallion Layer Design

### Bronze Layer — PySpark / pandas notebooks

**Schema conventions on all Bronze tables:**
- `_ingested_at` — timestamp when the record entered the platform
- `_source_file` — path to the originating file in the Volume
- `_record_hash` — SHA-256 hash of the primary key for deduplication
- `_pipeline_version` — version of the ingestion notebook that created the record

**Current row counts:**

| Table | Rows | Real vs Synthetic |
|---|---|---|
| `bronze.raw_match_extractions` | ~6,004 | 4 real + 6,000 synthetic |
| `bronze.raw_training_sessions` | ~12,002 | 2 real + 12,000 synthetic |
| `bronze.raw_screenshots` | 9 | All real |
| `bronze.raw_students` | varies | Live Typeform events |
| `bronze.extraction_failures` | — | Dead letter queue |

---

### Silver Layer — dbt views (`silver_stg` + `silver_int` schemas)

Silver models are dbt **views** — they compute on read from Bronze rather than materialising as tables. This keeps storage costs minimal at this project scale while preserving all transformation logic in dbt.

**Staging models:**

| Model | Key transformations |
|---|---|
| `stg_match_stats` | Casts `match_date` to DateType; computes percentages from raw counts (`first_serve_pct` = `first_serves_in / first_serves_total`); excludes void columns; null handling |
| `stg_training_sessions` | Normalises accuracy metrics; handles nullable serve/return fields; casts `session_date`; excludes `drills_completed` (void) |
| `stg_students` | SCD Type 2 implementation — tracks `valid_from`, `valid_to`, `is_current` for student profile changes over time |
| `stg_player_name_mapping` | Reads from `player_name_mapping.csv` seed; resolves aliases to canonical player IDs (e.g. "Alex" / "Alex Chen" / Chinese name → single canonical ID) |

**Intermediate models:**

| Model | Description |
|---|---|
| `int_match_with_opponent_strength` | Calculates `opponent_strength_weight` (= 1.0 until opponent UTR data available), `weighted_win`, and date dimension fields (`match_week`, `match_month`, `match_year_month`) |
| `int_session_rollup_daily` | Groups multiple training sessions on the same day per player; computes averages across all accuracy metrics and rally stats |
| `int_notes_with_match_linkage` | Time-window JOIN: links coaching note sessions to match records within a 7-day forward window. Window size grounded in coaching expertise: technique focus from a training session typically shows in match results within the following week |

---

### Gold Layer — dbt mart tables (`silver_gold` schema)

All Gold models materialise as Delta tables. Fact tables are partitioned by `match_year_month`. Incremental models use `unique_key` to avoid reprocessing historical data.

**Schema naming note:** dbt appends custom schema to the target schema (`silver` in `profiles.yml`). Models with `+schema: gold` land in `silver_gold`. PySpark-managed tables remain in `gold`. See Design Decision 13.

| Model | Grain | Notes |
|---|---|---|
| `dim_players` | Per player version | SCD Type 2 — `valid_from`, `valid_to`, `is_current` |
| `fct_match_performance` | Per match × player | All serve, return, rally stats as raw counts and percentages; `raw_note_text`; `has_note`; `opponent_strength_weight` |
| `fct_training_sessions` | Per training day × player | Aggregated accuracy rates, serve/return speeds, rally depth metrics |
| `mart_player_development_score` | Per player × week | Flagship metric — see formula below |
| `mart_coach_weekly_digest` | Per player × week | Incremental. `development_score`, `score_change`, `score_trend`, `matches_played`, `sessions_completed`, `avg_shots_per_hour` |
| `mart_parent_monthly_report` | Per player × month | Incremental. Monthly roll-up for parent reporting |

**Player Development Score Formula:**

```
development_score = (
    win_rate_score            × 0.40   +   -- Win rate trend over last 8 weeks
    opponent_strength_score   × 0.30   +   -- Weighted win rate (defaults to 1.0 until UTR available)
    technique_score           × 0.20   +   -- Training accuracy trend
    break_point_score         × 0.10       -- Break point conversion rate
) × 100
```

Weights are grounded in coaching expertise:
- Win rate is the most visible outcome but needs opponent context (hence 40% not 70%)
- Technique progression often predicts match improvement 2–3 weeks before it appears in results
- Break point conversion is the single highest-leverage micro-metric in competitive tennis

Score range: 0–100. Full formula documented in the dbt model description for `mart_player_development_score`.

---

## 6. AI / LLM Integration

### Integration 1: Vision + Text Extraction

**Model:** `claude-sonnet-4-6`
**API:** Direct `requests.post` to `https://api.anthropic.com/v1/messages`

> **Why not the Anthropic SDK?**
> The Anthropic Python SDK is incompatible with the Databricks runtime environment. Direct HTTP requests via `requests` library work identically and have no dependency issues. This is a deliberate pragmatic choice, not an oversight.

**Input:** SwingVision screenshot encoded as base64
**Output:** Structured JSON validated against expected schema

The extraction notebook (`extract_sessions.py`) handles both match and training screenshots. The filename (`_match_` vs `_training_`) determines which extraction schema and prompt to use.

Coaching notes written by the coach in the SwingVision app are embedded as text in the screenshots. Claude extracts both the quantitative statistics AND the qualitative coaching text (`raw_note_text`) in a single API call.

**Confidence scoring:** Claude returns a 0–1 confidence score per extraction. Records with score < 0.8 are flagged for HITL review.

**Failure handling:**
```
3 retries
    → bronze.extraction_failures (with error_reason, source_file, timestamp)
    → Surfaced in HITL Streamlit app
    → Coach manually enters correct values
    → Correction saved to gold.extraction_eval_set
```

**Prompt versioning:** Current version `v1.0`. Prompts stored in `prompts/` as YAML. `prompt_version` stored on every extracted record — enables accuracy tracking by prompt version.

---

### Integration 2: HITL Review App

Streamlit app on Community Cloud. Surfaces extractions with confidence < 0.8 showing the original screenshot alongside the AI output. Coach can approve or correct each field. Approved corrections write to `gold.extraction_eval_set` which forms the ground truth for future prompt evaluation.

---

### Integration 3: LLM Quality Auditor

Weekly Databricks Workflow task (`llm_quality_auditor.py`) that uses Claude API to compare coaching notes against match statistics for semantic consistency. Example: if notes say "backhand is inconsistent" but match data shows backhand error rate dropped 20% that week, it flags the discrepancy. Output written to `gold.llm_quality_findings`. Findings visible in the coach dashboard.

This demonstrates a key architectural principle: **LLMs as components in a data platform with defined roles**, not as chatbots. The auditor has a specific, bounded job — semantic cross-source consistency checking — and writes structured output to a queryable table.

---

### Integration 4: Coach Dashboard Recommendations

The coach dashboard includes a "Generate recommendation" button. It constructs a prompt from recent match performance stats, training session accuracy trends, and coaching notes, then calls `claude-sonnet-4-6` to produce a specific, actionable next session plan. Output is rendered inline in the Streamlit UI. This is the visible stakeholder-facing AI feature that demonstrates the full pipeline value.

---

### Integration 5: Databricks Genie (Natural Language Querying)

Genie Space configured on `silver_gold` tables. Business term definitions written in Genie instructions (what "break point conversion" means, how the development score is calculated, what "improvement" means). Sample queries seeded for both coach and parent personas. Quality of Genie answers depends 90% on how well the Gold layer tables and columns are named and documented — this is core Analytics Engineering output.

---

## 7. Production Engineering Practices

### CI/CD — GitHub Actions

Two workflows:

**`dbt-ci.yml`** (runs on every PR):
```
1. dbt deps          — install dbt-utils package
2. dbt parse         — syntax check all models
3. dbt build --select state:modified+   — slim CI (changed models + downstream only)
```

**`python-ci.yml`** (runs on every PR):
```
1. ruff              — Python linting
2. pytest            — unit tests
```

Branch protection on `main`: both workflows must pass before merge. All changes go through PRs — no direct pushes to `main`.

### Orchestration — Databricks Workflows

8-task DAG in dependency order:

```
ingest_students ──────────────────────────────────────┐
ingest_match_stats → ingest_training_sessions          │
                            ↓                          │
                    ingest_screenshots                 │ (parallel)
                            ↓                          │
                    extract_sessions                   │
                       ↓          ↓                    │
            validate_contracts  llm_quality_auditor    │
                                                       └──→ (ingest_students_stream — continuous)
```

SLA alert: email to `kemmwu@gmail.com` if any task exceeds 30 minutes or fails.

### Data Quality — Multi-Layer

| Layer | Tool | What It Checks |
|---|---|---|
| Bronze | Python assertions in notebooks | Schema enforcement, null critical fields, deduplication |
| Bronze | Data contracts (YAML + validate_contracts.py) | Schema, nullability, minimum row count at Bronze-to-Silver boundary |
| Silver/Gold | dbt built-in tests | `not_null`, `unique`, `accepted_values`, relationships |
| Silver/Gold | Custom dbt tests | `first_serve_pct_valid_range`, `match_date_not_future`, `shots_in_valid_range` |
| All layers | Databricks Lakehouse Monitoring | Freshness, volume anomalies, schema drift over time |

Total: **51 dbt tests** across all models.

### Data Contracts

YAML files in `data_contracts/` define expected schema, nullability, and SLA at the Bronze-to-Silver boundary. `scripts/validate_contracts.py` runs as a Databricks Workflow task after extraction to verify compliance before dbt runs. One of the most discussed patterns in AE roles in 2026.

### Observability

- **Column-level lineage:** Unity Catalog traces any Gold metric back to its source Bronze file
- **Table monitoring:** Databricks Lakehouse Monitoring on `fct_match_performance`, `fct_training_sessions`, `mart_player_development_score` — freshness, volume, and schema drift
- **Dead letter queue:** `bronze.extraction_failures` captures every failed LLM call with full context

### Pre-commit Hooks

`.pre-commit-config.yaml` enforces on every local commit:
- `dbt-checkpoint` — every dbt model must have a description; every column documented
- `ruff` — Python linting with auto-fix
- `trailing-whitespace`, `end-of-file-fixer`, `check-yaml`

---

## 8. Deliverables

### What Was Actually Built

| Deliverable | Status | Notes |
|---|---|---|
| Bronze ingestion (real screenshots) | ✅ | Auto Loader + PySpark |
| Bronze ingestion (synthetic data) | ✅ | Pandas batch approach |
| Streaming intake (Kafka + Lakeflow Connect) | ✅ | Typeform → Make.com → Confluent → Databricks |
| Claude API extraction (match + training) | ✅ | 9 real sessions, 95% confidence |
| HITL Streamlit review app | ✅ | Deployed on Streamlit Community Cloud |
| LLM quality auditor | ✅ | Writes to gold.llm_quality_findings |
| dbt Silver layer (4 staging + 3 intermediate) | ✅ | All 51 tests passing |
| dbt Gold layer (2 dim + 2 fct + 3 mart) | ✅ | All incremental models working |
| Player Development Score | ✅ | Formula documented in dbt model description |
| Coach dashboard (Streamlit) | ✅ | LLM recommendations working |
| Databricks SQL Dashboard | ✅ | 5 widgets on Gold layer |
| Databricks Genie | ✅ | Configured on silver_gold tables |
| GitHub Actions CI/CD | ✅ | dbt slim CI + Python CI |
| Databricks Workflows orchestration | ✅ | 8-task DAG with SLA alerts |
| Databricks Lakehouse Monitoring | ✅ | 3 key tables monitored |
| Data contracts | ✅ | 2 YAML contracts + validation notebook |
| Unity Catalog lineage | ✅ | Column-level lineage tracking |
| Pre-commit hooks | ✅ | dbt-checkpoint + ruff |
| Synthetic data at scale | ✅ | 6k matches + 12k sessions |

### What Was Designed but Not Built

| Item | Reason |
|---|---|
| Automated monthly PDF report | Deprioritised — Streamlit dashboard covers the same use case |
| MLflow player similarity clustering | Out of scope for portfolio timeline |
| Databricks Apps deployment | Streamlit Community Cloud simpler for single-developer project |
| Multi-tenancy (multiple coaches) | Known limitation, documented |

### Repository Structure

```
tennis-player-development-platform/
├── ingestion/              # Bronze ingestion notebooks
├── extraction/             # Claude API extraction + LLM auditor
├── tennis_analytics/       # dbt project (13 models, 51 tests)
├── streamlit/              # HITL review app + coach dashboard
├── scripts/                # Data generators, Z-order demo, contract validation
├── data_contracts/         # Bronze schema YAML contracts
├── docs/                   # decisions.md, known_limitations.md, self_assessment.md
├── .github/workflows/      # CI/CD pipeline definitions
├── CONTRIBUTING.md
├── LICENSE
└── README.md
```

---

## 9. Design Decisions Log

Full detail in `/docs/decisions.md`. Summary:

| # | Decision | Chosen approach | Key tradeoff |
|---|---|---|---|
| 1 | Databricks tier | Premium | Unity Catalog, Lakehouse Monitoring, Lakeflow Connect require Premium |
| 2 | IDE | VS Code + Databricks Extension | Local development comfort + Git integration |
| 3 | Bronze materialisation | PySpark notebooks (not DLT) | Simpler, lower cost, identical engineering concepts |
| 4 | Silver materialisation | dbt views (not tables) | Lower storage cost at this scale; compute on read |
| 5 | LLM API method | Direct `requests` (not Anthropic SDK) | SDK incompatible with Databricks runtime |
| 6 | Streaming stack | Typeform → Make.com → Confluent → Lakeflow Connect | Production-grade pattern regardless of event volume |
| 7 | Synthetic ingestion | Pandas batch (not Auto Loader) | Unity Catalog type conflicts with void columns in real data |
| 8 | Void columns | Excluded from dbt models | SwingVision does not capture opponent UTR, surface, drills |
| 9 | Opponent strength | Defaults to 1.0 | Formula correct; data not yet available from SwingVision |
| 10 | Note-to-match window | 7 days | Coaching expertise: training impact visible within one week |
| 11 | Streamlit hosting | Community Cloud (not Databricks Apps) | Simpler deployment for portfolio scope |
| 12 | Synthetic data scale | 3 years × 50 students | Sufficient for Z-order, partitioning, incremental demos |
| 13 | Schema naming | `silver_gold` for dbt marts | dbt appends custom schema to `profiles.yml` target schema (`silver`) |

---

## 10. Resume Bullet Points

**Tennis Player Development Platform** | Personal Project | Jan 2026 – May 2026
[GitHub](https://github.com/kemmwu/tennis-player-development-platform) · [Demo Video](#) · [Coach Dashboard](#)

- Architected end-to-end Lakehouse on Databricks (Auto Loader, Lakeflow Connect + Confluent Kafka) ingesting SwingVision match screenshots, embedded coaching notes, and real-time student intake events — covering batch, micro-batch, and streaming ingestion patterns across 3 heterogeneous sources.

- Built AI-augmented extraction pipeline using Claude API (multimodal Vision + text) to parse structured match and training statistics from SwingVision screenshots, with confidence scoring, a Dead Letter Queue, and a Human-in-the-Loop review workflow deployed on Streamlit Community Cloud.

- Designed 13 dbt models across Bronze/Silver/Gold Medallion architecture with a custom Player Development Score metric combining win rate trends (40%), opponent strength weighting (30%), technique progression from LLM-parsed coaching notes (20%), and break point conversion (10%) — formula documented in dbt model descriptions.

- Implemented production engineering practices: GitHub Actions slim CI (`dbt state:modified+`), Databricks Lakehouse Monitoring, YAML data contracts with validation notebook, Unity Catalog column-level lineage, and Databricks Workflows 8-task DAG with SLA alerting.

- Enabled stakeholder self-service via Databricks Genie natural language querying and a Streamlit coach dashboard with LLM-generated next session recommendations, backed by 6,000 match records and 12,000 training sessions across 50 modelled students.

---

## 11. Interview Narrative

### 30-Second Pitch

> *"I'm a tennis coach transitioning into Analytics Engineering. I identified a real pain point: there's no systematic player development tracking tool for independent coaches. So I built a production-grade Lakehouse on Databricks that ingests SwingVision match screenshots, embedded coaching notes, and real-time student intake events via Kafka and Lakeflow Connect — with Claude API extracting structured statistics from the unstructured screenshots. What I'm most proud of is that because I'm the domain expert and the engineer, every modeling decision — the development score weights, the 7-day note-to-match window, the alias resolution logic — is grounded in real coaching logic. Four real students' data is in the platform today."*

---

### Key Interview Q&A

**Q: Walk me through your architecture.**
Open the architecture diagram. Layer by layer: data source → ingestion → Bronze (PySpark/pandas) → Claude API extraction → HITL → Silver (dbt views) → Gold (dbt tables) → BI. For each layer explain *why* that design choice, not just what it does.

**Q: Why PySpark notebooks for Bronze instead of Delta Live Tables?**
DLT is a great framework but adds cost and complexity for a project at this scale. PySpark notebooks give full control over ingestion logic and are easier to debug. The engineering concepts are identical — schema enforcement, deduplication, dead letter queue — just implemented explicitly rather than declaratively. In a larger team I would evaluate DLT and can speak confidently to that tradeoff.

**Q: Why did you use pandas for synthetic data ingestion instead of Auto Loader?**
Unity Catalog has strict type enforcement. The existing Bronze tables had some columns stored as void type (from early real extractions where Claude returned nulls). Auto Loader's schema caching made merging the new Parquet files — which had integer types for those same columns — irresolvable without a full table rebuild. The pandas approach bypasses schema caching entirely: read with pandas, explicitly cast every column to match the existing Delta schema, then write. A pragmatic production engineering decision.

**Q: What is the Player Development Score and why those weights?**
A composite of four components: win rate trend (40%), opponent strength-adjusted wins (30%), training technique accuracy trend (20%), and break point conversion (10%). The weights come from coaching experience. Win rate is the most visible indicator but needs opponent context — a 60% win rate against USTA 4.0 players means something very different than 60% against 5.0 players. Technique progression from training often predicts match improvement 2–3 weeks before it shows in results. Break point conversion is the single highest-leverage micro-metric — it directly measures performance under pressure.

**Q: How do you handle LLM failures in production?**
3 retries → Dead Letter Queue (`bronze.extraction_failures`) with `error_reason` and full context → surfaced in HITL Streamlit app → coach approves/corrects → correction saved to `gold.extraction_eval_set`. The loop never closes without measurement — a monthly evaluation notebook compares the latest prompt against the eval set and tracks field-level accuracy by prompt version over time.

**Q: What was the hardest technical decision?**
Player name entity resolution. A student appears as "Alex", "Alex Chen", or their Chinese name across different sources. I evaluated fuzzy matching (non-deterministic, hard to audit), LLM-based resolution (overkill, adds latency and cost), and a manual seed table. The seed table won — 100% accurate, fully auditable, trivial to maintain for a coaching practice with 10–15 active students. The right tool for the right problem.

**Q: What would you do differently?**
Write data contracts before building the Silver layer — I refactored mid-project when Bronze schema changed. Set the dbt target schema to a neutral value from day one to avoid the `silver_gold` naming inconsistency. And collect real stakeholder feedback earlier — the dashboard design assumptions changed after showing it to actual users.

**Q: If you had 3 more months, what would you add?**
Multi-tenancy with per-coach Unity Catalog schemas. Automated prompt improvement based on eval set accuracy trends. Integration with tournament scheduling APIs to populate opponent data and finally activate the opponent strength weight in the development score. Mobile-first UI for on-court coaching use.

---

*Last updated: May 2026*
