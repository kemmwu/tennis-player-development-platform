# Data Schema Documentation

This document defines every table in the Tennis Player Development Platform,
including column names, data types, and business meaning.

Last updated: May 2026

---

## Schema Overview

```
tennis_dev/
├── bronze/     Raw ingested data — never modified after landing
├── silver/     Cleaned, typed, standardized — managed by dbt
├── gold/       Business-ready marts — managed by dbt
└── staging/    dbt development schema
```

---

## Entity Relationships

- A **student** has many **match_stats** and many **training_sessions**
- A **match** or **training_session** optionally contains `raw_note_text`
  captured from SwingVision's notes page screenshot
- A **note_extraction** stores LLM-extracted observations for a specific
  match or training session, referenced by `event_id` + `event_type`
- A **student** record includes all intake form data merged at ingestion
- All SwingVision screenshots (stats + notes) share the same filename
  convention: `YYYYMMDDHHNN_firstname_match/training_N.png`

---

## Bronze Layer Tables

### bronze.raw_screenshots
Metadata for every SwingVision screenshot uploaded to the Volume.
One row per file — stats pages and notes pages alike.

| Column | Type | Description |
|---|---|---|
| `_id` | STRING | Auto-generated row ID |
| `source_file` | STRING | Full path of source file in Volume |
| `file_hash` | STRING | SHA-256 hash for deduplication |
| `file_size_bytes` | BIGINT | File size in bytes |
| `_ingested_at` | TIMESTAMP | When Auto Loader picked up the file |
| `_pipeline_version` | STRING | Version of the ingestion notebook |

---

### bronze.raw_match_extractions
Match statistics extracted by Claude API Vision from SwingVision screenshots.
All pages in a session group (same filename prefix) are merged into one record.
Raw note text is extracted from the notes page if present.

| Column | Type | Description |
|---|---|---|
| `match_id` | STRING | Unique match ID (parsed from filename prefix) |
| `player_id` | STRING | Links to raw_students |
| `match_date` | DATE | Date parsed from filename |
| `session_time` | STRING | Start time parsed from filename (e.g. "1400") |
| `opponent_name` | STRING | Opponent name |
| `opponent_utr` | DOUBLE | Opponent UTR rating |
| `tournament_name` | STRING | Tournament or event name |
| `surface` | STRING | hard / clay / grass / carpet |
| `score` | STRING | Match score string |
| `player_won` | BOOLEAN | Match result |
| `winners` | INT | Total winners |
| `unforced_errors` | INT | Total unforced errors |
| `forehand_winners` | INT | Forehand winners |
| `forehand_unforced_errors` | INT | Forehand unforced errors |
| `backhand_winners` | INT | Backhand winners |
| `backhand_unforced_errors` | INT | Backhand unforced errors |
| `total_points_won` | INT | Total points won |
| `total_points_played` | INT | Total points played |
| `break_points_won` | INT | Break points converted (returning) |
| `break_points_total` | INT | Break point opportunities (returning) |
| `break_points_saved` | INT | Break points saved (serving) |
| `break_points_saved_total` | INT | Break points faced (serving) |
| `aces` | INT | Aces served |
| `service_winners` | INT | Service winners |
| `double_faults` | INT | Double faults |
| `first_serves_in` | INT | First serves in |
| `first_serves_total` | INT | First serves attempted |
| `second_serves_in` | INT | Second serves in |
| `second_serves_total` | INT | Second serves attempted |
| `serve_points_won` | INT | Total serve points won |
| `serve_points_total` | INT | Total serve points played |
| `first_serves_won` | INT | Points won on first serve |
| `first_serves_won_total` | INT | First serve points played |
| `second_serves_won` | INT | Points won on second serve |
| `second_serves_won_total` | INT | Second serve points played |
| `return_points_won` | INT | Total return points won |
| `return_points_total` | INT | Total return points played |
| `first_returns_won` | INT | Points won returning first serve |
| `first_returns_total` | INT | First serve returns played |
| `second_returns_won` | INT | Points won returning second serve |
| `second_returns_total` | INT | Second serve returns played |
| `rallies_1_4_won` | INT | 1–4 shot rallies won |
| `rallies_1_4_total` | INT | 1–4 shot rallies played |
| `rallies_5_8_won` | INT | 5–8 shot rallies won |
| `rallies_5_8_total` | INT | 5–8 shot rallies played |
| `rallies_9plus_won` | INT | 9+ shot rallies won |
| `rallies_9plus_total` | INT | 9+ shot rallies played |
| `raw_note_text` | STRING | Coach note extracted from notes page (nullable) |
| `pages_processed` | INT | Number of screenshots merged for this session |
| `source_file` | STRING | Filename prefix used as session identifier |
| `extraction_confidence` | DOUBLE | LLM confidence score (0–1) |
| `prompt_version` | STRING | Prompt version used |
| `_ingested_at` | TIMESTAMP | When file was ingested |

---

### bronze.raw_training_sessions
Training session data extracted from SwingVision screenshots.
All pages in a session group are merged into one record.
Raw note text extracted from notes page if present.

| Column | Type | Description |
|---|---|---|
| `session_id` | STRING | Unique session ID (parsed from filename prefix) |
| `player_id` | STRING | Links to raw_students |
| `session_date` | DATE | Date parsed from filename |
| `session_time` | STRING | Start time parsed from filename (e.g. "1400") |
| `session_type` | STRING | training |
| `drills_completed` | STRING | Description of drills (nullable) |
| `raw_note_text` | STRING | Coach note extracted from notes page (nullable) |
| `shots_in` | DOUBLE | Overall shot accuracy (0–1) |
| `shots_per_hour` | INT | Shot volume per hour |
| `longest_rally` | INT | Longest rally in shots |
| `rallies_above_5_shots` | DOUBLE | % rallies exceeding 5 shots (0–1) |
| `serves_in_ad` | DOUBLE | Serve accuracy on ad side (0–1, nullable) |
| `serves_in_deuce` | DOUBLE | Serve accuracy on deuce side (0–1, nullable) |
| `avg_serve_speed_ad` | INT | Avg serve speed on ad side (nullable) |
| `avg_serve_speed_deuce` | INT | Avg serve speed on deuce side (nullable) |
| `returns_in_ad` | DOUBLE | Return accuracy on ad side (0–1, nullable) |
| `returns_in_deuce` | DOUBLE | Return accuracy on deuce side (0–1, nullable) |
| `avg_return_speed_ad` | INT | Avg return speed on ad side (nullable) |
| `avg_return_speed_deuce` | INT | Avg return speed on deuce side (nullable) |
| `forehand_cross_court_in` | DOUBLE | FH cross-court accuracy (0–1) |
| `forehand_down_the_line_in` | DOUBLE | FH down-the-line accuracy (0–1) |
| `forehand_avg_cross_court_speed` | INT | FH cross-court average speed |
| `forehand_avg_down_the_line_speed` | INT | FH down-the-line average speed |
| `forehand_cross_court_deep` | DOUBLE | FH cross-court depth accuracy (0–1) |
| `forehand_down_the_line_deep` | DOUBLE | FH down-the-line depth accuracy (0–1) |
| `backhand_cross_court_in` | DOUBLE | BH cross-court accuracy (0–1) |
| `backhand_down_the_line_in` | DOUBLE | BH down-the-line accuracy (0–1) |
| `backhand_avg_cross_court_speed` | INT | BH cross-court average speed |
| `backhand_avg_down_the_line_speed` | INT | BH down-the-line average speed |
| `backhand_cross_court_deep` | DOUBLE | BH cross-court depth accuracy (0–1) |
| `backhand_down_the_line_deep` | DOUBLE | BH down-the-line depth accuracy (0–1) |
| `pages_processed` | INT | Number of screenshots merged for this session |
| `source_file` | STRING | Filename prefix used as session identifier |
| `extraction_confidence` | DOUBLE | LLM confidence score (0–1) |
| `prompt_version` | STRING | Prompt version used |
| `_ingested_at` | TIMESTAMP | Ingestion timestamp |

---

### bronze.raw_students
Student profiles merged with intake form data from Typeform via Kafka.
One record per student. All intake form fields stored directly on this table.

| Column | Type | Description |
|---|---|---|
| `student_id` | STRING | Unique student ID (PK) |
| `intake_id` | STRING | Unique intake submission ID |
| `full_name` | STRING | Student full name |
| `chinese_name` | STRING | Chinese name if applicable (nullable) |
| `preferred_name` | STRING | Name used in coaching |
| `date_of_birth` | DATE | Date of birth |
| `utr_rating` | DOUBLE | Self-reported UTR rating |
| `age_group` | STRING | U10 / U12 / U14 / U16 / U18 / Adult |
| `dominant_hand` | STRING | Right / Left |
| `height` | STRING | Student height |
| `years_playing` | INT | Years of tennis experience |
| `training_frequency_per_week` | INT | Sessions per week |
| `coach_id` | STRING | Assigned coach |
| `submitted_at` | TIMESTAMP | Form submission time |
| `goals` | STRING | Student's stated goals |
| `injury_history` | STRING | Known injuries |
| `previous_coaching` | STRING | Prior coaching background |
| `competition_level` | STRING | competitive / recreational |
| `contact_name` | STRING | Student or guardian contact name |
| `contact_email` | STRING | Student or guardian contact email |
| `kafka_offset` | BIGINT | Kafka message offset |
| `kafka_partition` | INT | Kafka partition |
| `is_active` | BOOLEAN | Whether student is currently active |
| `valid_from` | TIMESTAMP | SCD2 record start |
| `valid_to` | TIMESTAMP | SCD2 record end (null = current) |
| `is_current` | BOOLEAN | True for the active record |
| `_ingested_at` | TIMESTAMP | Ingestion timestamp |

---

### bronze.note_extractions
LLM-extracted observations from coaching notes captured in SwingVision.
Each record links to either a match or training session via `event_id` + `event_type`.

| Column | Type | Description |
|---|---|---|
| `event_id` | STRING | match_id or session_id this note belongs to |
| `event_type` | STRING | 'match' or 'training' |
| `event_date` | DATE | Date of the event |
| `coach_id` | STRING | Coach who wrote the note |
| `player_id` | STRING | Player this note is about |
| `technique` | STRING | Tennis technique referenced |
| `issue` | STRING | Problem identified (nullable) |
| `recommendation` | STRING | Coach's recommendation (nullable) |
| `sentiment` | STRING | improving / needs_work / neutral |
| `source_file` | STRING | Source screenshot filename prefix |
| `extraction_confidence` | DOUBLE | LLM confidence score (0–1) |
| `_ingested_at` | TIMESTAMP | Ingestion timestamp |

---

### bronze.extraction_failures
Dead letter queue for all failed LLM extraction calls.

| Column | Type | Description |
|---|---|---|
| `failure_id` | STRING | Unique failure ID |
| `source_file` | STRING | File that failed extraction |
| `error_reason` | STRING | Category of failure |
| `error_message` | STRING | Full error message |
| `retry_count` | INT | Number of retry attempts made |
| `pipeline_version` | STRING | Pipeline version at time of failure |
| `failed_at` | TIMESTAMP | When failure occurred |

---

## Silver Layer Tables (dbt)

### silver.stg_students
Cleaned student profiles with SCD Type 2 history tracking.

| Column | Type | Description |
|---|---|---|
| `student_sk` | STRING | Surrogate key (SCD2) |
| `student_id` | STRING | Natural business key |
| `full_name` | STRING | Standardized name |
| `preferred_name` | STRING | Name used in day-to-day coaching |
| `date_of_birth` | DATE | Typed and validated |
| `utr_rating` | DOUBLE | Cleaned UTR rating |
| `age_group` | STRING | U10 / U12 / U14 / U16 / U18 / Adult |
| `dominant_hand` | STRING | Right / Left |
| `competition_level` | STRING | competitive / recreational |
| `coach_id` | STRING | Assigned coach |
| `goals` | STRING | Student's stated goals |
| `valid_from` | TIMESTAMP | When this version became active |
| `valid_to` | TIMESTAMP | When superseded (null = current) |
| `is_current` | BOOLEAN | True for the active record |

---

### silver.stg_match_stats
Cleaned match statistics. One row per match per player.
Percentages calculated here from raw counts in Bronze.

| Column | Type | Description |
|---|---|---|
| `match_id` | STRING | Natural key |
| `player_id` | STRING | Resolved canonical player ID |
| `match_date` | DATE | Cleaned and type-cast |
| `session_time` | STRING | Start time from filename (e.g. "1400") |
| `opponent_name` | STRING | Opponent name |
| `opponent_utr` | DOUBLE | Opponent UTR for strength weighting |
| `surface` | STRING | hard / clay / grass / carpet |
| `score` | STRING | Match score |
| `player_won` | BOOLEAN | Match result |
| `winners` | INT | Validated non-negative |
| `unforced_errors` | INT | Validated non-negative |
| `forehand_winners` | INT | |
| `forehand_unforced_errors` | INT | |
| `backhand_winners` | INT | |
| `backhand_unforced_errors` | INT | |
| `total_points_won` | INT | |
| `total_points_played` | INT | |
| `total_points_won_pct` | DOUBLE | Calculated: won / played |
| `break_points_won` | INT | |
| `break_points_total` | INT | |
| `break_point_conversion` | DOUBLE | Calculated: won / total |
| `break_points_saved` | INT | |
| `break_points_saved_total` | INT | |
| `break_points_saved_pct` | DOUBLE | Calculated: saved / total |
| `aces` | INT | |
| `service_winners` | INT | |
| `double_faults` | INT | |
| `first_serves_in` | INT | |
| `first_serves_total` | INT | |
| `first_serve_pct` | DOUBLE | Calculated: in / total |
| `second_serves_in` | INT | |
| `second_serves_total` | INT | |
| `second_serve_pct` | DOUBLE | Calculated: in / total |
| `first_serves_won_pct` | DOUBLE | Calculated |
| `second_serves_won_pct` | DOUBLE | Calculated |
| `return_points_won_pct` | DOUBLE | Calculated |
| `first_returns_won_pct` | DOUBLE | Calculated |
| `second_returns_won_pct` | DOUBLE | Calculated |
| `rallies_1_4_won_pct` | DOUBLE | Calculated |
| `rallies_5_8_won_pct` | DOUBLE | Calculated |
| `rallies_9plus_won_pct` | DOUBLE | Calculated |
| `raw_note_text` | STRING | Coach's note (nullable) |
| `extraction_confidence` | DOUBLE | LLM confidence score |

---

### silver.stg_training_sessions
Cleaned training session data with SwingVision shot statistics.

| Column | Type | Description |
|---|---|---|
| `session_id` | STRING | Natural key |
| `player_id` | STRING | Resolved canonical player ID |
| `session_date` | DATE | Type-cast session date |
| `session_time` | STRING | Start time from filename (e.g. "1400") |
| `session_type` | STRING | Type of training session |
| `raw_note_text` | STRING | Coach's note (nullable) |
| `shots_in` | DOUBLE | Validated 0–1 |
| `shots_per_hour` | INT | Validated non-negative |
| `longest_rally` | INT | Validated non-negative |
| `rallies_above_5_shots` | DOUBLE | Validated 0–1 |
| `serves_in_ad` | DOUBLE | Validated 0–1 (nullable) |
| `serves_in_deuce` | DOUBLE | Validated 0–1 (nullable) |
| `avg_serve_speed_ad` | INT | Nullable |
| `avg_serve_speed_deuce` | INT | Nullable |
| `returns_in_ad` | DOUBLE | Validated 0–1 (nullable) |
| `returns_in_deuce` | DOUBLE | Validated 0–1 (nullable) |
| `avg_return_speed_ad` | INT | Nullable |
| `avg_return_speed_deuce` | INT | Nullable |
| `forehand_cross_court_in` | DOUBLE | Validated 0–1 |
| `forehand_down_the_line_in` | DOUBLE | Validated 0–1 |
| `forehand_avg_cross_court_speed` | INT | |
| `forehand_avg_down_the_line_speed` | INT | |
| `forehand_cross_court_deep` | DOUBLE | Validated 0–1 |
| `forehand_down_the_line_deep` | DOUBLE | Validated 0–1 |
| `backhand_cross_court_in` | DOUBLE | Validated 0–1 |
| `backhand_down_the_line_in` | DOUBLE | Validated 0–1 |
| `backhand_avg_cross_court_speed` | INT | |
| `backhand_avg_down_the_line_speed` | INT | |
| `backhand_cross_court_deep` | DOUBLE | Validated 0–1 |
| `backhand_down_the_line_deep` | DOUBLE | Validated 0–1 |
| `extraction_confidence` | DOUBLE | LLM confidence score |

---

### silver.stg_note_extractions
Cleaned LLM extraction outputs. One row per match or training session
that has an associated coaching note.

| Column | Type | Description |
|---|---|---|
| `event_id` | STRING | Natural key |
| `event_type` | STRING | 'match' or 'training' |
| `event_date` | DATE | Type-cast event date |
| `coach_id` | STRING | Coach who wrote the note |
| `player_id` | STRING | Resolved canonical player ID |
| `technique` | STRING | Tennis technique referenced |
| `issue` | STRING | Problem identified (nullable) |
| `recommendation` | STRING | Coach's recommendation (nullable) |
| `sentiment` | STRING | improving / needs_work / neutral |
| `extraction_confidence` | DOUBLE | LLM confidence score |

---

### silver.stg_player_name_mapping
Resolves all name aliases to a canonical student_id.
Critical for linking LLM-extracted player names to real student records.

| Column | Type | Description |
|---|---|---|
| `alias` | STRING | Name as it appears in source data |
| `canonical_student_id` | STRING | Resolved student_id |
| `alias_source` | STRING | Where this alias was found |
| `created_at` | TIMESTAMP | When this mapping was added |

---

## Gold Layer Tables (dbt)

### gold.dim_players
Player dimension table with full SCD Type 2 history.

| Column | Type | Description |
|---|---|---|
| `player_sk` | STRING | Surrogate key |
| `student_id` | STRING | Natural key |
| `preferred_name` | STRING | |
| `utr_rating` | DOUBLE | |
| `age_group` | STRING | |
| `competition_level` | STRING | |
| `valid_from` | TIMESTAMP | |
| `valid_to` | TIMESTAMP | |
| `is_current` | BOOLEAN | |

---

### gold.fct_match_performance
Core fact table. Grain: one match × one player.
Partitioned by `match_year_month` for query performance.

| Column | Type | Description |
|---|---|---|
| `match_sk` | STRING | Surrogate key |
| `player_sk` | STRING | FK to dim_players |
| `match_date_key` | INT | YYYYMMDD format |
| `session_time` | STRING | Match start time |
| `opponent_utr` | DOUBLE | Used for strength weighting |
| `player_won` | BOOLEAN | |
| `winners` | INT | |
| `unforced_errors` | INT | |
| `forehand_winners` | INT | |
| `backhand_winners` | INT | |
| `total_points_won_pct` | DOUBLE | |
| `break_point_conversion` | DOUBLE | |
| `break_points_saved_pct` | DOUBLE | |
| `first_serve_pct` | DOUBLE | |
| `second_serve_pct` | DOUBLE | |
| `first_serves_won_pct` | DOUBLE | |
| `second_serves_won_pct` | DOUBLE | |
| `return_points_won_pct` | DOUBLE | |
| `rallies_1_4_won_pct` | DOUBLE | |
| `rallies_5_8_won_pct` | DOUBLE | |
| `rallies_9plus_won_pct` | DOUBLE | |
| `has_note` | BOOLEAN | Whether a coaching note exists |
| `note_sentiment` | STRING | LLM sentiment if note exists (nullable) |
| `match_year_month` | STRING | Partition key (YYYY-MM) |

---

### gold.fct_training_sessions
Training session facts. Grain: one session × one player.

| Column | Type | Description |
|---|---|---|
| `session_sk` | STRING | Surrogate key |
| `player_sk` | STRING | FK to dim_players |
| `session_date_key` | INT | YYYYMMDD format |
| `session_time` | STRING | Session start time |
| `session_type` | STRING | Type of training |
| `shots_in` | DOUBLE | |
| `shots_per_hour` | INT | |
| `longest_rally` | INT | |
| `rallies_above_5_shots` | DOUBLE | |
| `forehand_cross_court_in` | DOUBLE | |
| `forehand_down_the_line_in` | DOUBLE | |
| `forehand_cross_court_deep` | DOUBLE | |
| `forehand_down_the_line_deep` | DOUBLE | |
| `backhand_cross_court_in` | DOUBLE | |
| `backhand_down_the_line_in` | DOUBLE | |
| `backhand_cross_court_deep` | DOUBLE | |
| `backhand_down_the_line_deep` | DOUBLE | |
| `has_note` | BOOLEAN | Whether a coaching note exists |
| `note_sentiment` | STRING | LLM sentiment if note exists (nullable) |

---

### gold.mart_player_development_score
Flagship metric. One row per player per week.

| Column | Type | Description |
|---|---|---|
| `score_id` | STRING | Surrogate key |
| `player_id` | STRING | FK to dim_players |
| `score_week` | DATE | Week start date |
| `development_score` | DOUBLE | Composite score 0–100 |
| `win_rate_trend` | DOUBLE | 40% weight component |
| `opponent_strength_score` | DOUBLE | 30% weight component |
| `technique_progression` | DOUBLE | 20% weight — derived from LLM sentiment trend |
| `break_point_conversion` | DOUBLE | 10% weight component |
| `calculated_at` | TIMESTAMP | When score was computed |

---

### gold.mart_coach_weekly_digest
Weekly summary for coaches. One row per player per week.

| Column | Type | Description |
|---|---|---|
| `digest_id` | STRING | Surrogate key |
| `player_id` | STRING | FK to dim_players |
| `week_start` | DATE | Week start date |
| `matches_played` | INT | Matches this week |
| `matches_won` | INT | Wins this week |
| `sessions_completed` | INT | Training sessions this week |
| `avg_development_score` | DOUBLE | Average score this week |
| `score_trend` | STRING | improving / declining / stable |
| `top_issue` | STRING | Most common LLM-flagged issue |
| `calculated_at` | TIMESTAMP | When digest was computed |

---

### gold.mart_parent_monthly_report
Monthly progress report. One row per player per month.

| Column | Type | Description |
|---|---|---|
| `report_id` | STRING | Surrogate key |
| `player_id` | STRING | FK to dim_players |
| `report_month` | STRING | YYYY-MM |
| `matches_played` | INT | |
| `win_rate` | DOUBLE | |
| `avg_first_serve_pct` | DOUBLE | Averaged across month's matches |
| `avg_break_point_conversion` | DOUBLE | Averaged across month's matches |
| `development_score` | DOUBLE | End-of-month score |
| `score_vs_last_month` | DOUBLE | Change from prior month |
| `sessions_completed` | INT | Training sessions this month |
| `calculated_at` | TIMESTAMP | When report was computed |

---

## Naming Conventions

| Pattern | Meaning | Example |
|---|---|---|
| `raw_` prefix | Bronze table, append-only | `raw_screenshots` |
| `stg_` prefix | Silver staging model | `stg_match_stats` |
| `int_` prefix | Silver intermediate model | `int_session_rollup` |
| `dim_` prefix | Gold dimension table | `dim_players` |
| `fct_` prefix | Gold fact table | `fct_match_performance` |
| `mart_` prefix | Gold business mart | `mart_player_development_score` |
| `_sk` suffix | Surrogate key (Gold only) | `player_sk` |
| `_id` suffix | Natural / business key | `student_id` |
| `_at` suffix | Timestamp column | `ingested_at` |
| `_date` suffix | Date column | `match_date` |
| `_in` suffix | Accuracy stored as 0–1 | `forehand_cross_court_in` |
| `_deep` suffix | Depth accuracy stored as 0–1 | `forehand_cross_court_deep` |
| `_speed` suffix | Speed measurement | `avg_serve_speed_ad` |
| `_pct` suffix | Percentage stored as 0–1, calculated in Silver | `first_serve_pct` |
| `_total` suffix | Denominator count for a ratio | `first_serves_total` |

## Design Notes

**Raw counts vs percentages:**
SwingVision shows serve stats as fractions e.g. "18/36 (50%)". Bronze stores
raw counts (`first_serves_in = 18`, `first_serves_total = 36`). Silver
calculates percentages (`first_serve_pct = 0.50`). This avoids rounding
loss and gives downstream models full flexibility.

**Nullable serve/return stats in training sessions:**
SwingVision only tracks serve and return stats when those shots actually
occur in a session. A baseline rally session will show 0% or blank for
serves. These columns are nullable in both Bronze and Silver.

**Session identity:**
Every match and training session is uniquely identified by the filename
prefix `YYYYMMDDHHNN_firstname_match/training`. This time-based identifier
handles multiple sessions for the same player on the same day without
ambiguity.