# Data Schema Documentation

This document defines every table in the Tennis Player Development Platform,
including column names, data types, and business meaning.

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
  written by the coach at upload time
- A **note_extraction** stores LLM-extracted observations for a specific
  match or training session, referenced by `event_id` + `event_type`
- A **student** record includes all intake form data merged at ingestion

---

## Bronze Layer Tables

### bronze.raw_screenshots
Metadata for every SwingVision screenshot uploaded to the Volume.

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
Also stores the coach's raw note text for that match.

| Column | Type | Description |
|---|---|---|
| `match_id` | STRING | Unique match ID |
| `player_id` | STRING | Links to raw_students |
| `match_date` | DATE | Date of the match |
| `opponent_name` | STRING | Opponent name |
| `opponent_utr` | DOUBLE | Opponent UTR rating |
| `tournament_name` | STRING | Tournament or event name |
| `surface` | STRING | hard / clay / grass / carpet |
| `serves_in_ad` | DOUBLE | Serve accuracy on ad side (0–1) |
| `serves_in_deuce` | DOUBLE | Serve accuracy on deuce side (0–1) |
| `avg_serve_speed` | INT | Average serve speed |
| `max_serve_speed` | INT | Maximum serve speed |
| `returns_in_ad` | DOUBLE | Return accuracy on ad side (0–1) |
| `returns_in_deuce` | DOUBLE | Return accuracy on deuce side (0–1) |
| `avg_return_speed` | INT | Average return speed |
| `max_return_speed` | INT | Maximum return speed |
| `winners` | INT | Total winners hit |
| `unforced_errors` | INT | Total unforced errors |
| `break_points_won` | INT | Break points converted |
| `break_points_total` | INT | Total break point opportunities |
| `avg_rally_length` | DOUBLE | Average shots per rally |
| `player_won` | BOOLEAN | Match result |
| `score` | STRING | Raw match score string |
| `raw_note_text` | STRING | Coach's note for this match (nullable) |
| `source_file` | STRING | Source screenshot path |
| `extraction_confidence` | DOUBLE | LLM confidence score (0–1) |
| `prompt_version` | STRING | Prompt version used |
| `_ingested_at` | TIMESTAMP | When file was ingested |

---

### bronze.raw_training_sessions
Training session data with SwingVision shot statistics.
Also stores the coach's raw note text for that session.

| Column | Type | Description |
|---|---|---|
| `session_id` | STRING | Unique session ID |
| `player_id` | STRING | Links to raw_students |
| `session_date` | DATE | Date of session |
| `session_type` | STRING | Baseline / serve / volleys / fitness etc. |
| `drills_completed` | STRING | Description of drills done |
| `raw_note_text` | STRING | Coach's note for this session (nullable) |
| `heart_rate` | STRING | Average heart rate during session |
| `shots_in` | DOUBLE | % of shots that landed in (0–1) |
| `longest_rally` | INT | Longest rally in number of shots |
| `rallies_above_5_shots` | DOUBLE | % of rallies exceeding 5 shots (0–1) |
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
| `avg_ball_speed` | DOUBLE | Average ball speed across all shots |
| `max_ball_speed` | DOUBLE | Maximum ball speed recorded |
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
| `chinese_name` | STRING | Chinese name if applicable |
| `preferred_name` | STRING | Name used in coaching |
| `date_of_birth` | DATE | Date of birth |
| `utr_rating` | DOUBLE | Self-reported UTR rating |
| `age_group` | STRING | U10 / U12 / U14 / U16 / Adult |
| `dominant_hand` | STRING | Right / Left |
| `height` | STRING | Student height |
| `years_playing` | INT | Years of tennis experience |
| `training_frequency_per_week` | INT | Sessions per week |
| `coach_id` | STRING | Assigned coach |
| `submitted_at` | TIMESTAMP | Form submission time |
| `goals` | STRING | Student's stated goals |
| `injury_history` | STRING | Known injuries |
| `previous_coaching` | STRING | Prior coaching background |
| `competition_level` | STRING | Recreational / competitive |
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
LLM-extracted observations from coach notes. Each record links to either
a match or a training session via `event_id` + `event_type`.
`raw_note_text` lives in the parent session table, not here.

| Column | Type | Description |
|---|---|---|
| `event_id` | STRING | ID of the match or session this note belongs to |
| `event_type` | STRING | 'match' or 'training' — determines which table event_id references |
| `event_date` | DATE | Date of the event |
| `coach_id` | STRING | Coach who wrote the note |
| `player_id` | STRING | Player this note is about |
| `technique` | STRING | Tennis technique referenced |
| `issue` | STRING | Problem identified (nullable) |
| `recommendation` | STRING | Coach's recommendation (nullable) |
| `sentiment` | STRING | improving / needs_work / neutral |
| `source_file` | STRING | Source markdown file path |
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
| `age_group` | STRING | U10 / U12 / U14 / U16 / Adult |
| `dominant_hand` | STRING | Right / Left |
| `competition_level` | STRING | Recreational / competitive |
| `coach_id` | STRING | Assigned coach |
| `goals` | STRING | Student's stated goals |
| `valid_from` | TIMESTAMP | When this version became active |
| `valid_to` | TIMESTAMP | When superseded (null = current) |
| `is_current` | BOOLEAN | True for the active record |

---

### silver.stg_match_stats
Cleaned match statistics. One row per match per player.

| Column | Type | Description |
|---|---|---|
| `match_id` | STRING | Natural key |
| `player_id` | STRING | Resolved canonical player ID |
| `match_date` | DATE | Cleaned and type-cast |
| `opponent_name` | STRING | Opponent name |
| `opponent_utr` | DOUBLE | Opponent UTR for strength weighting |
| `surface` | STRING | hard / clay / grass / carpet |
| `serves_in_ad` | DOUBLE | Validated 0–1 |
| `serves_in_deuce` | DOUBLE | Validated 0–1 |
| `avg_serve_speed` | INT | Average serve speed |
| `max_serve_speed` | INT | Maximum serve speed |
| `returns_in_ad` | DOUBLE | Validated 0–1 |
| `returns_in_deuce` | DOUBLE | Validated 0–1 |
| `winners` | INT | Validated non-negative |
| `unforced_errors` | INT | Validated non-negative |
| `break_points_won` | INT | Break points converted |
| `break_points_total` | INT | Total break point chances |
| `avg_rally_length` | DOUBLE | Average shots per rally |
| `player_won` | BOOLEAN | Match result |
| `raw_note_text` | STRING | Coach's note (nullable) |
| `extraction_confidence` | DOUBLE | Screenshot LLM confidence score |

---

### silver.stg_training_sessions
Cleaned training session data with shot statistics.

| Column | Type | Description |
|---|---|---|
| `session_id` | STRING | Natural key |
| `player_id` | STRING | Resolved canonical player ID |
| `session_date` | DATE | Type-cast session date |
| `session_type` | STRING | Type of training session |
| `raw_note_text` | STRING | Coach's note (nullable) |
| `shots_in` | DOUBLE | Validated 0–1 |
| `longest_rally` | INT | Validated non-negative |
| `rallies_above_5_shots` | DOUBLE | Validated 0–1 |
| `forehand_cross_court_in` | DOUBLE | Validated 0–1 |
| `forehand_down_the_line_in` | DOUBLE | Validated 0–1 |
| `forehand_cross_court_deep` | DOUBLE | Validated 0–1 |
| `forehand_down_the_line_deep` | DOUBLE | Validated 0–1 |
| `backhand_cross_court_in` | DOUBLE | Validated 0–1 |
| `backhand_down_the_line_in` | DOUBLE | Validated 0–1 |
| `backhand_cross_court_deep` | DOUBLE | Validated 0–1 |
| `backhand_down_the_line_deep` | DOUBLE | Validated 0–1 |
| `avg_ball_speed` | DOUBLE | Average ball speed |
| `max_ball_speed` | DOUBLE | Maximum ball speed |

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
| `opponent_utr` | DOUBLE | Used for strength weighting |
| `serves_in_ad` | DOUBLE | |
| `serves_in_deuce` | DOUBLE | |
| `avg_serve_speed` | INT | |
| `winners` | INT | |
| `unforced_errors` | INT | |
| `break_points_won` | INT | |
| `break_points_total` | INT | |
| `break_point_conversion` | DOUBLE | won ÷ total |
| `avg_rally_length` | DOUBLE | |
| `player_won` | BOOLEAN | |
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
| `session_type` | STRING | Type of training |
| `shots_in` | DOUBLE | |
| `longest_rally` | INT | |
| `rallies_above_5_shots` | DOUBLE | |
| `forehand_cross_court_in` | DOUBLE | |
| `forehand_down_the_line_in` | DOUBLE | |
| `backhand_cross_court_in` | DOUBLE | |
| `backhand_down_the_line_in` | DOUBLE | |
| `avg_ball_speed` | DOUBLE | |
| `max_ball_speed` | DOUBLE | |
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
| `technique_progression` | DOUBLE | 20% weight — from LLM sentiment trend |
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
| `avg_serves_in_ad` | DOUBLE | Averaged across month's matches |
| `avg_serves_in_deuce` | DOUBLE | Averaged across month's matches |
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
| `_speed` suffix | Speed measurement | `avg_serve_speed` |