# Typeform Field Mapping

Documents how each Typeform question maps to `bronze.raw_students` columns.

## Form URL
https://form.typeform.com/to/N1yCg9tE

## Field Mapping

| Typeform Question | Typeform Field ID | Bronze Column | Type | Notes |
|---|---|---|---|---|
| Full name | field_001 | `full_name` | STRING | Required |
| Chinese name | field_002 | `chinese_name` | STRING | Optional |
| Preferred name | field_003 | `preferred_name` | STRING | Required |
| Date of birth | field_004 | `date_of_birth` | DATE | Required |
| UTR rating | field_005 | `utr_rating` | DOUBLE | Optional |
| Dominant hand | field_006 | `dominant_hand` | STRING | Right/Left |
| Height | field_007 | `height` | STRING | e.g. 165cm |
| Years playing | field_008 | `years_playing` | INT | Required |
| Training frequency | field_009 | `training_frequency_per_week` | INT | Required |
| Competition level | field_010 | `competition_level` | STRING | competitive/recreational |
| Goals | field_011 | `goals` | STRING | Free text |
| Injury history | field_012 | `injury_history` | STRING | Free text |
| Previous coaching | field_013 | `previous_coaching` | STRING | Free text |
| Contact name | field_014 | `contact_name` | STRING | Required |
| Contact email | field_015 | `contact_email` | STRING | Required |

## Kafka-Added Fields
These fields are added automatically by the streaming pipeline, not from the form:

| Column | Source |
|---|---|
| `student_id` | Generated UUID at ingestion |
| `intake_id` | Typeform response ID |
| `submitted_at` | Typeform submission timestamp |
| `kafka_offset` | Kafka message offset |
| `kafka_partition` | Kafka partition number |
| `_ingested_at` | Databricks ingestion timestamp |