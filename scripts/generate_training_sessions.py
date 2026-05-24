"""
generate_training_sessions.py
Generates synthetic training session statistics matching current Bronze schema.
Based on real SwingVision training screenshot fields.
"""

import random
from datetime import date, timedelta

import pandas as pd
from faker import Faker

fake = Faker()

# ── CONFIGURATION ─────────────────────────────────────────────────
NUM_STUDENTS         = 50
YEARS_OF_DATA        = 3
SESSIONS_PER_YEAR    = 80       # per student → ~12,000 total records
OUTPUT_FILE          = "data/training_sessions.parquet"
START_DATE           = date(2023, 1, 1)
END_DATE             = date(2025, 12, 31)
RANDOM_SEED          = 42
# ──────────────────────────────────────────────────────────────────

random.seed(RANDOM_SEED)
fake.seed_instance(RANDOM_SEED)

FIRST_NAMES = [
    "alex", "tim", "jessica", "michael", "sarah", "kevin", "emily",
    "ryan", "lisa", "daniel", "natalie", "james", "sophia", "andrew",
    "grace", "david", "olivia", "jason", "madison", "christopher",
    "ashley", "matthew", "hannah", "brandon", "samantha", "tyler",
    "rachel", "austin", "lauren", "dylan", "megan", "jordan",
    "brittany", "zachary", "kayla", "nicholas", "amanda", "caleb",
    "stephanie", "nathan", "jessica", "aaron", "chelsea", "adam",
    "allison", "eric", "vanessa", "sean", "heather", "kyle",
]

TIME_SLOTS = ["0800", "0900", "1000", "1100", "1300", "1400",
              "1500", "1600", "1700", "1800", "1900"]

DRILL_TYPES = [
    "Baseline rally + serve practice",
    "Cross-court consistency drills",
    "Serve and return practice",
    "Approach shot and volley",
    "Match play simulation",
    "Forehand and backhand targets",
    "Serve placement drills",
    "Net approach and finishing",
    "Rally consistency and depth",
    "Movement and recovery drills",
]


def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def generate_training_session(student_name: str, session_date: date,
                               session_time: str) -> dict:
    """Generate one realistic training session record."""

    # Skill level — consistent per student but varies slightly per session
    skill = random.uniform(0.45, 0.92)

    # Overall stats
    shots_in              = round(random.uniform(0.60, 0.95), 4)
    shots_per_hour        = random.randint(150, 400)
    longest_rally         = random.randint(8, 45)
    rallies_above_5_shots = round(random.uniform(0.30, 0.75), 4)

    # Serve stats (nullable — only present when serves were practiced)
    has_serve_data = random.random() > 0.3
    if has_serve_data:
        serves_in_ad    = round(random.uniform(0.40, 0.80), 4)
        serves_in_deuce = round(random.uniform(0.40, 0.80), 4)
        avg_serve_speed_ad    = random.randint(80, 140) \
            if random.random() > 0.2 else None
        avg_serve_speed_deuce = random.randint(80, 140) \
            if random.random() > 0.2 else None
    else:
        serves_in_ad          = None
        serves_in_deuce       = None
        avg_serve_speed_ad    = None
        avg_serve_speed_deuce = None

    # Return stats (nullable)
    has_return_data = random.random() > 0.3
    if has_return_data:
        returns_in_ad    = round(random.uniform(0.50, 0.90), 4)
        returns_in_deuce = round(random.uniform(0.50, 0.90), 4)
        avg_return_speed_ad    = random.randint(55, 100) \
            if random.random() > 0.2 else None
        avg_return_speed_deuce = random.randint(55, 100) \
            if random.random() > 0.2 else None
    else:
        returns_in_ad          = None
        returns_in_deuce       = None
        avg_return_speed_ad    = None
        avg_return_speed_deuce = None

    # Forehand stats
    fh_base = skill * random.uniform(0.85, 1.05)
    forehand_cross_court_in          = round(min(0.99, fh_base * random.uniform(0.75, 0.95)), 4)
    forehand_down_the_line_in        = round(min(0.99, fh_base * random.uniform(0.65, 0.88)), 4)
    forehand_avg_cross_court_speed   = random.randint(38, 72)
    forehand_avg_down_the_line_speed = random.randint(40, 75)
    forehand_cross_court_deep        = round(min(0.99, forehand_cross_court_in * random.uniform(0.80, 0.98)), 4)
    forehand_down_the_line_deep      = round(min(0.99, forehand_down_the_line_in * random.uniform(0.80, 0.98)), 4)

    # Backhand stats
    bh_base = skill * random.uniform(0.80, 1.00)
    backhand_cross_court_in          = round(min(0.99, bh_base * random.uniform(0.72, 0.94)), 4)
    backhand_down_the_line_in        = round(min(0.99, bh_base * random.uniform(0.62, 0.85)), 4)
    backhand_avg_cross_court_speed   = random.randint(35, 68)
    backhand_avg_down_the_line_speed = random.randint(36, 70)
    backhand_cross_court_deep        = round(min(0.99, backhand_cross_court_in * random.uniform(0.80, 0.97)), 4)
    backhand_down_the_line_deep      = round(min(0.99, backhand_down_the_line_in * random.uniform(0.80, 0.97)), 4)

    # Session ID from filename convention
    session_id = (
        f"{session_date.strftime('%Y%m%d')}"
        f"{session_time}_{student_name}_training"
    )

    return {
        "session_id":                       session_id,
        "player_id":                        student_name,
        "session_date":                     str(session_date),
        "session_time":                     session_time,
        "session_type":                     "training",
        "drills_completed":                 random.choice(DRILL_TYPES),
        "raw_note_text":                    None,
        "shots_in":                         shots_in,
        "shots_per_hour":                   shots_per_hour,
        "longest_rally":                    longest_rally,
        "rallies_above_5_shots":            rallies_above_5_shots,
        "serves_in_ad":                     serves_in_ad,
        "serves_in_deuce":                  serves_in_deuce,
        "avg_serve_speed_ad":               avg_serve_speed_ad,
        "avg_serve_speed_deuce":            avg_serve_speed_deuce,
        "returns_in_ad":                    returns_in_ad,
        "returns_in_deuce":                 returns_in_deuce,
        "avg_return_speed_ad":              avg_return_speed_ad,
        "avg_return_speed_deuce":           avg_return_speed_deuce,
        "forehand_cross_court_in":          forehand_cross_court_in,
        "forehand_down_the_line_in":        forehand_down_the_line_in,
        "forehand_avg_cross_court_speed":   forehand_avg_cross_court_speed,
        "forehand_avg_down_the_line_speed": forehand_avg_down_the_line_speed,
        "forehand_cross_court_deep":        forehand_cross_court_deep,
        "forehand_down_the_line_deep":      forehand_down_the_line_deep,
        "backhand_cross_court_in":          backhand_cross_court_in,
        "backhand_down_the_line_in":        backhand_down_the_line_in,
        "backhand_avg_cross_court_speed":   backhand_avg_cross_court_speed,
        "backhand_avg_down_the_line_speed": backhand_avg_down_the_line_speed,
        "backhand_cross_court_deep":        backhand_cross_court_deep,
        "backhand_down_the_line_deep":      backhand_down_the_line_deep,
        "pages_processed":                  random.randint(1, 3),
        "source_file":                      session_id,
        "extraction_confidence":            round(random.uniform(0.82, 0.99), 2),
        "prompt_version":                   "synthetic",
        "_ingested_at":                     fake.date_time_between(
                                                start_date=session_date
                                            ).isoformat(),
    }


def main():
    students = FIRST_NAMES[:NUM_STUDENTS]
    records  = []

    print(f"Generating training sessions for {NUM_STUDENTS} students...")

    for student in students:
        for _ in range(SESSIONS_PER_YEAR * YEARS_OF_DATA):
            session_date = random_date(START_DATE, END_DATE)
            session_time = random.choice(TIME_SLOTS)
            record       = generate_training_session(
                student, session_date, session_time
            )
            records.append(record)

    df = pd.DataFrame(records)

    # Ensure date column is proper type
    df["session_date"] = pd.to_datetime(df["session_date"]).dt.date

    print(f"Generated {len(df):,} training session records")
    print(f"Date range: {df['session_date'].min()} to {df['session_date'].max()}")
    print(f"Unique players: {df['player_id'].nunique()}")
    print(f"Avg shots_in: {df['shots_in'].mean():.1%}")
    print(f"Sessions with serve data: {df['serves_in_ad'].notna().sum():,}")

    df.to_parquet(
        OUTPUT_FILE,
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=True
    )
    print(f"\nSaved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
