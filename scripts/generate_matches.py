"""
generate_matches.py
Generates synthetic match statistics matching the current Bronze schema.
Based on real SwingVision match screenshot fields.
"""

import random
from datetime import date, timedelta

import pandas as pd
from faker import Faker

fake = Faker()

# ── CONFIGURATION ─────────────────────────────────────────────────
NUM_STUDENTS      = 50
YEARS_OF_DATA     = 3
MATCHES_PER_YEAR  = 40        # per student → ~6,000 total records
OUTPUT_FILE       = "data/match_stats.parquet"
START_DATE        = date(2023, 1, 1)
END_DATE          = date(2025, 12, 31)
RANDOM_SEED       = 42
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

SURFACES    = ["hard", "clay", "grass", "carpet"]
TIME_SLOTS  = ["0900", "1000", "1100", "1300", "1400", "1500",
               "1600", "1700", "1800", "1900"]


def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def generate_match(student_name: str, match_date: date,
                   session_time: str) -> dict:
    """Generate one realistic match record."""

    # Skill level affects stats
    skill = random.uniform(0.4, 0.9)

    # Serve stats (raw counts)
    first_serves_total    = random.randint(30, 60)
    first_serves_in       = int(first_serves_total * random.uniform(0.45, 0.75))
    second_serves_total   = first_serves_total - first_serves_in
    second_serves_in      = int(second_serves_total * random.uniform(0.65, 0.90))

    serve_points_total    = first_serves_total
    first_serves_won      = int(first_serves_in * random.uniform(0.55, 0.80))
    second_serves_won     = int(second_serves_in * random.uniform(0.35, 0.60))
    serve_points_won      = first_serves_won + second_serves_won

    first_serves_won_total  = first_serves_in
    second_serves_won_total = second_serves_in

    # Return stats
    return_points_total   = random.randint(28, 55)
    return_points_won     = int(return_points_total * random.uniform(0.30, 0.65))
    first_returns_total   = int(return_points_total * 0.55)
    first_returns_won     = int(first_returns_total * random.uniform(0.25, 0.55))
    second_returns_total  = return_points_total - first_returns_total
    second_returns_won    = int(second_returns_total * random.uniform(0.40, 0.70))

    # Point stats
    total_points_played   = serve_points_total + return_points_total
    total_points_won      = serve_points_won + return_points_won

    winners               = int(random.gauss(15 * skill, 5))
    unforced_errors       = int(random.gauss(20 * (1 - skill * 0.5), 6))
    forehand_winners      = int(winners * random.uniform(0.55, 0.70))
    backhand_winners      = winners - forehand_winners
    forehand_unforced     = int(unforced_errors * random.uniform(0.45, 0.65))
    backhand_unforced     = unforced_errors - forehand_unforced

    # Break points
    bp_total              = random.randint(2, 10)
    bp_won                = int(bp_total * random.uniform(0.25, 0.65))
    bp_saved_total        = random.randint(1, 8)
    bp_saved              = int(bp_saved_total * random.uniform(0.30, 0.75))

    # Serve specials
    aces                  = max(0, int(random.gauss(3 * skill, 2)))
    service_winners       = max(0, int(random.gauss(4 * skill, 2)))
    double_faults         = max(0, int(random.gauss(3 * (1 - skill * 0.5), 2)))

    # Rally length buckets
    rallies_1_4_total     = random.randint(30, 60)
    rallies_1_4_won       = int(rallies_1_4_total * random.uniform(0.35, 0.65))
    rallies_5_8_total     = random.randint(15, 35)
    rallies_5_8_won       = int(rallies_5_8_total * random.uniform(0.35, 0.65))
    rallies_9plus_total   = random.randint(3, 15)
    rallies_9plus_won     = int(rallies_9plus_total * random.uniform(0.30, 0.70))

    # Match result
    player_won            = random.random() < (0.3 + skill * 0.4)
    sets_won              = random.randint(0, 2) if not player_won \
        else random.randint(1, 2)
    score                 = f"{sets_won}-{random.randint(0,1)}"

    # Session ID from filename convention
    session_id = (
        f"{match_date.strftime('%Y%m%d')}"
        f"{session_time}_{student_name}_match"
    )

    match_id = session_id

    return {
        "match_id":                 match_id,
        "player_id":                student_name,
        "match_date":               str(match_date),
        "session_time":             session_time,
        "score":                    score,
        "player_won":               player_won,
        "winners":                  max(0, winners),
        "unforced_errors":          max(0, unforced_errors),
        "forehand_winners":         max(0, forehand_winners),
        "forehand_unforced_errors": max(0, forehand_unforced),
        "backhand_winners":         max(0, backhand_winners),
        "backhand_unforced_errors": max(0, backhand_unforced),
        "total_points_won":         max(0, total_points_won),
        "total_points_played":      max(1, total_points_played),
        "break_points_won":         max(0, bp_won),
        "break_points_total":       max(1, bp_total),
        "break_points_saved":       max(0, bp_saved),
        "break_points_saved_total": max(1, bp_saved_total),
        "aces":                     max(0, aces),
        "service_winners":          max(0, service_winners),
        "double_faults":            max(0, double_faults),
        "first_serves_in":          max(0, first_serves_in),
        "first_serves_total":       max(1, first_serves_total),
        "second_serves_in":         max(0, second_serves_in),
        "second_serves_total":      max(1, second_serves_total),
        "serve_points_won":         max(0, serve_points_won),
        "serve_points_total":       max(1, serve_points_total),
        "first_serves_won":         max(0, first_serves_won),
        "first_serves_won_total":   max(1, first_serves_won_total),
        "second_serves_won":        max(0, second_serves_won),
        "second_serves_won_total":  max(1, second_serves_won_total),
        "return_points_won":        max(0, return_points_won),
        "return_points_total":      max(1, return_points_total),
        "first_returns_won":        max(0, first_returns_won),
        "first_returns_total":      max(1, first_returns_total),
        "second_returns_won":       max(0, second_returns_won),
        "second_returns_total":     max(1, second_returns_total),
        "rallies_1_4_won":          max(0, rallies_1_4_won),
        "rallies_1_4_total":        max(1, rallies_1_4_total),
        "rallies_5_8_won":          max(0, rallies_5_8_won),
        "rallies_5_8_total":        max(1, rallies_5_8_total),
        "rallies_9plus_won":        max(0, rallies_9plus_won),
        "rallies_9plus_total":      max(1, rallies_9plus_total),
        "raw_note_text":            None,
        "pages_processed":          random.randint(1, 3),
        "source_file":              match_id,
        "extraction_confidence":    round(random.uniform(0.82, 0.99), 2),
        "prompt_version":           "synthetic",
        "_ingested_at":             fake.date_time_between(
                                        start_date=match_date
                                    ).isoformat(),
    }


def main():
    students = FIRST_NAMES[:NUM_STUDENTS]
    records  = []

    print(f"Generating matches for {NUM_STUDENTS} students...")

    for student in students:
        for _ in range(MATCHES_PER_YEAR * YEARS_OF_DATA):
            match_date   = random_date(START_DATE, END_DATE)
            session_time = random.choice(TIME_SLOTS)
            record       = generate_match(student, match_date, session_time)
            records.append(record)

    df = pd.DataFrame(records)

    # Ensure date column is proper type
    df["match_date"] = pd.to_datetime(df["match_date"]).dt.date

    print(f"Generated {len(df):,} match records")
    print(f"Date range: {df['match_date'].min()} to {df['match_date'].max()}")
    print(f"Unique players: {df['player_id'].nunique()}")
    print(f"Win rate: {df['player_won'].mean():.1%}")

    df.to_parquet(
        OUTPUT_FILE,
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=True
    )
    print(f"\nSaved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
