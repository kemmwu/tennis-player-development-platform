import uuid
import random
import pandas as pd
from faker import Faker
from datetime import date, timedelta
from pathlib import Path

fake = Faker()
random.seed(42)

# ── CONFIG ────────────────────────────────────────────────────────
OUTPUT_DIR  = Path(__file__).parent.parent / "data" / "synthetic"
OUTPUT_FILE = OUTPUT_DIR / "match_stats.parquet"
START_DATE  = date(2024, 1, 1)
END_DATE    = date(2025, 12, 31)
# ─────────────────────────────────────────────────────────────────

SURFACES      = ["hard", "clay", "grass", "carpet"]
SURFACE_WEIGHTS = [0.55, 0.25, 0.10, 0.10]
TOURNAMENTS   = [
    "USTA Junior Circuit",
    "Local Club Championship",
    "Sectional Qualifier",
    "City Open",
    "Regional Junior Open",
    "Adult League Finals",
    "Club Round Robin",
    "Friendly Match",
]


def utr_to_serve_speed(utr: float) -> tuple[int, int]:
    """Higher UTR = faster serves on average."""
    base_avg = int(90 + utr * 5)
    base_max = int(base_avg + random.randint(15, 35))
    noise_avg = random.randint(-8, 8)
    noise_max = random.randint(-5, 5)
    return base_avg + noise_avg, base_max + noise_max


def utr_to_accuracy(utr: float, base_low: float, base_high: float) -> float:
    """Higher UTR = better accuracy, with realistic noise."""
    scale  = (utr / 16.0)
    value  = base_low + scale * (base_high - base_low)
    noise  = random.uniform(-0.05, 0.05)
    return round(min(max(value + noise, 0.0), 1.0), 3)


def improvement_factor(match_date: date) -> float:
    """Students improve gradually over 2 years."""
    days_in = (match_date - START_DATE).days
    total   = (END_DATE - START_DATE).days
    return days_in / total * 0.08   # up to 8% improvement over 2 years


def win_probability(player_utr: float, opponent_utr: float) -> float:
    """UTR difference predicts win probability."""
    diff = player_utr - opponent_utr
    # Sigmoid-like: each UTR point = ~15% swing
    prob = 0.5 + diff * 0.15
    return min(max(prob, 0.05), 0.95)


def generate_score(player_won: bool) -> str:
    """Generate a realistic tennis score string."""
    if player_won:
        patterns = [
            "6-4 6-3", "6-3 6-2", "7-5 6-4", "6-4 7-5",
            "6-2 6-4", "6-1 6-3", "7-6 6-4", "6-4 6-4",
            "6-3 3-6 6-4", "7-5 4-6 6-3", "6-4 6-7 6-3",
        ]
    else:
        patterns = [
            "4-6 3-6", "3-6 2-6", "5-7 4-6", "4-6 5-7",
            "4-6 3-6", "1-6 3-6", "4-7 4-6", "4-6 4-6",
            "4-6 6-3 4-6", "5-7 6-4 3-6", "4-6 7-6 3-6",
        ]
    return random.choice(patterns)


def generate_match(student: dict, match_date: date) -> dict:
    utr      = student["utr_rating"]
    improve  = improvement_factor(match_date)
    eff_utr  = utr * (1 + improve)   # effective UTR on that date

    # Opponent: within ±2.5 UTR of player
    opp_utr  = round(max(1.0, eff_utr + random.uniform(-2.5, 2.5)), 1)
    opp_name = fake.name()

    avg_spd, max_spd   = utr_to_serve_speed(eff_utr)
    avg_ret, max_ret   = utr_to_serve_speed(eff_utr * 0.85)

    player_won = random.random() < win_probability(eff_utr, opp_utr)

    # Winners and errors: better UTR = more winners, fewer errors
    winners = int(max(1, random.gauss(8 + eff_utr * 1.2, 4)))
    errors  = int(max(1, random.gauss(25 - eff_utr * 1.0, 6)))

    bp_total = random.randint(0, 8)
    bp_won   = random.randint(0, bp_total)

    # Moving distance: longer rallies = more movement
    avg_rally      = round(max(1.5, random.gauss(4 + eff_utr * 0.3, 1.5)), 1)
    moving_dist    = round(avg_rally * random.uniform(120, 180), 1)

    return {
        "match_id":               str(uuid.uuid4()),
        "player_id":              student["student_id"],
        "match_date":             match_date.isoformat(),
        "opponent_name":          opp_name,
        "opponent_utr":           opp_utr,
        "tournament_name":        random.choice(TOURNAMENTS),
        "surface":                random.choices(SURFACES, weights=SURFACE_WEIGHTS)[0],
        "serves_in_ad":           utr_to_accuracy(eff_utr, 0.45, 0.75),
        "serves_in_deuce":        utr_to_accuracy(eff_utr, 0.50, 0.80),
        "avg_serve_speed":        avg_spd,
        "max_serve_speed":        max_spd,
        "returns_in_ad":          utr_to_accuracy(eff_utr, 0.40, 0.72),
        "returns_in_deuce":       utr_to_accuracy(eff_utr, 0.45, 0.75),
        "avg_return_speed":       avg_ret,
        "max_return_speed":       max_ret,
        "total_moving_distance":  moving_dist,
        "winners":                winners,
        "unforced_errors":        errors,
        "break_points_won":       bp_won,
        "break_points_total":     bp_total,
        "avg_rally_length":       avg_rally,
        "player_won":             player_won,
        "score":                  generate_score(player_won),
        "raw_note_text":          None,
        "source_file":            None,
        "extraction_confidence":  None,
        "prompt_version":         None,
        "ingested_at":            match_date.isoformat(),
    }


def generate_match_schedule(student: dict) -> list[dict]:
    """Generate a realistic match schedule for one student over 2 years."""
    matches      = []
    competition  = student["competition_level"]

    # Competitive players play more matches
    if competition == "competitive":
        matches_per_month = random.randint(2, 6)
    else:
        matches_per_month = random.randint(0, 2)

    current = START_DATE
    while current <= END_DATE:
        month_matches = random.randint(
            max(0, matches_per_month - 2),
            matches_per_month + 2
        )
        for _ in range(month_matches):
            day_offset = random.randint(0, 27)
            match_date = current + timedelta(days=day_offset)
            if match_date <= END_DATE:
                matches.append(generate_match(student, match_date))
        # Move to next month
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)

    return matches


def main():
    students_file = OUTPUT_DIR / "students.csv"
    if not students_file.exists():
        print("ERROR: students.csv not found. Run generate_students.py first.")
        return

    students = pd.read_csv(students_file).to_dict("records")
    print(f"Loaded {len(students)} students")
    print("Generating match records...")

    all_matches = []
    for student in students:
        matches = generate_match_schedule(student)
        all_matches.extend(matches)

    df = pd.DataFrame(all_matches)
    df["match_date"] = pd.to_datetime(df["match_date"])
    df = df.sort_values("match_date").reset_index(drop=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)

    print(f"\nSaved {len(df):,} match records to {OUTPUT_FILE}")
    print("\n── Summary ──────────────────────────────────────")
    print(f"Date range:     {df['match_date'].min().date()} → {df['match_date'].max().date()}")
    print(f"Unique players: {df['player_id'].nunique()}")
    print(f"Win rate:       {df['player_won'].mean():.1%}")
    print(f"Avg winners:    {df['winners'].mean():.1f}")
    print(f"Avg errors:     {df['unforced_errors'].mean():.1f}")
    print(f"Avg rally len:  {df['avg_rally_length'].mean():.1f} shots")
    print("─────────────────────────────────────────────────")


if __name__ == "__main__":
    main()
