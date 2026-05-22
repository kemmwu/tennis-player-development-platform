import uuid
import random
import pandas as pd
from datetime import date, timedelta
from pathlib import Path

random.seed(123)

# ── CONFIG ────────────────────────────────────────────────────────
OUTPUT_DIR  = Path(__file__).parent.parent / "data" / "synthetic"
OUTPUT_FILE = OUTPUT_DIR / "training_sessions.parquet"
START_DATE  = date(2024, 1, 1)
END_DATE    = date(2025, 12, 31)
# ─────────────────────────────────────────────────────────────────

SESSION_TYPES = [
    "Baseline Rally",
    "Serve Practice",
    "Volleys and Net Play",
    "Return of Serve",
    "Match Play",
    "Fitness and Footwork",
    "Slice and Drop Shot",
    "Topspin Groundstrokes",
]

DRILLS = {
    "Baseline Rally":          "Cross-court consistency drill · Down-the-line targets · Rally depth challenge",
    "Serve Practice":          "First serve placement · Second serve kick · Serve and recover",
    "Volleys and Net Play":    "Approach shot drill · Reflex volleys · Overhead smash practice",
    "Return of Serve":         "Return positioning · Return direction control · Chip and charge",
    "Match Play":              "Full point play · Tiebreak practice · Pressure point simulation",
    "Fitness and Footwork":    "Cone agility drills · Split-step timing · Side shuffle ladders",
    "Slice and Drop Shot":     "Backhand slice consistency · Drop shot placement · Defensive slice",
    "Topspin Groundstrokes":   "Heavy topspin looping · Angle creation · High ball handling",
}


def utr_to_shot_accuracy(utr: float, base_low: float, base_high: float) -> float:
    scale = utr / 16.0
    value = base_low + scale * (base_high - base_low)
    noise = random.uniform(-0.04, 0.04)
    return round(min(max(value + noise, 0.0), 1.0), 3)


def improvement_factor(session_date: date) -> float:
    days_in = (session_date - START_DATE).days
    total   = (END_DATE - START_DATE).days
    return days_in / total * 0.10   # up to 10% improvement over 2 years


def generate_session(student: dict, session_date: date) -> dict:
    utr     = student["utr_rating"]
    improve = improvement_factor(session_date)
    eff_utr = utr * (1 + improve)

    session_type = random.choice(SESSION_TYPES)

    # Heart rate: fitness sessions higher, baseline lower
    if session_type == "Fitness and Footwork":
        avg_hr = random.randint(145, 175)
    elif session_type == "Match Play":
        avg_hr = random.randint(130, 165)
    else:
        avg_hr = random.randint(110, 150)

    # Shot accuracy: correlated with UTR
    fh_cc_in   = utr_to_shot_accuracy(eff_utr, 0.45, 0.82)
    fh_dtl_in  = utr_to_shot_accuracy(eff_utr, 0.40, 0.78)
    bh_cc_in   = utr_to_shot_accuracy(eff_utr, 0.42, 0.80)
    bh_dtl_in  = utr_to_shot_accuracy(eff_utr, 0.38, 0.75)

    # Depth: harder than accuracy, slightly lower
    fh_cc_deep  = round(fh_cc_in  * random.uniform(0.65, 0.85), 3)
    fh_dtl_deep = round(fh_dtl_in * random.uniform(0.65, 0.85), 3)
    bh_cc_deep  = round(bh_cc_in  * random.uniform(0.65, 0.85), 3)
    bh_dtl_deep = round(bh_dtl_in * random.uniform(0.65, 0.85), 3)

    # Speeds: forehand typically faster than backhand
    fh_cc_spd  = int(90 + eff_utr * 4 + random.randint(-10, 10))
    fh_dtl_spd = int(fh_cc_spd + random.randint(-5, 10))
    bh_cc_spd  = int(fh_cc_spd * 0.88 + random.randint(-8, 8))
    bh_dtl_spd = int(bh_cc_spd + random.randint(-5, 8))

    # Overall session stats
    shots_in            = utr_to_shot_accuracy(eff_utr, 0.50, 0.85)
    longest_rally       = int(max(3, random.gauss(8 + eff_utr * 0.8, 3)))
    rallies_above_5     = utr_to_shot_accuracy(eff_utr, 0.20, 0.55)
    avg_ball_speed      = round(85 + eff_utr * 4 + random.uniform(-10, 10), 1)
    max_ball_speed      = round(avg_ball_speed + random.uniform(15, 35), 1)

    return {
        "session_id":                       str(uuid.uuid4()),
        "player_id":                        student["student_id"],
        "session_date":                     session_date.isoformat(),
        "session_type":                     session_type,
        "drills_completed":                 DRILLS[session_type],
        "raw_note_text":                    None,
        "avg_heart_rate":                   avg_hr,
        "ingested_at":                      session_date.isoformat(),
        "shots_in":                         shots_in,
        "longest_rally":                    longest_rally,
        "rallies_above_5_shots":            rallies_above_5,
        "forehand_cross_court_in":          fh_cc_in,
        "forehand_down_the_line_in":        fh_dtl_in,
        "forehand_avg_cross_court_speed":   fh_cc_spd,
        "forehand_avg_down_the_line_speed": fh_dtl_spd,
        "forehand_cross_court_deep":        fh_cc_deep,
        "forehand_down_the_line_deep":      fh_dtl_deep,
        "backhand_cross_court_in":          bh_cc_in,
        "backhand_down_the_line_in":        bh_dtl_in,
        "backhand_avg_cross_court_speed":   bh_cc_spd,
        "backhand_avg_down_the_line_speed": bh_dtl_spd,
        "backhand_cross_court_deep":        bh_cc_deep,
        "backhand_down_the_line_deep":      bh_dtl_deep,
        "avg_ball_speed":                   avg_ball_speed,
        "max_ball_speed":                   max_ball_speed,
    }


def generate_session_schedule(student: dict) -> list[dict]:
    sessions    = []
    freq        = int(student["training_frequency_per_week"])

    current = START_DATE
    while current <= END_DATE:
        # Generate sessions for this week
        week_sessions = random.randint(
            max(0, freq - 1),
            freq + 1
        )
        used_days = set()
        for _ in range(week_sessions):
            day = random.randint(0, 6)
            # Avoid duplicate days in same week
            attempts = 0
            while day in used_days and attempts < 10:
                day = random.randint(0, 6)
                attempts += 1
            used_days.add(day)
            session_date = current + timedelta(days=day)
            if session_date <= END_DATE:
                sessions.append(generate_session(student, session_date))
        # Move to next week
        current += timedelta(days=7)

    return sessions


def main():
    students_file = OUTPUT_DIR / "students.csv"
    if not students_file.exists():
        print("ERROR: students.csv not found. Run generate_students.py first.")
        return

    students = pd.read_csv(students_file).to_dict("records")
    print(f"Loaded {len(students)} students")
    print("Generating training sessions...")

    all_sessions = []
    for student in students:
        sessions = generate_session_schedule(student)
        all_sessions.extend(sessions)

    df = pd.DataFrame(all_sessions)
    df["session_date"] = pd.to_datetime(df["session_date"])
    df = df.sort_values("session_date").reset_index(drop=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)

    print(f"\nSaved {len(df):,} training sessions to {OUTPUT_FILE}")
    print("\n── Summary ──────────────────────────────────────────────")
    print(f"Date range:        {df['session_date'].min().date()} → {df['session_date'].max().date()}")
    print(f"Unique players:    {df['player_id'].nunique()}")
    print(f"Avg heart rate:    {df['avg_heart_rate'].mean():.0f} bpm")
    print(f"Avg shots in:      {df['shots_in'].mean():.1%}")
    print(f"Avg longest rally: {df['longest_rally'].mean():.1f} shots")
    print(f"\nSession types:\n{df['session_type'].value_counts().to_string()}")
    print("─────────────────────────────────────────────────────────")


if __name__ == "__main__":
    main()
