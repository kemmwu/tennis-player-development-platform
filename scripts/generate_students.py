import uuid
import random
import pandas as pd
from faker import Faker
from datetime import datetime, date, timedelta
from pathlib import Path

fake = Faker(['en_US', 'zh_CN'])
random.seed(42)

# ── CONFIG ────────────────────────────────────────────────────────
NUM_STUDENTS = 50
OUTPUT_DIR   = Path(__file__).parent.parent / "data" / "synthetic"
OUTPUT_FILE  = OUTPUT_DIR / "students.csv"
COACH_IDS    = ["coach_kem", "coach_zhang", "coach_li"]
# ─────────────────────────────────────────────────────────────────


def generate_age_group(dob: date) -> str:
    age = (date.today() - dob).days // 365
    if age < 10:
        return "U10"
    elif age < 12:
        return "U12"
    elif age < 14:
        return "U14"
    elif age < 16:
        return "U16"
    elif age < 18:
        return "U18"
    else:
        return "Adult"


def generate_utr(age_group: str, years_playing: int) -> float:
    """UTR ranges by age group and experience."""
    base_ranges = {
        "U10":   (1.0, 4.0),
        "U12":   (2.0, 6.0),
        "U14":   (3.0, 9.0),
        "U16":   (4.0, 11.0),
        "U18":   (4.0, 13.0),
        "Adult": (2.0, 14.0),
    }
    low, high = base_ranges.get(age_group, (2.0, 10.0))
    # More years playing = skewed higher
    skew = min(years_playing * 0.3, (high - low) * 0.5)
    return round(random.uniform(low + skew, high), 1)


def generate_height(age_group: str, dob: date) -> str:
    age = (date.today() - dob).days // 365
    if age < 10:
        base = random.randint(120, 140)
    elif age < 14:
        base = random.randint(140, 165)
    elif age < 18:
        base = random.randint(155, 185)
    else:
        base = random.randint(160, 195)
    return f"{base}cm"


def generate_goals(competition_level: str) -> str:
    competitive_goals = [
        "Reach top 10 in USTA sectionals",
        "Qualify for national championships",
        "Earn a college tennis scholarship",
        "Improve UTR rating to 10+",
        "Win local tournament circuit",
        "Develop a consistent serve and volley game",
    ]
    recreational_goals = [
        "Stay fit and enjoy the game",
        "Improve consistency on groundstrokes",
        "Learn proper technique from scratch",
        "Play social tennis on weekends",
        "Improve serve accuracy",
        "Have fun and meet new people",
    ]
    goals = competitive_goals if competition_level == "competitive" else recreational_goals
    return random.choice(goals)


def generate_injury_history() -> str:
    injuries = [
        "None",
        "None",
        "None",
        "Minor right shoulder strain, fully recovered",
        "Left knee tendinitis, managed with physio",
        "Right wrist sprain 2023, fully recovered",
        "Tennis elbow (right), ongoing management",
        "Lower back tightness, no structural issues",
        "Ankle sprain 2024, fully recovered",
    ]
    return random.choice(injuries)


def generate_previous_coaching() -> str:
    options = [
        "None",
        "None",
        "Group lessons at local club for 1 year",
        "Private coaching 3 years with local coach",
        "School tennis program",
        "USTA junior development program",
        "Self-taught with online videos",
        "College recreational team",
        "Club coaching in China for 2 years",
    ]
    return random.choice(options)


def generate_student(index: int) -> dict:
    # Decide if this student is Chinese or Western background
    is_chinese = random.random() < 0.4

    if is_chinese:
        fake_cn = Faker('zh_CN')
        full_name_cn = fake_cn.name()
        chinese_name = full_name_cn
        # Romanized version
        first_names = ["Alex", "Kevin", "Amy", "Jessica", "Michael",
                       "Emily", "David", "Sarah", "Jason", "Lisa",
                       "Brian", "Michelle", "Ryan", "Tina", "Eric"]
        last_names  = ["Chen", "Wang", "Li", "Zhang", "Liu",
                       "Yang", "Wu", "Zhao", "Sun", "Zhou"]
        full_name   = f"{random.choice(first_names)} {random.choice(last_names)}"
        preferred   = full_name.split()[0]
    else:
        chinese_name = None
        full_name    = fake.name()
        preferred    = full_name.split()[0]

    # Demographics
    competition_level = random.choices(
        ["competitive", "recreational"],
        weights=[0.6, 0.4]
    )[0]

    years_playing = random.randint(1, 12)

    # Age: mix of juniors and adults
    if random.random() < 0.65:
        # Junior: 8–17
        age_years = random.randint(8, 17)
        dob = date.today() - timedelta(days=age_years * 365 + random.randint(0, 364))
    else:
        # Adult: 18–45
        age_years = random.randint(18, 45)
        dob = date.today() - timedelta(days=age_years * 365 + random.randint(0, 364))

    age_group = generate_age_group(dob)
    utr       = generate_utr(age_group, years_playing)
    height    = generate_height(age_group, dob)

    # SCD Type 2 fields — all current on generation
    valid_from = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))

    # Contact info
    if age_group in ["U10", "U12", "U14", "U16", "U18"]:
        contact_name  = fake.name()
        contact_email = fake.email()
    else:
        # Adult — own contact
        contact_name  = full_name
        contact_email = fake.email()

    return {
        "student_id":                  str(uuid.uuid4()),
        "intake_id":                   str(uuid.uuid4()),
        "full_name":                   full_name,
        "chinese_name":                chinese_name,
        "preferred_name":              preferred,
        "date_of_birth":               dob.isoformat(),
        "utr_rating":                  utr,
        "age_group":                   age_group,
        "training_frequency_per_week": random.randint(1, 5),
        "dominant_hand":               random.choices(["Right", "Left"], weights=[0.88, 0.12])[0],
        "height":                      height,
        "years_playing":               years_playing,
        "coach_id":                    random.choice(COACH_IDS),
        "submitted_at":                valid_from.isoformat(),
        "goals":                       generate_goals(competition_level),
        "injury_history":              generate_injury_history(),
        "previous_coaching":           generate_previous_coaching(),
        "competition_level":           competition_level,
        "contact_name":                contact_name,
        "contact_email":               contact_email,
        "kafka_offset":                index,
        "kafka_partition":             index % 3,
        "ingested_at":                 valid_from.isoformat(),
        "is_active":                   random.choices([True, False], weights=[0.9, 0.1])[0],
        "valid_from":                  valid_from.isoformat(),
        "valid_to":                    None,
        "is_current":                  True,
    }


def main():
    print(f"Generating {NUM_STUDENTS} students...")

    students = [generate_student(i) for i in range(NUM_STUDENTS)]
    df       = pd.DataFrame(students)

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Save CSV
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved {len(df)} students to {OUTPUT_FILE}")

    # Print a quick summary
    print("\n── Summary ──────────────────────────────")
    print(f"Age groups:\n{df['age_group'].value_counts().to_string()}")
    print(f"\nCompetition level:\n{df['competition_level'].value_counts().to_string()}")
    print(f"\nUTR range: {df['utr_rating'].min()} – {df['utr_rating'].max()}")
    print(f"Avg UTR:   {df['utr_rating'].mean():.1f}")
    print(f"Chinese name present: {df['chinese_name'].notna().sum()} students")
    print("─────────────────────────────────────────")


if __name__ == "__main__":
    main()

