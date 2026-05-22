# Databricks notebook source
# extract_sessions.py
# Reads session groups from bronze.raw_screenshots
# Sends each session to Claude API for extraction
# Writes results to bronze.raw_match_extractions or bronze.raw_training_sessions

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI Extraction: SwingVision Sessions
# MAGIC
# MAGIC **Source:** `tennis_dev.bronze.raw_screenshots`
# MAGIC **Target:** `tennis_dev.bronze.raw_match_extractions`
# MAGIC **Target:** `tennis_dev.bronze.raw_training_sessions`
# MAGIC **Model:** claude-sonnet-4

# COMMAND ----------

%pip install requests  # noqa: E999

# COMMAND ----------

import json
import base64
import requests
from datetime import datetime
from pyspark.sql import functions as F
import pandas as pd

# COMMAND ----------

# ── CONFIGURATION ─────────────────────────────────────────────────
CATALOG              = "tennis_dev"
SCHEMA               = "bronze"
SCREENSHOTS_TABLE    = f"{CATALOG}.{SCHEMA}.raw_screenshots"
MATCH_TABLE          = f"{CATALOG}.{SCHEMA}.raw_match_extractions"
TRAINING_TABLE       = f"{CATALOG}.{SCHEMA}.raw_training_sessions"
FAILURES_TABLE       = f"{CATALOG}.{SCHEMA}.extraction_failures"
SCREENSHOTS_PATH     = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files/screenshots/"
CONFIDENCE_THRESHOLD = 0.8
PROMPT_VERSION       = "v1.0"
MAX_RETRIES          = 3
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 1: LOAD API KEY ──────────────────────────────────────────
ANTHROPIC_API_KEY = dbutils.secrets.get(scope="tennis", key="anthropic_api_key")


def call_claude(content: list) -> str:
    """Call Claude API directly via requests."""
    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key":          ANTHROPIC_API_KEY,
            "anthropic-version":  "2023-06-01",
            "content-type":       "application/json"
        },
        json={
            "model": "claude-sonnet-4-6",
            "max_tokens": 2000,
            "system":     SYSTEM_PROMPT,
            "messages":   [{"role": "user", "content": content}]
        },
        timeout=120
    )
    if not response.ok:
        print(f"  API Error {response.status_code}: {response.text}")
    response.raise_for_status()
    return response.json()["content"][0]["text"]


print("Claude API client ready.")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 2: GET UNPROCESSED SESSIONS ──────────────────────────────
screenshots_df = spark.table(SCREENSHOTS_TABLE)

if spark.catalog.tableExists(MATCH_TABLE):
    processed_matches = (
        spark.table(MATCH_TABLE)
        .select("match_id")
        .withColumnRenamed("match_id", "session_id")
    )
else:
    processed_matches = spark.createDataFrame([], "session_id STRING")

if spark.catalog.tableExists(TRAINING_TABLE):
    processed_training = spark.table(TRAINING_TABLE).select("session_id")
else:
    processed_training = spark.createDataFrame([], "session_id STRING")

processed = processed_matches.union(processed_training).distinct()

unprocessed = (
    screenshots_df
    .join(processed, on="session_id", how="left_anti")
    .groupBy("session_id", "player_name", "session_date",
             "session_time", "session_type")
    .agg(
        F.collect_list("_source_file").alias("file_paths"),
        F.count("*").alias("page_count")
    )
    .orderBy("session_date", "session_id")
)

sessions = unprocessed.collect()
print(f"Found {len(sessions)} unprocessed sessions to extract")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 3: EXTRACTION PROMPT ─────────────────────────────────────
SYSTEM_PROMPT = """You are an expert tennis data extraction assistant.
You will receive one or more SwingVision screenshots from a single tennis session.
Your job is to:
1. Identify what type each screenshot is (match_summary, serve_stats,
   return_stats, rally_stats, training_stats, coaching_notes, other)
2. Extract all available statistics from stats screenshots
3. Extract coaching notes text from notes screenshots
4. Return a single JSON object combining all pages

For match sessions extract:
- winners, unforced_errors, forehand_winners, forehand_unforced_errors,
  backhand_winners, backhand_unforced_errors
- total_points_won, total_points_played
- break_points_won, break_points_total, break_points_saved, break_points_saved_total
- aces, service_winners, double_faults
- first_serves_in, first_serves_total, second_serves_in, second_serves_total
- serve_points_won, serve_points_total
- first_serves_won, first_serves_won_total, second_serves_won, second_serves_won_total
- return_points_won, return_points_total
- first_returns_won, first_returns_total, second_returns_won, second_returns_total
- rallies_1_4_won, rallies_1_4_total, rallies_5_8_won, rallies_5_8_total,
  rallies_9plus_won, rallies_9plus_total
- score (e.g. "6-4 6-3"), player_won (true/false)
- opponent_name if visible

For training sessions extract:
- shots_in (as decimal 0-1), shots_per_hour, longest_rally, rallies_above_5_shots
- serves_in_ad, serves_in_deuce, avg_serve_speed_ad, avg_serve_speed_deuce
- returns_in_ad, returns_in_deuce, avg_return_speed_ad, avg_return_speed_deuce
- forehand_cross_court_in, forehand_down_the_line_in
- forehand_avg_cross_court_speed, forehand_avg_down_the_line_speed
- forehand_cross_court_deep, forehand_down_the_line_deep
- backhand_cross_court_in, backhand_down_the_line_in
- backhand_avg_cross_court_speed, backhand_avg_down_the_line_speed
- backhand_cross_court_deep, backhand_down_the_line_deep

For coaching notes extract:
- raw_note_text: full text of the notes
- technique: primary technique mentioned
- issue: problem identified (or null)
- recommendation: coach recommendation (or null)
- sentiment: "improving", "needs_work", or "neutral"

Return ONLY valid JSON with no explanation text before or after.
Set confidence (0.0-1.0) based on image clarity and data completeness.
Set null for any field not visible in the screenshots.

Use this exact JSON structure:
{
  "session_type": "match" or "training",
  "screenshot_types_found": ["list of types identified"],
  "pages_processed": number,
  "stats": { all stat fields here },
  "coaching_notes": {
    "raw_note_text": "...",
    "technique": "...",
    "issue": "..." or null,
    "recommendation": "..." or null,
    "sentiment": "improving" or "needs_work" or "neutral"
  },
  "confidence": 0.0 to 1.0
}"""
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 4: EXTRACTION FUNCTIONS ──────────────────────────────────
def load_image_as_base64(file_path: str) -> str:
    """Load image from Volume and encode as base64."""
    # Strip dbfs: prefix — Python open() needs /Volumes/... not dbfs:/Volumes/...
    clean_path = file_path.replace("dbfs:", "")
    with open(clean_path, "rb") as f:
        return base64.standard_b64encode(f.read()).decode("utf-8")


def extract_session(session: dict, retries: int = 0) -> dict:
    """Send all screenshots for a session to Claude API."""
    try:
        content = []
        for file_path in sorted(session["file_paths"]):
            img_data = load_image_as_base64(file_path)
            content.append({
                "type": "image",
                "source": {
                    "type":       "base64",
                    "media_type": "image/png",
                    "data":       img_data
                }
            })

        content.append({
            "type": "text",
            "text": (
                f"Extract all statistics from these "
                f"{len(session['file_paths'])} SwingVision screenshots "
                f"for a {session['session_type']} session.\n"
                f"Player: {session['player_name']}\n"
                f"Date: {session['session_date']}\n"
                f"Session ID: {session['session_id']}\n\n"
                f"Return JSON only. Follow the exact structure in the system prompt."
            )
        })

        raw_text = call_claude(content).strip()

        # Strip markdown code fences if present
        if raw_text.startswith("```"):
            lines = raw_text.split("\n")
            raw_text = "\n".join(
                line for line in lines
                if not line.strip().startswith("```")
            )

        result                   = json.loads(raw_text.strip())
        result["session_id"]     = session["session_id"]
        result["player_name"]    = session["player_name"]
        result["session_date"]   = str(session["session_date"])
        result["session_time"]   = session["session_time"]
        result["source_file"]    = session["session_id"]
        result["prompt_version"] = PROMPT_VERSION
        result["extracted_at"]   = datetime.now().isoformat()
        return result

    except Exception as e:
        if retries < MAX_RETRIES:
            print(f"  Retry {retries + 1} for {session['session_id']}: {e}")
            return extract_session(session, retries + 1)
        return {
            "session_id":  session["session_id"],
            "error":       str(e),
            "source_file": session["session_id"],
            "failed_at":   datetime.now().isoformat()
        }
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 5: RUN EXTRACTION ────────────────────────────────────────
match_records    = []
training_records = []
failure_records  = []

for i, session in enumerate(sessions):
    session_dict = session.asDict()
    print(f"[{i+1}/{len(sessions)}] Extracting {session_dict['session_id']}...")

    result = extract_session(session_dict)

    if "error" in result:
        print(f"  FAILED: {result['error']}")
        failure_records.append({
            "failure_id":       f"fail_{session_dict['session_id']}",
            "source_file":      session_dict["session_id"],
            "error_reason":     "extraction_failed",
            "error_message":    result["error"],
            "retry_count":      MAX_RETRIES,
            "pipeline_version": PROMPT_VERSION,
            "failed_at":        result["failed_at"]
        })
        continue

    confidence = result.get("confidence", 0.0)
    print(f"  OK confidence={confidence:.2f} pages={result.get('pages_processed', '?')}")

    stats = result.get("stats", {})
    notes = result.get("coaching_notes", {})

    if session_dict["session_type"] == "match":
        match_records.append({
            "match_id":                 session_dict["session_id"],
            "player_id":                session_dict["player_name"],
            "match_date":               str(session_dict["session_date"]),
            "session_time":             result["session_time"],
            "opponent_name":            stats.get("opponent_name"),
            "opponent_utr":             None,
            "tournament_name":          None,
            "surface":                  None,
            "score":                    stats.get("score"),
            "player_won":               stats.get("player_won"),
            "winners":                  stats.get("winners"),
            "unforced_errors":          stats.get("unforced_errors"),
            "forehand_winners":         stats.get("forehand_winners"),
            "forehand_unforced_errors": stats.get("forehand_unforced_errors"),
            "backhand_winners":         stats.get("backhand_winners"),
            "backhand_unforced_errors": stats.get("backhand_unforced_errors"),
            "total_points_won":         stats.get("total_points_won"),
            "total_points_played":      stats.get("total_points_played"),
            "break_points_won":         stats.get("break_points_won"),
            "break_points_total":       stats.get("break_points_total"),
            "break_points_saved":       stats.get("break_points_saved"),
            "break_points_saved_total": stats.get("break_points_saved_total"),
            "aces":                     stats.get("aces"),
            "service_winners":          stats.get("service_winners"),
            "double_faults":            stats.get("double_faults"),
            "first_serves_in":          stats.get("first_serves_in"),
            "first_serves_total":       stats.get("first_serves_total"),
            "second_serves_in":         stats.get("second_serves_in"),
            "second_serves_total":      stats.get("second_serves_total"),
            "serve_points_won":         stats.get("serve_points_won"),
            "serve_points_total":       stats.get("serve_points_total"),
            "first_serves_won":         stats.get("first_serves_won"),
            "first_serves_won_total":   stats.get("first_serves_won_total"),
            "second_serves_won":        stats.get("second_serves_won"),
            "second_serves_won_total":  stats.get("second_serves_won_total"),
            "return_points_won":        stats.get("return_points_won"),
            "return_points_total":      stats.get("return_points_total"),
            "first_returns_won":        stats.get("first_returns_won"),
            "first_returns_total":      stats.get("first_returns_total"),
            "second_returns_won":       stats.get("second_returns_won"),
            "second_returns_total":     stats.get("second_returns_total"),
            "rallies_1_4_won":          stats.get("rallies_1_4_won"),
            "rallies_1_4_total":        stats.get("rallies_1_4_total"),
            "rallies_5_8_won":          stats.get("rallies_5_8_won"),
            "rallies_5_8_total":        stats.get("rallies_5_8_total"),
            "rallies_9plus_won":        stats.get("rallies_9plus_won"),
            "rallies_9plus_total":      stats.get("rallies_9plus_total"),
            "raw_note_text":            notes.get("raw_note_text") if notes else None,
            "pages_processed":          result.get("pages_processed"),
            "source_file":              result["source_file"],
            "extraction_confidence":    confidence,
            "prompt_version":           PROMPT_VERSION,
            "_ingested_at":             datetime.now().isoformat()
        })
    else:
        training_records.append({
            "session_id":                       session_dict["session_id"],
            "player_id":                        session_dict["player_name"],
            "session_date":                     str(session_dict["session_date"]),
            "session_time":                     result["session_time"],
            "session_type":                     "training",
            "drills_completed":                 None,
            "raw_note_text":                    notes.get("raw_note_text") if notes else None,
            "shots_in":                         stats.get("shots_in"),
            "shots_per_hour":                   stats.get("shots_per_hour"),
            "longest_rally":                    stats.get("longest_rally"),
            "rallies_above_5_shots":            stats.get("rallies_above_5_shots"),
            "serves_in_ad":                     stats.get("serves_in_ad"),
            "serves_in_deuce":                  stats.get("serves_in_deuce"),
            "avg_serve_speed_ad":               stats.get("avg_serve_speed_ad"),
            "avg_serve_speed_deuce":            stats.get("avg_serve_speed_deuce"),
            "returns_in_ad":                    stats.get("returns_in_ad"),
            "returns_in_deuce":                 stats.get("returns_in_deuce"),
            "avg_return_speed_ad":              stats.get("avg_return_speed_ad"),
            "avg_return_speed_deuce":           stats.get("avg_return_speed_deuce"),
            "forehand_cross_court_in":          stats.get("forehand_cross_court_in"),
            "forehand_down_the_line_in":        stats.get("forehand_down_the_line_in"),
            "forehand_avg_cross_court_speed":   stats.get("forehand_avg_cross_court_speed"),
            "forehand_avg_down_the_line_speed": stats.get("forehand_avg_down_the_line_speed"),
            "forehand_cross_court_deep":        stats.get("forehand_cross_court_deep"),
            "forehand_down_the_line_deep":      stats.get("forehand_down_the_line_deep"),
            "backhand_cross_court_in":          stats.get("backhand_cross_court_in"),
            "backhand_down_the_line_in":        stats.get("backhand_down_the_line_in"),
            "backhand_avg_cross_court_speed":   stats.get("backhand_avg_cross_court_speed"),
            "backhand_avg_down_the_line_speed": stats.get("backhand_avg_down_the_line_speed"),
            "backhand_cross_court_deep":        stats.get("backhand_cross_court_deep"),
            "backhand_down_the_line_deep":      stats.get("backhand_down_the_line_deep"),
            "pages_processed":                  result.get("pages_processed"),
            "source_file":                      result["source_file"],
            "extraction_confidence":            confidence,
            "prompt_version":                   PROMPT_VERSION,
            "_ingested_at":                     datetime.now().isoformat()
        })

print(f"\nExtraction complete:")
print(f"  Match records:    {len(match_records)}")
print(f"  Training records: {len(training_records)}")
print(f"  Failures:         {len(failure_records)}")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 6: WRITE RESULTS TO BRONZE ───────────────────────────────

if match_records:
    match_df = spark.createDataFrame(pd.DataFrame(match_records))
    if spark.catalog.tableExists(MATCH_TABLE):
        match_df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(MATCH_TABLE)
    else:
        match_df.write.format("delta").mode("overwrite").saveAsTable(MATCH_TABLE)
    print(f"Wrote {len(match_records)} match records to {MATCH_TABLE}")

if training_records:
    training_df = spark.createDataFrame(pd.DataFrame(training_records))
    if spark.catalog.tableExists(TRAINING_TABLE):
        training_df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(TRAINING_TABLE)
    else:
        training_df.write.format("delta").mode("overwrite").saveAsTable(TRAINING_TABLE)
    print(f"Wrote {len(training_records)} training records to {TRAINING_TABLE}")

if failure_records:
    failures_df = spark.createDataFrame(pd.DataFrame(failure_records))
    if spark.catalog.tableExists(FAILURES_TABLE):
        failures_df.write.format("delta").mode("append").saveAsTable(FAILURES_TABLE)
    else:
        failures_df.write.format("delta").mode("overwrite").saveAsTable(FAILURES_TABLE)
    print(f"Wrote {len(failure_records)} failures to {FAILURES_TABLE}")

# COMMAND ----------

# ── STEP 7: VERIFY ────────────────────────────────────────────────
if spark.catalog.tableExists(MATCH_TABLE):
    m = spark.table(MATCH_TABLE)
    print(f"\n── bronze.raw_match_extractions ─────────────────")
    print(f"Total records:  {m.count():,}")
    avg_conf = m.agg({"extraction_confidence": "avg"}).collect()[0][0]
    print(f"Avg confidence: {avg_conf:.2f}" if avg_conf else "Avg confidence: N/A")
    display(m.select(
        "match_id", "player_id", "match_date",
        "winners", "first_serves_in", "first_serves_total",
        "extraction_confidence"
    ).orderBy("match_date"))

if spark.catalog.tableExists(TRAINING_TABLE):
    t = spark.table(TRAINING_TABLE)
    print(f"\n── bronze.raw_training_sessions ─────────────────")
    print(f"Total records:  {t.count():,}")
    avg_conf = t.agg({"extraction_confidence": "avg"}).collect()[0][0]
    print(f"Avg confidence: {avg_conf:.2f}" if avg_conf else "Avg confidence: N/A")
    display(t.select(
        "session_id", "player_id", "session_date",
        "shots_in", "forehand_cross_court_in",
        "extraction_confidence"
    ).orderBy("session_date"))

if spark.catalog.tableExists(FAILURES_TABLE):
    f = spark.table(FAILURES_TABLE)
    if f.count() > 0:
        print(f"\n── bronze.extraction_failures ───────────────────")
        print(f"Total failures: {f.count()}")
        display(f)
# ──────────────────────────────────────────────────────────────────
