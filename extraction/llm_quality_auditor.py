# Databricks notebook source
# llm_quality_auditor.py
# Weekly job: compares coaching notes vs match statistics for semantic consistency
# Flags inconsistencies for coach review
# Output: tennis_dev.gold.llm_quality_findings

# COMMAND ----------

# MAGIC %md
# MAGIC ## LLM Quality Auditor
# MAGIC
# MAGIC **Purpose:** Detect semantic inconsistencies between coaching notes and match data
# MAGIC **Schedule:** Weekly
# MAGIC **Output:** `tennis_dev.gold.llm_quality_findings`

# COMMAND ----------

%pip install requests

# COMMAND ----------

import json
import uuid
import requests
from datetime import datetime, date, timedelta
from pyspark.sql import functions as F
import pandas as pd

# COMMAND ----------

# ── CONFIGURATION ─────────────────────────────────────────────────
CATALOG         = "tennis_dev"
FINDINGS_TABLE  = f"{CATALOG}.gold.llm_quality_findings"
ANTHROPIC_KEY   = dbutils.secrets.get(scope="tennis", key="anthropic_api_key")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── CLAUDE CLIENT ─────────────────────────────────────────────────
def call_claude(prompt: str) -> str:
    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key":         ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json"
        },
        json={
            "model":      "claude-sonnet-4-6",
            "max_tokens": 1000,
            "messages":   [{"role": "user", "content": prompt}]
        },
        timeout=60
    )
    response.raise_for_status()
    return response.json()["content"][0]["text"]
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 1: LOAD RECENT DATA ──────────────────────────────────────

# Load match stats with notes
matches_df = spark.table(f"{CATALOG}.bronze.raw_match_extractions") \
    .filter(F.col("raw_note_text").isNotNull()) \
    .select(
        "match_id", "player_id", "match_date",
        "winners", "unforced_errors",
        "first_serves_in", "first_serves_total",
        "break_points_won", "break_points_total",
        "raw_note_text"
    )

# Load training sessions with notes
training_df = spark.table(f"{CATALOG}.bronze.raw_training_sessions") \
    .filter(F.col("raw_note_text").isNotNull()) \
    .select(
        "session_id", "player_id", "session_date",
        "shots_in", "forehand_cross_court_in",
        "backhand_cross_court_in",
        "raw_note_text"
    )

matches  = matches_df.collect()
sessions = training_df.collect()

print(f"Loaded {len(matches)} match records with notes")
print(f"Loaded {len(sessions)} training records with notes")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 2: DEFINE INCONSISTENCY CHECKS ───────────────────────────
# Five types of semantic inconsistencies to detect

AUDITOR_PROMPT = """You are a tennis data quality auditor.
I will give you a coaching note and the corresponding session statistics.
Identify any semantic inconsistencies — cases where the note contradicts the data.

Five inconsistency types to check:
1. PERFORMANCE_CONTRADICTION: Note says improving but stats show decline (or vice versa)
2. TECHNIQUE_STAT_MISMATCH: Note mentions a specific technique issue but relevant stats look good
3. MISSING_CONTEXT: Note references an event (e.g. "won the tiebreak") not supported by stats
4. OVERSTATEMENT: Note says "excellent" or "perfect" but stats are average or below
5. UNDERSTATEMENT: Note says "struggled" or "poor" but stats are actually strong

Return JSON only:
{
  "has_inconsistency": true or false,
  "inconsistency_type": "one of the five types above or null",
  "severity": "high" or "medium" or "low" or null,
  "description": "one sentence explaining the inconsistency or null",
  "note_excerpt": "the specific part of the note that conflicts",
  "stat_evidence": "the specific stat that contradicts it"
}

Important: Return only ONE JSON object representing the single most important inconsistency found.
Do not return arrays or multiple findings.

If no inconsistency found, return has_inconsistency: false and null for all other fields."""


def audit_match(match: dict) -> dict:
    """Check one match record for note vs stats inconsistency."""
    first_serve_pct = None
    if match["first_serves_total"] and match["first_serves_total"] > 0:
        first_serve_pct = round(
            match["first_serves_in"] / match["first_serves_total"], 2
        )

    bp_pct = None
    if match["break_points_total"] and match["break_points_total"] > 0:
        bp_pct = round(match["break_points_won"] / match["break_points_total"], 2)

    prompt = f"""Coaching note:
"{match['raw_note_text']}"

Match statistics:
- Winners: {match['winners']}
- Unforced errors: {match['unforced_errors']}
- First serve %: {first_serve_pct if first_serve_pct else 'N/A'}
- Break point conversion: {bp_pct if bp_pct else 'N/A'}

Check for semantic inconsistencies between the note and the stats."""

    try:
        raw = call_claude(AUDITOR_PROMPT + "\n\n" + prompt).strip()
        if raw.startswith("```"):
            raw = "\n".join(
                l for l in raw.split("\n")
                if not l.strip().startswith("```")
            )
        result = json.loads(raw.strip())
        result["session_id"]   = match["match_id"]
        result["session_type"] = "match"
        result["player_id"]    = match["player_id"]
        result["session_date"] = str(match["match_date"])
        return result
    except Exception as e:
        return {
            "session_id":         match["match_id"],
            "session_type":       "match",
            "player_id":          match["player_id"],
            "session_date":       str(match["match_date"]),
            "has_inconsistency":  False,
            "error":              str(e)
        }


def audit_training(session: dict) -> dict:
    """Check one training session for note vs stats inconsistency."""
    fh_pct = round(float(session["forehand_cross_court_in"]), 2) \
        if session["forehand_cross_court_in"] else None
    bh_pct = round(float(session["backhand_cross_court_in"]), 2) \
        if session["backhand_cross_court_in"] else None
    shots  = round(float(session["shots_in"]), 2) \
        if session["shots_in"] else None

    prompt = f"""Coaching note:
"{session['raw_note_text']}"

Training statistics:
- Overall shots in: {shots if shots else 'N/A'}
- Forehand cross-court accuracy: {fh_pct if fh_pct else 'N/A'}
- Backhand cross-court accuracy: {bh_pct if bh_pct else 'N/A'}

Check for semantic inconsistencies between the note and the stats."""

    try:
        raw = call_claude(AUDITOR_PROMPT + "\n\n" + prompt).strip()
        if raw.startswith("```"):
            raw = "\n".join(
                l for l in raw.split("\n")
                if not l.strip().startswith("```")
            )
        result = json.loads(raw.strip())
        result["session_id"]   = session["session_id"]
        result["session_type"] = "training"
        result["player_id"]    = session["player_id"]
        result["session_date"] = str(session["session_date"])
        return result
    except Exception as e:
        return {
            "session_id":        session["session_id"],
            "session_type":      "training",
            "player_id":         session["player_id"],
            "session_date":      str(session["session_date"]),
            "has_inconsistency": False,
            "error":             str(e)
        }
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 3: RUN AUDITOR ───────────────────────────────────────────
findings = []

print("Auditing match records...")
for m in matches:
    result = audit_match(m.asDict())
    if result.get("has_inconsistency"):
        findings.append(result)
        print(f"  ⚠ {result['session_id']}: {result.get('inconsistency_type')}")

print(f"\nAuditing training sessions...")
for s in sessions:
    result = audit_training(s.asDict())
    if result.get("has_inconsistency"):
        findings.append(result)
        print(f"  ⚠ {result['session_id']}: {result.get('inconsistency_type')}")

print(f"\nTotal inconsistencies found: {len(findings)}")
print(f"  Out of {len(matches) + len(sessions)} records checked")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 4: WRITE FINDINGS TO GOLD ────────────────────────────────
if findings:
    findings_records = [{
        "finding_id":         str(uuid.uuid4()),
        "session_id":         f["session_id"],
        "session_type":       f["session_type"],
        "player_id":          f["player_id"],
        "session_date":       f["session_date"],
        "inconsistency_type": f.get("inconsistency_type"),
        "severity":           f.get("severity"),
        "description":        f.get("description"),
        "note_excerpt":       f.get("note_excerpt"),
        "stat_evidence":      f.get("stat_evidence"),
        "coach_status":       "pending",
        "audited_at":         datetime.now().isoformat()
    } for f in findings]

    findings_df = spark.createDataFrame(pd.DataFrame(findings_records))

    if spark.catalog.tableExists(FINDINGS_TABLE):
        # Deduplicate — skip sessions already in the findings table
        existing_ids = (
            spark.table(FINDINGS_TABLE)
            .select("session_id")
            .distinct()
        )
        findings_df = findings_df.join(
            existing_ids, on="session_id", how="left_anti"
        )
        new_count = findings_df.count()
        if new_count > 0:
            findings_df.write.format("delta").mode("append").saveAsTable(FINDINGS_TABLE)
            print(f"Wrote {new_count} new findings to {FINDINGS_TABLE}")
        else:
            print("All findings already exist in the table — nothing new to write.")
    else:
        findings_df.write.format("delta").mode("overwrite").saveAsTable(FINDINGS_TABLE)
        print(f"Wrote {len(findings_records)} findings to {FINDINGS_TABLE}")
else:
    print("No inconsistencies found — nothing to write.")
# ──────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── STEP 5: VERIFY ────────────────────────────────────────────────
if spark.catalog.tableExists(FINDINGS_TABLE):
    findings_out = spark.table(FINDINGS_TABLE)
    print(f"\n── gold.llm_quality_findings ─────────────────────")
    print(f"Total findings: {findings_out.count():,}")
    findings_out.groupBy("inconsistency_type", "severity").count().show()
    display(findings_out.orderBy("severity", "session_date"))
# ──────────────────────────────────────────────────────────────────
