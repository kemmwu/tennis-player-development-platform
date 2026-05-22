# Databricks notebook source
# evaluate_extraction_accuracy.py
# Monthly evaluation: compare extraction output against HITL-approved eval set
# Measures field-level accuracy and tracks trend over time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extraction Accuracy Evaluation
# MAGIC
# MAGIC Compares Claude API extraction output against human-approved corrections
# MAGIC in `gold.extraction_eval_set`. Run monthly to track accuracy trends.

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

EVAL_TABLE   = "tennis_dev.gold.extraction_eval_set"
MATCH_TABLE  = "tennis_dev.bronze.raw_match_extractions"
TRAIN_TABLE  = "tennis_dev.bronze.raw_training_sessions"

# COMMAND ----------

# ── LOAD EVAL SET ─────────────────────────────────────────────────
eval_df = spark.table(EVAL_TABLE)
eval_count = eval_df.count()
print(f"Eval set size: {eval_count} approved corrections")

if eval_count == 0:
    print("No approved corrections yet. Add some via the HITL app first.")
else:
    # ── MATCH SENTIMENT ACCURACY ──────────────────────────────────
    # Join eval set against note_extractions to compare sentiment

    note_extractions = spark.table("tennis_dev.bronze.note_extractions") \
        if spark.catalog.tableExists("tennis_dev.bronze.note_extractions") \
        else None

    if note_extractions:
        joined = eval_df.alias("eval").join(
            note_extractions.alias("ext"),
            F.col("eval.session_id") == F.col("ext.event_id"),
            how="inner"
        )

        total    = joined.count()
        correct  = joined.filter(
            F.col("eval.sentiment") == F.col("ext.sentiment")
        ).count()

        sentiment_accuracy = correct / total if total > 0 else 0.0
        print(f"\nSentiment accuracy: {sentiment_accuracy:.1%} ({correct}/{total})")
    else:
        print("note_extractions table not found — skipping sentiment accuracy.")

    # ── SUMMARY ───────────────────────────────────────────────────
    print(f"\n── Eval Set Summary ─────────────────────────────")
    eval_df.groupBy("session_type", "sentiment").count().show()

    print(f"Evaluation run at: {datetime.now()}")
# ──────────────────────────────────────────────────────────────────
